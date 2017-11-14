# coding: utf-8
import sys
import os
import sqlite3
import leveldb
import shutil
import struct
from lib.photo_data import unpack_row
from lib.zorder import to_morton_3d_approx
import time
import argparse


margin_time = 1000
margin_lat = 0.0004
margin_lon = 0.0004
max_results_in_request = 3500


point_fmt = '<iiI'


def pack_point(lon, lat, ts):
    return struct.pack(point_fmt, lon, lat, ts)


def unpack_point(s):
    return struct.unpack(point_fmt, s)


def get_lat_lon_time_from_record(s):
    photo = unpack_row(s)
    return photo.lat_e7, photo.lon_e7, photo.upload_date


def build_sorted_points_db(src_db, temp_dir):
    sorted_db_filename = os.path.join(temp_dir, 'flickr_sorted_3d_tmp')
    if os.path.exists(sorted_db_filename):
        shutil.rmtree(sorted_db_filename)
    sorted_db = leveldb.LevelDB(sorted_db_filename, max_open_files=100)

    batch = leveldb.WriteBatch()
    for i, (photo_id, v) in enumerate(src_db.RangeIter(fill_cache=False), 1):
        lat, lon, ts = get_lat_lon_time_from_record(v)
        z = to_morton_3d_approx(lon + 1800000000, lat + 1800000000, ts)
        k = struct.pack('>Q', z) + photo_id
        v = pack_point(lon, lat, ts)
        batch.Put(k, v)
        if i % 1000 == 0:
            sorted_db.Write(batch)
            batch = leveldb.WriteBatch()
    sorted_db.Write(batch)
    return sorted_db


def build_tree(uniq_db, temp_dir):
    tree_filename = os.path.join(temp_dir, 'flickr_tree_3d_tmp')
    if os.path.exists(tree_filename):
        os.remove(tree_filename)
    tree = sqlite3.connect(tree_filename)
    tree.executescript('''
        PRAGMA journal_mode = off;
        PRAGMA synchronous = off;
        PRAGMA cache_size=-200000;
        CREATE VIRTUAL TABLE point USING rtree_i32(id, min_lat, max_lat, min_lon, max_lon, min_upload_date, max_upload_date);
    ''')
    for i, (k, v) in enumerate(uniq_db.RangeIter(fill_cache=False), 1):
        lon, lat, ts = unpack_point(v)
        tree.execute('INSERT INTO point VALUES (?, ?, ?, ?, ?, ?, ?)',
                     (i, lat, lat, lon, lon, ts, ts))
    tree.commit()
    return tree


def get_queue_db(filename):
    queue_db = sqlite3.connect(filename)
    queue_db.executescript('''
        CREATE TABLE IF NOT EXISTS queue (
          id INTEGER PRIMARY KEY,
          priority INTEGER NOT NULL,
          overflow_expected BOOL,
          flag BOOL,
          min_lat NUMBER,
          max_lat NUMBER,
          min_lon NUMBER,
          max_lon NUMBER,
          min_date INTEGER,
          max_date INTEGER
        );
        
        CREATE INDEX IF NOT EXISTS idx_queue_order_id ON queue(priority DESC, id DESC);
  ''')
    return queue_db


def check_points_count_exceeds(tree, job, max_points):
    job = pad_job_with_margin(job)
    coords_q = 10000000
    min_lat = int(round(job['min_lat'] * coords_q))
    max_lat = int(round(job['max_lat'] * coords_q))
    min_lon = int(round(job['min_lon'] * coords_q))
    max_lon = int(round(job['max_lon'] * coords_q))
    cnt = tree.execute('''
      SELECT count(1) FROM (SELECT 1 FROM point
      WHERE min_lat >= ? AND min_lat < ? AND min_lon >= ? AND min_lon < ? AND
      min_upload_date >= ? AND min_upload_date < ? LIMIT ?)''',
                         (min_lat, max_lat, min_lon, max_lon, job['min_date'], job['max_date'], max_points + 1)).fetchone()[0]
    return cnt > max_points



def select_axis_for_split(job):
    ratios = {
        'lat': float(job['max_lat'] - job['min_lat']) / margin_lat,
        'lon': float(job['max_lon'] - job['min_lon']) / margin_lon,
        'upload_date': float(job['max_date'] - job['min_date']) / margin_time
    }
    return max(ratios.keys(), key=ratios.__getitem__)


def get_middle(job, axis):
    if axis == 'lat':
        return (job['min_lat'] + job['max_lat']) / 2.
    if axis == 'lon':
        return (job['min_lon'] + job['max_lon']) / 2.
    return (job['min_date'] + job['max_date']) / 2


def split_job(job):
    axis = select_axis_for_split(job)
    middle = get_middle(job, axis)

    new_jobs = [dict(job), dict(job)]
    if axis == 'lat':
        new_jobs[0]['max_lat'] = middle
        new_jobs[1]['min_lat'] = middle
    elif axis == 'lon':
        new_jobs[0]['max_lon'] = middle
        new_jobs[1]['min_lon'] = middle
    else:
        new_jobs[0]['max_date'] = middle
        new_jobs[1]['min_date'] = middle
    new_jobs[0]['flag'] = 0
    new_jobs[1]['flag'] = 0
    return new_jobs


def pad_job_with_margin(job):
    job = dict(job)
    if job['max_lat'] - job['min_lat'] > margin_lat:
        job['min_lat'] = max(-90, job['min_lat'] - margin_lat)
        job['max_lat'] = min(90, job['max_lat'] + margin_lat)
    if job['max_lon'] - job['min_lon'] > margin_lon:
        job['min_lon'] = max(-180, job['min_lon'] - margin_lon)
        job['max_lon'] = min(180, job['max_lon'] + margin_lon)
    if job['max_date'] - job['min_date'] > margin_time:
        job['min_date'] = max(0, job['min_date'] - margin_time)
        job['max_date'] += margin_time
    return job


def check_job_too_small(job):
    return (
        (job['max_lat'] - job['min_lat']) < margin_lat * 0.25 and
        (job['max_lon'] - job['min_lon']) < margin_lon * 0.25 and
        (job['max_date'] - job['min_date']) < margin_time * 0.25
    )


def put_job(queue_db, job):
    queue_db.execute('''INSERT INTO queue (priority, overflow_expected, min_lat, max_lat, min_lon, max_lon, min_date, max_date) 
                        VALUES (?,?,?,?,?,?,?,?)''',
                     (job['priority'], job['overflow_expected'], job['min_lat'], job['max_lat'],
                      job['min_lon'], job['max_lon'], job['min_date'], job['max_date']))


def build_queue(queue_filename, tree, add_flag):
    queue_db = get_queue_db(queue_filename)
    requests_n = 0
    n = 1
    t = time.time()
    queue = [{
        'min_lat': -90.,
        'max_lat': 90.,
        'min_lon': -180.,
        'max_lon': 180.,
        'min_date': 0,
        'max_date': int(time.time()) + 600,
        'priority': 1,
        'overflow_expected': 0,
        'flag': 0}]
    first_result = True
    while queue:
        requests_n += 1
        job = queue.pop()
        if check_points_count_exceeds(tree, job, max_results_in_request) and not check_job_too_small(job):
            queue.extend(split_job(job))
        else:
            if first_result:
                queue_db.execute('INSERT INTO queue(priority, flag) VALUES (?,?)', (1, 1))
            put_job(queue_db, job)
            first_result = False
            n += 1
            # FIXME: remove debug print
            # print '\r', n, '%.3f' % ((time.time() - t) / requests_n * 1000),
            sys.stdout.flush()
    queue_db.commit()


def queue_recent(queue_filename, days, add_flag):
    db = get_queue_db(queue_filename)
    now = int(time.time())
    ts1 = now - days * 24 * 3600
    ts2 = now + 24 * 3600
    priority = 10
    with db:
        if add_flag:
            db.execute('INSERT INTO queue(priority, flag) VALUES (?,?)', (priority, 1))
        db.execute('''INSERT INTO queue (priority, overflow_expected, flag, min_lat, max_lat, min_lon, max_lon, min_date, max_date) 
                      VALUES (?,?,?,?,?,?,?,?,?)''', (priority, 1, 0, -90., 90., -180., 180., ts1, ts2))
    db.close()


def queue_all(queue_filename, src_db_filename, temp_dir, add_flag):
    if not os.path.isdir(src_db_filename):
        raise Exception('%s not found' % queue_filename)
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    src_db = leveldb.LevelDB(src_db_filename, max_open_files=100)
    # print 'Sorting'
    t = time.time()
    points_db = build_sorted_points_db(src_db, temp_dir)
    # print time.time() - t
    # print 'Indexing'
    t = time.time()
    tree = build_tree(points_db, temp_dir)
    del points_db
    # print time.time() - t
    # print 'Building'
    t = time.time()
    build_queue(queue_filename, tree, add_flag)
    # print time.time() - t
    tree.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--queue-db', required=True)
    parser.add_argument('-f', '--flag', action='store_true', help='add flag message at end of queue')
    subparsers = parser.add_subparsers(dest='command')
    parser_all = subparsers.add_parser('full')
    parser_recent = subparsers.add_parser('recent')
    parser_all.add_argument('-p', '--photo-db', required=True)
    parser_all.add_argument('-t', '--temp-dir', required=True)
    parser_recent.add_argument('-d', '--days', type=int, required=True)
    conf = parser.parse_args()
    if conf.command == 'recent':
        queue_recent(conf.queue_db, conf.days, conf.flag)
    else:
        queue_all(conf.queue_db, conf.photo_db, conf.temp_dir, conf.flag)


if __name__ == '__main__':
    main()