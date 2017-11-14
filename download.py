# coding: utf-8
import sys
import os
import requests
import sqlite3
import leveldb
import time
import json
import pyproj
from multiprocessing.dummy import Pool as ThreadPool
from requests.adapters import HTTPAdapter
import datetime
import build_queue
from lib.photo_data import pack_row, pack_id
import argparse


proj_wgs84 = pyproj.Proj('+init=EPSG:4326')
proj_gmerc = pyproj.Proj('+init=EPSG:3857')

MAX_PHOTOS_PER_PAGE = 250


def get_job(db):
    return db.execute('''
      SELECT * 
      FROM queue 
      ORDER BY priority DESC, id DESC 
      LIMIT 1''').fetchone()


def remove_job(db, job):
    db.execute('DELETE FROM queue WHERE id=?', (job['id'],))


def put_photos(db, photos):
    batch = leveldb.WriteBatch()
    for photo in photos:
        batch.Put(pack_id(photo['id']), pack_row(photo))
    db.Write(batch)


def commit(db):
    db.commit()


def get_queue_database(filename):
    if not os.path.exists(filename):
        raise Exception('File "%s" not found' % filename)
    db = sqlite3.connect(filename)
    db.row_factory = sqlite3.Row
    return db


session = requests.Session()
adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
session.mount("http://", adapter)
session.mount("https://", adapter)
pool = ThreadPool(20)


def get_page(job, per_page, page):
    url = 'https://api.flickr.com/services/rest/'
    job = build_queue.pad_job_with_margin(job)
    # min_lat, max_lat, min_lon, max_lon, min_date, max_date = job
    bounds_param = ','.join(map(str, [job['min_lon'], job['min_lat'], job['max_lon'], job['max_lat']]))
    params = {
        'method': 'flickr.photos.search',
        'api_key': '1eff3f57e8aa5886a70a0a71c9a88217',
        'format': 'json',
        'nojsoncallback': '1',
        'per_page': per_page,
        'bbox': bounds_param,
        'max_upload_date': job['max_date'],
        'min_upload_date': job['min_date'],
        'page': page,
        'extras': 'geo,date_upload'
    }
    retries = 1000
    while True:
        try:
            resp = session.get(url, params=params, timeout=(3.05, 30))
            data = resp.content
            data = json.loads(data)
            if data['stat'] != 'ok':
                raise Exception('Invalid response: %r' % data)
        except:
            if not retries:
                raise
            retries -= 1
            # FIXME: remove debug print
            print 'Retrying'
            time.sleep(1)
        else:
            break
    return data['photos']


def get_pages_parallel(job, page_numbers):
    return pool.map(lambda page_i: get_page(job, MAX_PHOTOS_PER_PAGE, page=page_i), page_numbers)


def get_photos(job, ignore_overflow):
    optimistic = not bool(job['overflow_expected'])
    reqs_n = 0
    if not optimistic and not ignore_overflow:
        page = get_page(job, 1, 1)
        reqs_n += 1
        total = int(page['total'])
        if total > 4000:
            # FIXME: remove debug print
            print 'Overflow', total, job
            return {'overflow': True, 'reqs': reqs_n}
    pages = get_pages_parallel(job, [1, 2])
    reqs_n += 2
    total = int(pages[0]['total'])
    if total > 4000:
        if ignore_overflow:
            # FIXME: remove debug print
            print 'Overflow, not splitting', total, job
            pages_n = int(pages[0]['pages'])
            pages_n = min(pages_n, 20)
        else:
            # FIXME: remove debug print
            print 'Overflow', total, list(job)
            return {'overflow': True, 'reqs': reqs_n}
    else:
        pages_n = int(pages[0]['pages'])
    page_numbers = range(3, pages_n + 2)

    if total == 0:
        # FIXME: remove debug print
        print 'Empty'
        return {'overflow': False, 'photos': [], 'reqs': 2}
    # FIXME: remove debug print
    print 'Total', total, 'Pages', pages_n
    if page_numbers:
        reqs_n += len(page_numbers)
        pages += get_pages_parallel(job, page_numbers)

    photos = sum([page['photo'] for page in pages], [])
    photos = [{
                  'id': photo['id'],
                  'lat': float(photo['latitude']),
                  'lon': float(photo['longitude']),
                  'accuracy': int(photo['accuracy']),
                  'upload_date': int(photo['dateupload']),
                  'owner': str(photo['owner'])} for photo in photos]
    return {'overflow': False, 'photos': photos, 'reqs': reqs_n}


def signal_flag(flags_dir):
    flag_filename = 'flickr_queue_flag_%d' % (time.time() * 1000)
    open(os.path.join(flags_dir, flag_filename), 'w').close()


def download(photo_db_filename, queue_db_filename, flags_dir):
    photo_db = leveldb.LevelDB(photo_db_filename)
    queue_db = get_queue_database(queue_db_filename)
    fetch_time = 0
    db_time = 0
    reqs_n = 0
    t = time.time()
    processed_jobs = 0
    jobs_with_data = 0
    results_n = 0
    while True:
        t2 = time.time()
        job = get_job(queue_db)
        db_time += time.time() - t2
        if not job:
            break
        if job['flag'] and flags_dir:
            signal_flag(flags_dir)
            remove_job(queue_db, job)
            queue_db.commit()
            continue
        processed_jobs += 1
        t2 = time.time()
        job_is_small = build_queue.check_job_too_small(job)
        photos = get_photos(job, ignore_overflow=job_is_small)
        fetch_time += time.time() - t2

        t2 = time.time()
        if photos['overflow']:
            for new_job in build_queue.split_job(job):
                build_queue.put_job(queue_db, new_job)
        else:
            jobs_with_data += 1
            results_n += len(photos['photos'])
            put_photos(photo_db, photos['photos'])
        remove_job(queue_db, job)
        queue_db.commit()
        db_time += time.time() - t2
        reqs_n += photos['reqs']

        if time.time() - t > 60:
            queue_len = queue_db.execute('SELECT count(1) FROM queue').fetchone()[0]
            total_time = time.time() - t
            rps = float(reqs_n) / fetch_time
            db_time_share = db_time / total_time * 100
            timestr = datetime.datetime.now().strftime('%d %H:%M:%S')
            jobs_per_second = float(processed_jobs) / total_time
            jobs_with_data_share = 100. * jobs_with_data / processed_jobs
            # FIXME: remove debug print
            print \
                '\r', timestr,\
                'Queue:', queue_len,\
                'DB: %.1f%%' % db_time_share,\
                'Reqs/s: %.1f' % rps,\
                'Jobs/s: %.1f' % jobs_per_second,\
                'Hit rate: %.1f%%' %  jobs_with_data_share

            t = time.time()
            fetch_time = 0
            db_time = 0
            reqs_n = 0
            processed_jobs = 0
            jobs_with_data = 0
            # prev_photo_cnt = res_cnt
            results_n = 0
        sys.stdout.flush()
    print 'Done'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--photo-db', required=True)
    parser.add_argument('-q', '--queue-db', required=True)
    parser.add_argument('-f', '--flags-dir')
    conf = parser.parse_args()
    if conf.flags_dir and not os.path.isdir(conf.flags_dir):
        raise Exception('Directory %s not found' % conf.flags_dir)
    download(conf.photo_db, conf.queue_db, conf.flags_dir)


if __name__ == '__main__':
    main()
