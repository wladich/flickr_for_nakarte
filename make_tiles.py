# coding: utf-8
import sys
import sqlite3
from PIL import Image, ImageDraw
from cStringIO import StringIO
import os
from lib.image_store import MBTilesWriter
from lib.photo_data import unpack_row
from array import array
import shutil
import leveldb
import time
from lib import split_chunks
import struct
from lib.zorder import to_morton_2d
import pyproj
import argparse
import itertools
import gzip

symbol_radius = 5

max_overviews_level = 5
max_points_in_vector_tile = 2000
max_points_in_normal_tile = 100000
max_level = 18

banned_users = ['100597270@N04']

proj_wgs84 = pyproj.Proj('+init=EPSG:4326')
proj_gmerc = pyproj.Proj('+init=EPSG:3857')

_symbol = None


assert len(array('i', [1]).tostring()) == 4


def get_symbol():
    global _symbol
    if _symbol is None:
        r = symbol_radius
        dest_size = r * 2 + 1
        q = 4
        im = Image.new('L', (dest_size * q, dest_size * q), 0)
        draw = ImageDraw.Draw(im)
        draw.ellipse([0, 0, 2 * r * q, 2 * r * q], fill=255)
        del draw
        _symbol = im.resize((dest_size, dest_size), Image.ANTIALIAS)
    return _symbol


def get_tile_extents(x, y, z):
    max_coord = 20037508.342789244
    tile_size = 2 * max_coord / (1 << z)
    return (x * tile_size - max_coord, y * tile_size - max_coord, tile_size)


def draw_raster_tile(points, tile_bounds):
    im = Image.new('L', (256, 256), 0)
    tile_min_x, tile_min_y, tile_size = tile_bounds
    marker = get_symbol()
    r = symbol_radius
    # points can be iterator, so we cannot check it's length
    has_points = False
    for x, y in points:
        has_points = True
        pix_x = (x - tile_min_x) / tile_size * 256
        pix_y = (y - tile_min_y) / tile_size * 256
        pix_y = 256 - pix_y
        pix_x = int(pix_x)
        pix_y = int(pix_y)
        im.paste(255, (pix_x - r, pix_y - r, pix_x + r + 1, pix_y + r + 1), mask=marker)
    if not has_points:
        return None
    im2 = Image.new('LA', (256, 256), 0)
    im2.putalpha(im)
    fd = StringIO()
    im2.save(fd, 'PNG')
    return fd.getvalue()


def draw_overview_tile(db, tile_x, tile_y, tile_z):
    step_pixels = 2
    tile_bounds = tile_min_x, tile_min_y, tile_size = get_tile_extents(tile_x, tile_y, tile_z)
    points = []
    pixel_meters = tile_size / 256
    step_meters = step_pixels * pixel_meters
    margin_steps = (symbol_radius - 1) / step_pixels + 2
    margin_pixels = margin_steps * step_pixels
    for x in xrange(-margin_pixels, 256 + margin_pixels - step_pixels, step_pixels):
        min_x = tile_min_x + x * pixel_meters
        max_x = min_x + step_meters
        for y in xrange(-margin_pixels, 256 + margin_pixels - step_pixels, step_pixels):
            min_y = tile_min_y + y * pixel_meters
            max_y = min_y + step_meters
            if db.execute(
                    'SELECT EXISTS (SELECT 1 FROM point WHERE minx > ? AND minx <= ? AND miny > ? AND miny <= ? LIMIT 1)',
                    (min_x, max_x, min_y, max_y)).fetchall()[0][0]:
                points.append((min_x + step_pixels / 2, min_y + step_pixels / 2))
    if points:
        image_data = draw_raster_tile(points, tile_bounds)
        return {'data': image_data, 'is_vector': False}
    else:
        image_data = make_vector_tile([], tile_x, tile_y, tile_z)
        return {'data': image_data, 'is_vector': True}


def tile_with_margin_extents(tile_x, tile_y, tile_z):
    tile_min_x, tile_min_y, tile_size = get_tile_extents(tile_x, tile_y, tile_z)
    pixel_meters = tile_size / 256
    margin = symbol_radius * pixel_meters
    min_x = tile_min_x - margin
    max_x = tile_min_x + tile_size + margin
    min_y = tile_min_y - margin
    max_y = tile_min_y + tile_size + margin
    return min_x, max_x, min_y, max_y


def get_points_for_tile(db, tile_x, tile_y, tile_z):
    (min_x, max_x, min_y, max_y) = tile_with_margin_extents(tile_x, tile_y, tile_z)
    points = db.execute('SELECT minx, miny FROM point WHERE minx > ? AND minx <= ? AND miny > ? AND miny <= ?',
                        (min_x, max_x, min_y, max_y))
    return points


def draw_normal_tile(db, tile_x, tile_y, tile_z):
    points_iterator = get_points_for_tile(db, tile_x, tile_y, tile_z)
    points1 = list(itertools.islice(points_iterator, 0, max_points_in_normal_tile + 1))
    if len(points1) <= max_points_in_vector_tile:
        image_data = make_vector_tile(points1, tile_x, tile_y, tile_z)
        if len(image_data) > 500:
            compressed = gzip_compress(image_data)
            if len(compressed) < len(image_data):
                image_data = compressed
        return {'data': image_data, 'is_vector': True}
    elif len(points1) > max_points_in_normal_tile:
        return draw_overview_tile(db, tile_x, tile_y, tile_z)
    else:
        tile_bounds = get_tile_extents(tile_x, tile_y, tile_z)
        image_data = draw_raster_tile(itertools.chain(points1, points_iterator), tile_bounds)
        return {'data': image_data, 'is_vector': False}


def gzip_compress(s):
    f = StringIO()
    g = gzip.GzipFile(fileobj=f, mode='w')
    g.write(s)
    g.close()
    return f.getvalue()


def make_vector_tile(points, tile_x, tile_y, tile_z):
    extent = 1 << 20
    tile_min_x, tile_min_y, tile_size = get_tile_extents(tile_x, tile_y, tile_z)
    ar = array('i')
    ar.extend([tile_x, tile_y, tile_z])
    for x, y in points:
        x = (x - tile_min_x) / tile_size * extent
        y = (1 - (y - tile_min_y) / tile_size) * extent
        ar.append(int(round(x)))
        ar.append(int(round(y)))
    data = 'VPTT' + ar.tostring()
    return data


def tile_index_from_tms((x, y, z)):
    y = (2 ** z) - 1 - y
    return x, y, z


def make_tiles(tree, tiles_db_filename):
    if os.path.exists(tiles_db_filename):
        os.remove(tiles_db_filename)
    writer = MBTilesWriter(tiles_db_filename)
    queue = [(0, 0, 0)]
    n = 0

    while queue:
        tile = queue.pop()
        x, y, z = tile[:3]
        res = draw_normal_tile(tree, *tile)
        assert res['data']
        if res['data']:
            writer.write(res['data'], *tile_index_from_tms(tile[:3]))
            if (not res['is_vector']) and z <= max_level:
                queue.append((x * 2, y * 2, z + 1))
                queue.append((x * 2 + 1, y * 2, z + 1))
                queue.append((x * 2, y * 2 + 1, z + 1))
                queue.append((x * 2 + 1, y * 2 + 1, z + 1))
            n += 1
            print '\r', n,
            sys.stdout.flush()
    writer.close()


def get_banned_owners():
    return banned_users


def iterate_src_points(src_db_filename):
    if not os.path.exists(src_db_filename):
        raise Exception()

    src_db = leveldb.LevelDB(src_db_filename, max_open_files=100)
    banned_owners = get_banned_owners()
    for i, (_, v) in enumerate(src_db.RangeIter(fill_cache=False)):
        photo = unpack_row(v)
        if photo.owner in banned_owners:
            continue
        lat = photo.lat_e7
        lon = photo.lon_e7
        yield lat, lon


def store_chunk_sorted_db(db, points):
    batch = leveldb.WriteBatch()
    for z, lat, lon in points:
        k = struct.pack('>Q', z)
        v = struct.pack('<ii', lat, lon)
        batch.Put(k, v)
    db.Write(batch)


def build_sorted_points_db(photo_db, temp_dir):
    sorted_db_filename = os.path.join(temp_dir, 'flickr_sorted_2d_tmp')

    # for experiments:
    # db = leveldb.LevelDB(sorted_db_filename, max_open_files=100)
    # return db

    if os.path.exists(sorted_db_filename):
        shutil.rmtree(sorted_db_filename)
    db = leveldb.LevelDB(sorted_db_filename, max_open_files=100)
    chunk_size = 100000
    for i, points in enumerate(split_chunks(iterate_src_points(photo_db), chunk_size)):
        points = set(points)
        points = [(to_morton_2d(lon + 1800000000, lat + 1800000000), lat, lon)
                  for lat, lon in points]
        store_chunk_sorted_db(db, points)

        print '\r', i * chunk_size,
        sys.stdout.flush()

    return db


def iterate_sorted_points(db):
    for _, v in db.RangeIter(fill_cache=False):
        lat, lon = struct.unpack('<ii', v)
        lat /= 1e7
        lon /= 1e7
        if (-85.05113 < lat < 85.05113) and lat != 0 and lon != 0 and lat != lon:
            yield lat, lon


def store_chunk_to_tree(tree, latlons):
    lats, lons = zip(*latlons)
    x, y = pyproj.transform(proj_wgs84, proj_gmerc, lons, lats)
    x = (int(round(c)) for c in x)
    y = (int(round(c)) for c in y)
    xy = zip(x, y)
    params = (((x2 << 32) | y2, x2, x2, y2, y2) for (x2, y2) in xy)
    tree.executemany('INSERT OR IGNORE INTO point VALUES (?,?,?,?,?)', params)


def build_tree(sorted_db, temp_dir):
    tree_filename = os.path.join(temp_dir, 'flickr_tree_2d_tmp')

    # for experiments:
    # tree = sqlite3.connect(tree_filename)
    # tree.executescript('''
    #     PRAGMA journal_mode = off;
    #     PRAGMA synchronous = off;
    #     PRAGMA cache_size=-200000;
    # ''')
    # return tree

    if os.path.exists(tree_filename):
        os.remove(tree_filename)
    tree = sqlite3.connect(tree_filename)
    tree.executescript('''
        PRAGMA journal_mode = off;
        PRAGMA synchronous = off;
        PRAGMA cache_size=-200000;
        CREATE VIRTUAL TABLE point USING rtree_i32(id, minx, maxx, miny, maxy);
    ''')

    chunk_size = 10000
    for i, chunk in enumerate(split_chunks(iterate_sorted_points(sorted_db), chunk_size)):
        store_chunk_to_tree(tree, chunk)
        print '\r', i * chunk_size,
        sys.stdout.flush()
    tree.commit()
    return tree


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--tiles-db', required=True)
    parser.add_argument('-p', '--photo-db', required=True)
    parser.add_argument('-t', '--temp-dir', required=True)
    conf = parser.parse_args()

    if not os.path.exists(conf.temp_dir):
        os.makedirs(conf.temp_dir)

    print 'Sorting'
    t = time.time()
    sorted_db = build_sorted_points_db(conf.photo_db, conf.temp_dir)
    print
    print time.time() - t

    print 'Indexing'
    t = time.time()
    tree = build_tree(sorted_db, conf.temp_dir)
    print
    print time.time() - t
    del sorted_db

    print 'Making tiles'
    t = time.time()
    make_tiles(tree, conf.tiles_db)
    print
    print time.time() - t
    tree.close()


if __name__ == '__main__':
    main()
    print 'Done'
