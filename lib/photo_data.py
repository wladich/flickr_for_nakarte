# coding: utf-8
import struct
import cPickle as pickle
import time
from collections import namedtuple

id_fmt = '<Q'


Photo = namedtuple('Photo', 'lat_e7 lon_e7 accuracy fetch_ts upload_date owner')

def pack_id(id_):
    id_ = int(id_)
    return struct.pack(id_fmt, id_)


def packed_id_size():
    return struct.calcsize(id_fmt)


def unpack_id(s):
    return struct.unpack(id_fmt, s)[0]


def pack_row(photo):
    lat = int(round(photo['lat'] * 1e7))
    lon = int(round(photo['lon'] * 1e7))
    data = (lat, lon, int(photo['accuracy']), int(time.time()), int(photo['upload_date']), photo['owner'])
    return pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)


def unpack_row(s):
    return Photo(*pickle.loads(s))
