# coding: utf-8
import leveldb
import time
import sys


filename = sys.argv[1]

db = leveldb.LevelDB(filename, max_open_files=100)
n = 0
t = time.time()
for _ in db.RangeIter(include_value=False, fill_cache=False):
    n += 1

print n
print 'Time:', time.time() - t
