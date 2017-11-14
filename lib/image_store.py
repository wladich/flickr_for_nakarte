# -*- coding: utf-8 -*-
import os
import multiprocessing
import sqlite3

db_lock = multiprocessing.Lock()

class MBTilesWriter(object):
    SCHEME = '''
        CREATE TABLE tiles(
            zoom_level integer, tile_column integer, tile_row integer, tile_data blob,
            UNIQUE(zoom_level, tile_column, tile_row) ON CONFLICT REPLACE);
       
        CREATE TABLE metadata (name text, value text, UNIQUE(name) ON CONFLICT REPLACE);
    '''
    
    PRAGMAS = '''
        PRAGMA journal_mode = off;
        PRAGMA synchronous = 0;
        PRAGMA busy_timeout = 10000;
    '''

    def __init__(self, path):
        need_init = not os.path.exists(path)
        self.path = path
        if need_init:
            self.conn.executescript(self.SCHEME)

    _conn = None

    @property
    def conn(self):
        if self._conn is None:
            conn = self._conn = sqlite3.connect(self.path)
            conn.executescript(self.PRAGMAS)
        return self._conn
    
    def write(self, data, tile_x, tile_y, level):
        tile_y = 2 ** level - tile_y - 1
        s = buffer(data)
        with db_lock:
            conn = self.conn
            conn.execute('''
                INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?,?,?,?)''',
                (level, tile_x, tile_y, s))

    def close(self):
        conn = self.conn
        conn.commit()
        conn.close()
