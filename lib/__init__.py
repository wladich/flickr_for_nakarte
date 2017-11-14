# coding: utf-8
from itertools import islice, chain


def split_chunks(iterable, size):
    sourceiter = iter(iterable)
    while True:
        batchiter = islice(sourceiter, 0, size)
        yield chain([next(batchiter)], batchiter)


if __name__ == '__main__':
    for chunk in split_chunks(xrange(10), 3):
        print list(chunk)