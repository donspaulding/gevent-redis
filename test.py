#!/usr/bin/env python

import geventredis

def test():
    redis_client = geventredis.connect('127.0.0.1', 6379)
    for i in xrange(1000):
        redis_client.incr('x')
    print redis_client.get('x')

if __name__ == '__main__':
    test()