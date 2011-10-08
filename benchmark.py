#!/usr/bin/env python

import time

CONNECTIONS = 50
INCR_NUMBER = 1000

def test_redispy():
    import threading, redis
    redis_client = redis.Redis()
    redis_client.delete('x')
    del redis_client
    time_begin = time.time()
    print 'test_redispy begin', time_begin
    def worker():
        redis_client = redis.Redis()
        for i in xrange(INCR_NUMBER):
            redis_client.incr('x')
    jobs = [threading.Thread(target=worker) for i in xrange(CONNECTIONS)]
    for job in jobs:
        job.start()
    for job in jobs:
        job.join()
    time_end = time.time()
    print 'test_redispy end', time_end
    print 'test_redispy total', time_end - time_begin

def test_geventredis_without_gevent():
    import gevent, gevent.socket, socket
    gevent.socket = socket
    import threading, geventredis
    redis_client = geventredis.connect()
    redis_client.delete('x')
    del redis_client
    time_begin = time.time()
    print 'test_geventredis_without_gevent begin', time_begin
    def worker():
        redis_client = geventredis.connect()
        for i in xrange(INCR_NUMBER):
            redis_client.incr('x')
    jobs = [threading.Thread(target=worker) for i in xrange(CONNECTIONS)]
    for job in jobs:
        job.start()
    for job in jobs:
        job.join()
    time_end = time.time()
    print 'test_geventredis_without_gevent end', time_end
    print 'test_geventredis_without_gevent total', time_end - time_begin

def test_geventredis():
    import gevent, geventredis
    redis_client = geventredis.connect()
    redis_client.delete('x')
    del redis_client
    time_begin = time.time()
    print 'test_geventredis begin', time_begin
    def worker():
        redis_client = geventredis.connect()
        for i in xrange(INCR_NUMBER):
            redis_client.incr('x')
    jobs = [gevent.spawn(worker) for i in xrange(CONNECTIONS)]
    gevent.joinall(jobs)
    time_end = time.time()
    print 'test_geventredis end', time_end
    print 'test_geventredis total', time_end - time_begin

def test():
    print '-----------------------------'
    test_redispy()
    print '-----------------------------'
    test_geventredis_without_gevent()
    print '-----------------------------'
    test_geventredis()
    print '-----------------------------'

if __name__ == '__main__':
    test()
