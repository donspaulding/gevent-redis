#!/usr/bin/env python

import time

def test_redispy():
    import threading, redis
    redis_client = redis.Redis()
    redis_client.delete('x')
    del redis_client
    time_begin = time.time()
    print 'test_redispy begin', time_begin
    def worker():
        redis_client = redis.Redis()
        for i in xrange(10000):
            redis_client.incr('x')
    jobs = [threading.Thread(target=worker) for i in xrange(50)]
    for job in jobs:
        job.start()
    for job in jobs:
        job.join()
    time_end = time.time()
    print 'test_redispy end', time_end
    print 'total time', time_end - time_begin

def test_geventredis():
    import gevent, geventredis
    redis_client = geventredis.connect()
    redis_client.DEL('x')
    del redis_client
    time_begin = time.time()
    print 'test_geventredis begin', time_begin
    def worker():
        redis_client = geventredis.connect()
        for i in xrange(10000):
            redis_client.incr('x')
    jobs = [gevent.spawn(worker) for i in xrange(50)]
    gevent.joinall(jobs)
    time_end = time.time()
    print 'test_geventredis end', time_end
    print 'total time', time_end - time_begin

def test():
    test_redispy()
    test_geventredis()

if __name__ == '__main__':
    test()