#!/usr/bin/env python

import sys, os, re, time
import geventredis

def test():
    redis_client = geventredis.connect()
    print redis_client.info()
    for msg in redis_client.monitor():
        print msg

if __name__ == '__main__':
    test()
