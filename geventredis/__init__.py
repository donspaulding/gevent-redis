#!/usr/bin/env python
#
# Copyright 2009 Phus Lu
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""Redis client implementations using gevent.socket"""

try:
    from gevent import socket
except ImportError:
    print('import gevent.socket fail, fallback to standard socket module')
    import socket

def connect(host='localhost', port=6379, timeout=None):
    """Create gevent Redis client.

    Example usage::

        import geventredis

        redis_client = geventredis.connect('127.0.0.1', 6379)
        result = redis_client.set('foo', 'bar')
        print result

    This class implements a Redis client on top of Gevent Socket.
    It does not currently implement all applicable parts of the Redis
    specification, but it does enough to work with major redis server APIs
    (mostly tested against the LIST/HASH/PUBSUB API so far).
    """
    return RedisClient(host, port, timeout)

YIELD_COMMANDS = [
                    'subscribe',
                    'psubscribe',
                    'monitor',
                 ]
YIELD_COMMANDS = frozenset(sum([[x.lower(), x.upper()] for x in YIELD_COMMANDS], []))

class RedisClient(object):
    """An gevent Redis client.

    Example usage::

        import geventredis

        redis_client = geventredis.RedisClient('127.0.0.1', 6379)
        result = redis_client.get('foo')
        print result

    This class implements a Redis client on top of Gevent Socket.
    It does not currently implement all applicable parts of the Redis
    specification, but it does enough to work with major redis server APIs
    (mostly tested against the LIST/HASH/PUBSUB API so far).
    """

    def __init__(self, host='localhost', port=6379, timeout=None):
        """Connect to a Redis Server."""
        self._address = (host, port)
        self._timeout = timeout
        self._socket  = socket.create_connection(self._address, self._timeout)
        self._rfile   = self._socket.makefile('r', 0)

    def __del__(self):
        """Destroys this redis client, freeing any file descriptors used.

        If you want to disconnect redis immediately, you can use `del redis_client`
        """
        self._socket.close()

    def __getattr__(self, command):
        """Executes a redis command and return a result"""
        def command_warpper(*args):
            data = '*%d\r\n$%d\r\n%s\r\n' % (1+len(args), len(command), command)\
                   + ''.join(['$%d\r\n%s\r\n' % (len(str(x)), x) for x in args])
            self._socket.send(data)
            return self._parse_respone()

        def yield_command_warpper(*args):
            data = '*%d\r\n$%d\r\n%s\r\n' % (1+len(args), len(command), command)\
                   + ''.join(['$%d\r\n%s\r\n' % (len(str(x)), x) for x in args])
            self._socket.send(data)
            while 1:
                yield self._parse_respone()

        if command not in YIELD_COMMANDS:
            return command_warpper
        else:
            return yield_command_warpper

    def _parse_respone(self):
        """Read a completed result data from the redis server."""
        read = self._rfile.read
        readline = self._rfile.readline
        byte = read(1)
        if byte == '+':
            return readline()[:-2]
        elif byte == '-':
            return RedisError(readline()[:-2])
        elif byte == ':':
            return int(readline())
        elif byte == '$':
            number = int(readline())
            if number == -1:
                return None
            else:
                data = read(number)
                read(2)
                return data
        elif byte == '*':
            number = int(readline())
            if number == -1:
                return None
            else:
                result = []
                while number:
                    byte = read(1)
                    if byte == '$':
                        length  = int(readline())
                        element = read(length)
                        result.append(element)
                        read(2)
                    else:
                        if byte == ':':
                            element = int(readline())
                        else:
                            element = readline()[:-2]
                        result.append(element)
                    number -= 1
                return result
        else:
            raise RedisError('bulk cannot startswith %r' % c)

class RedisError(Exception):
    """Exception thrown for an unsuccessful Redis request."""
    def __init__(self, message):
        Exception.__init__(self, '(Error): %s' % message)

def test():
    redis_client = connect('127.0.0.1', 6379)
    print redis_client.set('foo', 'bar')
    print redis_client.get('foo')

if __name__ == '__main__':
    test()