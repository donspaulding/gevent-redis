"""
"""

import sys
from time import time
from os import strerror
from cStringIO import StringIO
from errno import EAGAIN, EWOULDBLOCK, EBADF, EISCONN, EWOULDBLOCK, EINPROGRESS, EALREADY, EINVAL, EINTR
from _socket import socket, getdefaulttimeout, getaddrinfo, error, timeout, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_ERROR
from gevent.hub import get_hub
from gevent.timeout import Timeout

cdef bint is_windows = sys.platform == 'win32'

cdef object cancel_wait_ex = error(EBADF, 'File descriptor was closed in another greenlet')

cdef object timeout_default = object()

if sys.version_info[:2] < (2, 7):
    _get_memory = buffer
else:
    def _get_memory(str string, int offset):
        return memoryview(string)[offset:]
        
class RedisError(Exception):
    """Exception thrown for an unsuccessful Redis request."""
    def __init__(self, message):
        Exception.__init__(self, '(Error): %s' % message)

class RedisSocket(object):
    """Green version of :class:`geventredis.client.RedisClient`

    The following methods are overridden:

        * send_request
        * read_response
    """

    def __init__(self, int family=AF_INET, int type=SOCK_STREAM, int proto=0):
        self._sock = socket(family, type, proto)
        self.timeout = getdefaulttimeout()
        self._sock.setblocking(0)
        fileno = self._sock.fileno()
        self.hub = get_hub()
        io = self.hub.loop.io
        self._read_event = io(fileno, 1)
        self._write_event = io(fileno, 2)
        self._rbuf = StringIO()

    def __repr__(self):
        return '<%s at %s %s>' % (type(self).__name__, hex(id(self)), self._formatinfo())

    def __str__(self):
        return '<%s %s>' % (type(self).__name__, self._formatinfo())

    def _formatinfo(self):
        try:
            fileno = self.fileno()
        except Exception:
            fileno = str(sys.exc_info()[1])
        try:
            sockname = self.getsockname()
            sockname = '%s:%s' % sockname
        except Exception:
            sockname = None
        try:
            peername = self.getpeername()
            peername = '%s:%s' % peername
        except Exception:
            peername = None
        result = 'fileno=%s' % fileno
        if sockname is not None:
            result += ' sock=' + str(sockname)
        if peername is not None:
            result += ' peer=' + str(peername)
        if getattr(self, 'timeout', None) is not None:
            result += ' timeout=' + str(self.timeout)
        return result

    def _wait(self, object watcher, object timeout_exc=timeout('timed out')):
        """Block the current greenlet until *watcher* has pending events.

        If *timeout* is non-negative, then *timeout_exc* is raised after *timeout* second has passed.
        By default *timeout_exc* is ``socket.timeout('timed out')``.

        If :func:`cancel_wait` is called, raise ``socket.error(EBADF, 'File descriptor was closed in another greenlet')``.
        """
        assert watcher.callback is None, 'This socket is already used by another greenlet: %r' % (watcher.callback, )
        if self.timeout is not None:
            timeout = Timeout.start_new(self.timeout, timeout_exc)
        else:
            timeout = None
        try:
            self.hub.wait(watcher)
        finally:
            if timeout is not None:
                timeout.cancel()

    def accept(self):
        sock = self._sock
        while 1:
            try:
                client_socket, address = sock.accept()
                break
            except error:
                ex = sys.exc_info()[1]
                if ex[0] != EWOULDBLOCK or self.timeout == 0.0:
                    raise
                sys.exc_clear()
            self._wait(self._read_event)
        return socket(_sock=client_socket), address

    def close(self, object _closedsocket=None, object _delegate_methods=None, object setattr=setattr):
        # This function should not reference any globals. See Python issue #808164.
        self.hub.cancel_wait(self._read_event, cancel_wait_ex)
        self.hub.cancel_wait(self._write_event, cancel_wait_ex)
        self._sock = _closedsocket()
        dummy = self._sock._dummy
        for method in _delegate_methods:
            setattr(self, method, dummy)

    def connect(self, tuple address):
        if self.timeout == 0.0:
            return self._sock.connect(address)
        sock = self._sock
        if isinstance(address, tuple):
            r = getaddrinfo(address[0], address[1], sock.family, sock.type, sock.proto)
            address = r[0][-1]
        if self.timeout is not None:
            timer = Timeout.start_new(self.timeout, timeout('timed out'))
        else:
            timer = None
        try:
            while True:
                err = sock.getsockopt(SOL_SOCKET, SO_ERROR)
                if err:
                    raise error(err, strerror(err))
                result = sock.connect_ex(address)
                if not result or result == EISCONN:
                    break
                elif (result in (EWOULDBLOCK, EINPROGRESS, EALREADY)) or (result == EINVAL and is_windows):
                    self._wait(self._write_event)
                else:
                    raise error(result, strerror(result))
        finally:
            if timer is not None:
                timer.cancel()

    def connect_ex(self, tuple address):
        try:
            return self.connect(address) or 0
        except timeout:
            return EAGAIN
        except error:
            ex = sys.exc_info()[1]
            if type(ex) is error:
                return ex.args[0]
            else:
                raise  # gaierror is not silented by connect_ex

    def dup(self):
        """dup() -> socket object

        Return a new socket object connected to the same system resource.
        Note, that the new socket does not inherit the timeout."""
        return socket(_sock=self._sock)
      
    def recv(self, *args):
        sock = self._sock  # keeping the reference so that fd is not closed during waiting
        while True:
            try:
                return sock.recv(*args)
            except error:
                ex = sys.exc_info()[1]
                if ex.args[0] == EBADF:
                    return ''
                if ex.args[0] != EWOULDBLOCK or self.timeout == 0.0:
                    raise
                # QQQ without clearing exc_info test__refcount.test_clean_exit fails
                sys.exc_clear()
            try:
                self._wait(self._read_event)
            except error:
                ex = sys.exc_info()[1]
                if ex.args[0] == EBADF:
                    return ''
                raise

    def recvfrom(self, *args):
        sock = self._sock
        while True:
            try:
                return sock.recvfrom(*args)
            except error:
                ex = sys.exc_info()[1]
                if ex.args[0] != EWOULDBLOCK or self.timeout == 0.0:
                    raise
                sys.exc_clear()
            self._wait(self._read_event)

    def recvfrom_into(self, *args):
        sock = self._sock
        while True:
            try:
                return sock.recvfrom_into(*args)
            except error:
                ex = sys.exc_info()[1]
                if ex.args[0] != EWOULDBLOCK or self.timeout == 0.0:
                    raise
                sys.exc_clear()
            self._wait(self._read_event)

    def recv_into(self, *args):
        sock = self._sock
        while True:
            try:
                return sock.recv_into(*args)
            except error:
                ex = sys.exc_info()[1]
                if ex.args[0] == EBADF:
                    return 0
                if ex.args[0] != EWOULDBLOCK or self.timeout == 0.0:
                    raise
                sys.exc_clear()
            try:
                self._wait(self._read_event)
            except error:
                ex = sys.exc_info()[1]
                if ex.args[0] == EBADF:
                    return 0
                raise

    def send(self, object data, int flags=0, object timeout=timeout_default):
        sock = self._sock
        if timeout is timeout_default:
            timeout = self.timeout
        try:
            return sock.send(data, flags)
        except error:
            ex = sys.exc_info()[1]
            if ex.args[0] != EWOULDBLOCK or timeout == 0.0:
                raise
            sys.exc_clear()
            try:
                self._wait(self._write_event)
            except error:
                ex = sys.exc_info()[1]
                if ex.args[0] == EBADF:
                    return 0
                raise
            try:
                return sock.send(data, flags)
            except error:
                ex2 = sys.exc_info()[1]
                if ex2.args[0] == EWOULDBLOCK:
                    return 0
                raise

    def sendall(self, object data, int flags=0):
        if isinstance(data, unicode):
            data = data.encode()
        # this sendall is also reused by gevent.ssl.SSLSocket subclass,
        # so it should not call self._sock methods directly
        if self.timeout is None:
            data_sent = 0
            while data_sent < len(data):
                data_sent += self.send(_get_memory(data, data_sent), flags)
        else:
            timeleft = self.timeout
            end = time() + timeleft
            data_sent = 0
            while True:
                data_sent += self.send(_get_memory(data, data_sent), flags, timeout=timeleft)
                if data_sent >= len(data):
                    break
                timeleft = end - time()
                if timeleft <= 0:
                    raise timeout('timed out')

    def sendto(self, *args):
        sock = self._sock
        try:
            return sock.sendto(*args)
        except error:
            ex = sys.exc_info()[1]
            if ex.args[0] != EWOULDBLOCK or timeout == 0.0:
                raise
            sys.exc_clear()
            self._wait(self._write_event)
            try:
                return sock.sendto(*args)
            except error:
                ex2 = sys.exc_info()[1]
                if ex2.args[0] == EWOULDBLOCK:
                    return 0
                raise

    def setblocking(self, int flag):
        if flag:
            self.timeout = None
        else:
            self.timeout = 0.0

    def settimeout(self, object howlong):
        if howlong is not None:
            try:
                f = howlong.__float__
            except AttributeError:
                raise TypeError('a float is required')
            howlong = f()
            if howlong < 0.0:
                raise ValueError('Timeout value out of range')
        self.timeout = howlong

    def gettimeout(self):
        return self.timeout

    def shutdown(self, int how):
        if how == 0:  # SHUT_RD
            self.hub.cancel_wait(self._read_event, cancel_wait_ex)
        elif how == 1:  # SHUT_RW
            self.hub.cancel_wait(self._write_event, cancel_wait_ex)
        else:
            self.hub.cancel_wait(self._read_event, cancel_wait_ex)
            self.hub.cancel_wait(self._write_event, cancel_wait_ex)
        self._sock.shutdown(how)

    def read(self, size):
        buf = self._rbuf
        buf.seek(0, 2)  # seek end
        # Read until size bytes or EOF seen, whichever comes first
        buf_len = buf.tell()
        if buf_len >= size:
            # Already have size bytes in our buffer?  Extract and return.
            buf.seek(0)
            rv = buf.read(size)
            self._rbuf = StringIO()
            self._rbuf.write(buf.read())
            return rv

        self._rbuf = StringIO()  # reset _rbuf.  we consume it via buf.
        while True:
            left = size - buf_len
            try:
                data = self.recv(left)
            except error, e:
                if e.args[0] == EINTR:
                    continue
                raise
            if not data:
                break
            n = len(data)
            if n == size and not buf_len:
                return data
            if n == left:
                buf.write(data)
                del data  # explicit free
                break
            assert n <= left, "recv(%d) returned %d bytes" % (left, n)
            buf.write(data)
            buf_len += n
            del data  # explicit free
            #assert buf_len == buf.tell()
        return buf.getvalue()

    def readline(self):
        buf = self._rbuf
        buf.seek(0, 2)  # seek end
        if buf.tell() > 0:
            # check if we already have it in our buffer
            buf.seek(0)
            bline = buf.readline()
            if bline.endswith('\n'):
                self._rbuf = StringIO()
                self._rbuf.write(buf.read())
                return bline
            del bline
        # Read until \n or EOF, whichever comes first
        buf.seek(0, 2)  # seek end
        self._rbuf = StringIO()  # reset _rbuf.  we consume it via buf.
        while True:
            try:
                data = self.recv(8192)
            except error, e:
                if e.args[0] == EINTR:
                    continue
                raise
            if not data:
                break
            nl = data.find('\n')
            if nl >= 0:
                nl += 1
                buf.write(data[:nl])
                self._rbuf.write(data[nl:])
                del data
                break
            buf.write(data)
        return buf.getvalue()

    def send_request(self, str command, *args):
        data = '*%d\r\n$%d\r\n%s\r\n' % (1+len(args), len(command), command) + ''.join(['$%d\r\n%s\r\n' % (len(str(x)), x) for x in args])
        self.send(data)
        
    def read_response(self):
        read = self.read
        readline = self.readline
        response = readline()
        byte, response = response[0], response[1:]
        if byte == '+':
            return response[:2]
        elif byte == ':':
            return int(response)
        elif byte == '$':
            number = int(response)
            if number == -1:
                return None
            else:
                return read(number+2)[:-2]
        elif byte == '*':
            number = int(readline())
            if number == -1:
                return None
            else:
                result = []
                while number:
                    response = readline()
                    byte, response = response[0], response[1:]
                    if byte == '$':
                        result.append(read(int(response)+2)[:-2])
                    else:
                        if byte == ':':
                            result.append(int(readline()))
                        else:
                            result.append(readline()[:-2])
                    number -= 1
                return result
        elif byte == '-':
            return RedisError(readline()[:-2])
        else:
            raise RedisError('bulk cannot startswith %r' % byte)
        
    
