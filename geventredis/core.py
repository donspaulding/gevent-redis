"""
"""

from cStringIO import StringIO
from errno import EINTR
from _socket import error
from gevent.socket import socket

class RedisSocket(socket):

    def __init__(self, *args, **kwargs):
        socket.__init__(self, *args, **kwargs)
        self._rbuf = StringIO()

    def _read(self, size):
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
        self_recv = self.recv
        buf_write = buf.write
        while True:
            left = size - buf_len
            try:
                data = self_recv(left)
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
                buf_write(data)
                del data  # explicit free
                break
            assert n <= left, "recv(%d) returned %d bytes" % (left, n)
            buf_write(data)
            buf_len += n
            del data  # explicit free
            #assert buf_len == buf.tell()
        return buf.getvalue()

    def _readline(self):
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
        self__rbuf_write = self._rbuf.write
        self_recv = self.recv
        buf_write = buf.write
        while True:
            try:
                data = self_recv(8192)
            except error, e:
                if e.args[0] == EINTR:
                    continue
                raise
            if not data:
                break
            nl = data.find('\n')
            if nl >= 0:
                nl += 1
                buf_write(data[:nl])
                self__rbuf_write(data[nl:])
                del data
                break
            buf_write(data)
        return buf.getvalue()

    def _read_response(self):
        read = self._read
        readline = self._readline
        response = readline()
        byte = ord(response[0])
        if byte is 43: # ord('+')
            return response[1:-2]
        elif byte is 58: # ord(':')
            return int(response[1:])
        elif byte is 36: # ord('$')
            number = int(response[1:])
            if number == -1:
                return None
            else:
                return read(number+2)[:-2]
        elif byte is 42: # ord('*')
            number = int(response[1:])
            if number == -1:
                return None
            else:
                result = []
                result_append = result.append
                while number:
                    response = readline()
                    byte = ord(response[0])
                    if byte is 36: # ord('$')
                        result_append(read(int(response[1:])+2)[:-2])
                    else:
                        if byte is 58: # ord(':')
                            result_append(int(response[1:]))
                        else:
                            result_append(response[1:-2])
                    number -= 1
                return result
        elif byte is 45: #ord('-')
            return RedisError(response[1:-2])
        else:
            raise RedisError('bulk cannot startswith %r' % byte)

    def _execute_command(self, *args):
        """Executes a redis command and return a result"""
        data = '*%d\r\n' % len(args) + ''.join(['$%d\r\n%s\r\n' % (len(x), x) for x in args])
        self.send(data)
        return self._read_response()

    def _execute_yield_command(self, *args):
        """Executes a redis command and yield multiple results"""
        data = '*%d\r\n' % len(args) + ''.join(['$%d\r\n%s\r\n' % (len(x), x) for x in args])
        self.send(data)
        while 1:
            yield self._read_response()

    def _execute_command_1(self, arg1):
        data = '*1\r\n$%d\r\n%s\r\n' % (len(arg1), arg1)
        self.send(data)
        return self._read_response()

    def _execute_command_2(self, arg1, arg2):
        data = '*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n' % (len(arg1), arg1, len(arg2), arg2)
        self.send(data)
        return self._read_response()

    def _execute_command_3(self, arg1, arg2, arg3):
        arg3_ = str(arg3)
        data = '*3\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n' % (len(arg1), arg1, len(arg2), arg2, len(arg3_), arg3_)
        self.send(data)
        return self._read_response()

    def _execute_command_4(self, arg1, arg2, arg3, arg4):
        arg3_ = str(arg3)
        arg4_ = str(arg4)
        data = '*4\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n' % (len(arg1), arg1, len(arg2), arg2, len(arg3_), arg3_, len(arg4_), arg4_)
        self.send(data)
        return self._read_response()

