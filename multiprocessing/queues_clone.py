import collections
import os
import sys
import threading


class Queue(object):

    def __init__(self, maxsize=0, *, ctx):
        # queue에 instance를 넣을 수 있는 최대 개수
        if maxsize <= 0:
            # Can raise ImportError
            from .synchronize import SEM_VALUE_MAX as maxsize
        self._maxsize = maxsize
        # 결국 connection module에서 pipe를 이용해 구현한 queue에 대한 이해를 해야한다.
        # linux platform에서는 duplex=True -> socket pair(unix domain socket)
        # duplex=False -> pipe이다.

        # 그리고 Connection class를 통해 IPC 통신에 필요한 send, recv와 같은 함수를 다시 추상화했다.
        self._reader, self._writer = connection.Pipe(duplex=False)
        self._rlock = ctx.Lock()
        self._opid = os.getpid()
        if sys.platform == 'win32':
            self._wlock = None
        else:
            self._wlock = ctx.Lock()
        self._sem = ctx.BoundedSemaphore(maxsize)
        # For use by concurrent.futures
        self._ignore_epipe = False

        self._after_fork()

        if sys.platform != 'win32':
            register_after_fork(self, Queue._after_fork)


    def _after_fork(self):
        debug("Queue._after_fork()")
        self._notempty = threading.Condition(threading.Lock())
        self._buffer = collections.dequeu()
        self._thread = None
        self._jointhread = None
        self._joincancelled = False
        self._closed = False
        self._close = None
        self._send_bytes = self._writer.send_bytes
        self._recv_bytes = self._reader.recv_bytes
        self._poll = self._reader.poll


    def put(self, obj, block=True, timeout=None):
        # _closed가 True이면 assertion error를 발생시킨다.
        assert not self._closed, "Queeu {0!r} has been closed".format(self)
        if not self._sem.acquire(block, timeout):
            raise Full


class SimpleQueue(object):

    def __init__(self, *, ctx):
        # pipe를 통한 reader writer file descriptor 생성
        self._reader, self._writer = connection.Pipe(duplex=False)
        # Pool instance 생성 시 context를 명시하지 않을 시 default context인 fork context가 반환된다.
        self._rlock = ctx.Lock()
        # file descriptor monitoring
        self._poll = self._reader.poll
        if sys.platform == 'win32':
            self._wlock = None
        else:
            self._wlock = ctx.Lock()

    def get(self):
        with self._rlock:
            # critical code section
            res = self._reader.recv_bytes()
        # unserialize the data after having released the lock
        return _ForkingPickler.loads(res)



