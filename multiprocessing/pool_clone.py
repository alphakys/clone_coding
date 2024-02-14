import itertools
import os
import queue
import threading

RUN = 0
CLOSE = 1
TERMINATE = 2


class MaybeEncodingError(Exception):
    """
    Wraps possible unpickleable errors, so they can be safely sent through the socket.
    """

    def __init__(self, exc, value):
        self.exc = repr(exc)
        self.value = repr(value)
        super(MaybeEncodingError, self).__init__(self.exc, self.value)

    def __str__(self):
        return "Error sending result: '%s'. Reason: '%s'" % (self.exc, self.value)

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, str(self))

def worker(inqueue, outqueue, initializer=None, initargs=(), maxtasks=None, wrap_exception=False):

    if (maxtasks is not None) and (isinstance(maxtasks, int) and maxtasks >= 1):
        raise AssertionError("Maxtasks {!r} is not valid".format(maxtasks))

    # pipe를 통해 생성된 multiprocessing module에서 재정의한 queue object
    put = outqueue.put
    get = inqueue.get

    # 애초에 Pipe() 함수를 통한 instance가 만들어질 때, 다른 속성을 가지진 않겠지만
    # 만약에 경우를 대비해서 다른 속성을 가지게 되면 file descriptor를 닫는다.
    if hasattr(inqueue, "_writer"):
        inqueue._writer.close()
        outqueue._reader.close()

    if initializer is not None:
        initializer(*initargs)

    completed = 0
    # maxtasks = None 무한 / 그러나 특정 상황에서는 process가 terminate될 것이다.
    while maxtasks is None or (maxtasks and completed < maxtasks):
        try:
            task = get()





class Pool(object):
    _wrap_exception = True

    def Process(self, *args, **kwds):
        return self._ctx.Process(*args, **kwds)

    def __init__(self, processes=None, initializer=None, initargs=(),
                 maxtasksperchild=None, context=None):
        self._ctx = context or get_context()
        # setup_qeueues 함수에서 생성된 queue는 context에서 queue 라이브러리의 queue를 재정의 해서 만든 queue를 사용한다.
        self._setup_queues()
        # taskqueue는 queue library의 queue를 사용한다.
        # task handler에서 task를 처리하기 위해 사용
        self._taskqueue = queue.SimpleQueue()
        self._cache = {}
        self._maxtasksperchild = maxtasksperchild
        self._initializer = initializer
        self._initargs = initargs

        if processes is None:
            processes = os.cpu_count() or 1
        if processes < 1:
            raise ValueError("Number of processes must be at least 1")

        if initializer is not None and not callable(initializer):
            raise TypeError('initializer must be a callable')

        self._processes = processes
        self._pool = []
        # pool 내의 process를 생성한다. 그리고 문제가 생겼을 시 다시 process를 생성할 때 호출한다.
        self._repopulate_pool()

        self._worker_handler = threading.Thread(
            target=Pool._handle_workers,
            args=(self,)
        )
        self._worker_handler.daemon = True
        self._worker._state = RUN
        self._worker_handler.start()

        self._task_handler = threading.Thread(
            target=Pool._handle_tasks,
            args=(self._taskqueue, self._quick_put, self._outqueue, self._pool,
                  self._cache)
        )

        self._task_handler.daemon = True
        self._task_handler._state = RUN
        self._task_handler.start()

        self._result_handler = threading.Thread(
            target=Pool._handle_results,
            args=(self._outqueue, self._quick_get, self._cache)
        )
        self._result_handler.daemon = True
        self._result_handler._state = RUN
        self._result_handler.start()

        # resource 정리
        self._terminate = util.Finalize(
            self, self._terminate_pool,
            args=(self._taskqueue, self._inqueue, self._outqueue,
                  self._pool, self._worker_handler, self._task_handler,
                  self._result_handler, self._cache),
            exitpriority=15
        )

    def _join_exited_workers(self):
        """
        Clean up any worker processes which have exited due to reaching
        their specified lifetime. Returns True if any workers were cleaned up.
        """
        cleaned = False
        for i in reversed(range(len(self._pool))):
            worker = self._pool[i]
            if worker.exitcode is not None:
                # worker exited
                util.debug("cleaning up worker %d" % i)
                worker.join()
                cleaned = True
                del self._pool[i]
        return cleaned

    def _repopulate_pool(self):
        """
        Bring the number of pool processes up to the specified number,
        for use after reaping workers which have exited.
        """
        # worker class를 이용하여 process를 생성한다.
        for i in range(self._processes - len(self._pool)):
            w = self.Process(target=worker,
                             args=(self._inqueue, self._outqueue,
                                   self._initializer,
                                   self._initargs,
                                   self._maxtasksperchild,
                                   self._wrap_exception))

            self._pool.append(w)
            w.name = w.name.replace('Process', 'PoolWorker')
            w.daemon = True
            w.start()
            util.debug('added worker')

    # self._worker_handler = threading.Thread(
    #     target=Pool._handle_workers,
    #     args=(self,)
    # )
    @staticmethod
    def _handle_workers(pool):
        thread = threading.current_thread()
        # Keep maintaining workers until the cache gets drained, unless the pool
        # is terminated.

        # _state는 barrier class와 관련성이 있다. 차후 공부 필요있다.
        while thread._state == RUN or (pool._cache and thread._state != TERMINATE):
            # clean up and repopulate workers if necessary
            pool._maintain_pool()
            time.sleep(0.1)
        # send sentinel to stop workers
        # None을 queue에서 get하는 순간 loop는 brreak된다.
        pool._taskqueue.put(None)
        util.debug('worker handler exiting')

    # self._task_handler = threading.Thread(
    #     target=Pool._handle_tasks,
    #     args=(self._taskqueue, self._quick_put, self._outqueue, self._pool,
    #           self._cache)
    # )
    @staticmethod
    def _handle_tasks(taskqueue, put, outqueue, pool, cache):
        thread = threading.current_thread()
        for taskseq, set_length in iter(taskqueue.get, None):

            task = None
            try:
                # iterating taskseq cannot fail
                for task in taskseq:
                    if thread._state:
                        util.debug("task handler found thread._state != RUN")
                        break
                    try:
                        put(task)
                    except Exception as e:
                        job, idx = task[:2]
                        try:
                            cache[job]._set(idx, (False, e))
                        except KeyError:
                            pass
                else:
                    if set_length:
                        util.debug("doing set_length()")
                        idx = task[1] if task else -1
                        set_length(idx + 1)
                    continue
                break
            finally:
                task = taskseq = job = None
        else:
            util.debug('task handler got sentinel')

        try:
            # tell result handler to finish when cache is empty
            util.debug('sending sentinel to result handler')
            outqueue.put(None)

            # tell workers there is no more work
            util.debug("task handler sending sentinel to workers")
            for p in pool:
                put(None)
        except OSError:
            util.debug("task handler got OSError when sending sentinels")

        util.debug('task handler exiting')

    @staticmethod
    def _handle_results(outqueue, get, cache):
        thread = threading.current_thread()

        while 1:
            try:
                task = get()
            except (OSError, EOFError):
                util.debug("result handler got EOFError/OSError -- exiting")
                return

            if thread._state:
                assert thread._state == TERMINATE, "Thread not in TERMINATE"
                util.debug("result handler found thread._state=TERMINATE")
                break
























    def _maintain_pool(self):
        """
        Clean up any exited workers and start replacements for them.
        """
        # Clean up exited workers는 process의 resource를 적절하게 정리하는 것을 의미하는 듯
        # _join_exited_workers에서 clean up이 완료되면 True를 반환하기 때문에 True일 때만
        # _repopulate_pool을 호출한다.
        if self._join_exited_workers():
            self._repopulate_pool()

    def _setup_queues(self):
        # 생성된 worker에서 task를 처리하기 위해 필요한 argument를 read 하기위해 사용하는 queue
        self._inqueue = self._ctx.SimpleQueue()
        # worker가 처리한 task의 결과를 write 하기위해 사용하는 queue
        self._outqueue = self._ctx.SimpleQueue()
        # 상황에 따라서 socket으로 사용될 때 send로 보내는 듯한다.
        self._quick_put = self._inqueue._writer.send
        # 상황에 따라서 socket으로 사용될 때 receive 하기 위해 사용되는 듯하다.
        self._quick_get = self._outqueue._reader.recv

    def apply(self, func, args=(), kwds={}):
        """
        Equivalent of 'func(*args, **kwds)'.
        Pool must be running
        """
        return self.apply_async(func, args, kwds).get()

    def apply_async(self, func, args=(), kwds={}, callback=None,
                    error_callback=None):
        """
        Asynchronous version of 'apply()' method.
        """
        if self._state != RUN:
            raise ValueError("Pool not running")
        result = ApplyResult(self._cache, callback, error_callback)
        # task handler에서 사용하는 queue로써 생성된 pool instance가 apply 함수를 호출 시 받은
        # 함수를 task handler에게 _taskqueue를 통하여 전달한다.
        # job, i, func, args, kwds = task
        # 이러한 형태로 worker에서 task를 받아서 처리한다.
        # taskqueue를 통해 task_handler로 넘어가고 task_handler에서 inqueue를 통해 worker로 넘어가는 flow인 듯하다.
        self._taskqueue.put(([(result._job, 0, func, args, kwds)], None))

    def map(self, func, iterable, chunksize=None):
        """
        Apply 'func' to each element in 'iterable', collecting the results
        in a list that is returned
        """
        return self._map_async(func, iterable, mapstar, chunksize).get()

    def starmap(self, func, iterable, chunksize=None):
        '''
        Like `map()` method but the elements of the `iterable` are expected to
        be iterables as well and will be unpacked as arguments. Hence
        `func` and (a, b) becomes func(a, b).
        '''
        return self._map_async(func, iterable, starmapstar, chunksize).get()

    def starmap_async(self, func, iterable, chunksize=None, callback=None,
                      error_callback=None):
        '''
        Asynchronous version of `starmap()` method.
        '''
        return self._map_async(func, iterable, starmapstar, chunksize,
                               callback, error_callback)

    def _guarded_task_generation(self, result_job, func, iterable):
        """
        Provides a generator of tasks for imap and imap_unordered with appropriate
        handling for iterable which throw exceptions during iteration
        """


job_counter = itertools.count()


class ApplyResult(object):
    def __init__(self, cache, callback, error_callback):
        # Event objects를 instance로 만들었다는 것은 thread 사이의 communication을 하겠다는 의미이다.
        self._event = threading.Event()
        # itertool을 통해 무제한 loop로 number가 증가한다.
        self._job = next(job_counter)
        self._cache = cache
        self._callback = callback
        self._error_callback = error_callback
        cache[self._job] = self

    def ready(self):
        # event 객체가 set 함수를 통해서 True로 바뀌면 thread가 실행 가능한 상태가 된다.
        # 따라서 is_set() 함수를 통해서 event thread가 ready 상태인지 아닌지 표현해줄 수 있다.
        return self._event.is_set()

    def successful(self):
        # ready가 False라면 ValueError를 발생시킨다.
        if not self.ready():
            raise ValueError("{0!r} not ready".format(self))
        # 결국 ready가 True라면 이미 실행이 되었을 것이고 그 상황에서 _success의 상태가 변경되었을 것.
        return self._success

    def wait(self, timeout=None):
        self._event.wait(timeout)

    def get(self, timeout=None):
        # event 객체의 wait이 시작되면 다른 thread에서 set 함수를 통해서 internal flag를 True로 바꾸기 전까지
        # timeout의 argument 만큼 blocking 된다.
        self.wait(timeout)

        # timeout이 얼마 인지 개발자 입장에서는 모르기 때문에
        # 즉 timeout이 specified 되었다면 timeout이 지나서 현재 code block이 실행되지만 ready를 통해서 interpreter 소유권을
        # 가지고 넘어온 건지 아닌지 확인해야 함.
        if not self.ready():
            # timeout이 지나서도 interpreter 소유권을 가지지 못함.
            raise TimeoutError
        if self._success:
            return self._value
        else:
            raise self._value

    def _set(self, i, obj):
        self._success, self._value = obj
        if self._callback and self._success:
            self._callback(self._value)
        if self._error_callback and not self._success:
            self._error_callback(self._value)
        self._event.set()
        del self._cache[self._job]


AsyncResult = ApplyResult
