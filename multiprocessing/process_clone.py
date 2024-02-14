import itertools
import os
import sys
import threading

_process_counter = itertools.count(1)
_current_process = _MainProcess()
_children = set()

_dangling = WeakSet()

class BaseProcess(object):
    """
    Process objects represent activity that is run in a separate process

    The class is analogous to 'threading.Thread'
    """

    def _Popen(self):
        raise NotImplementedError

    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, *, daemon=None):
        # group is not None -> assert
        assert group is None, "group argument must be None for now"
        count = next(_process_counter)
        # _MainProcess의 속성 _identity -> () 따라서 (1,) (2, )와 같이 증가할 것이다.
        self._identity = _current_process._identity + (count, )
        # _current_process._config => {'authkey': AuthenticationString(os.urandom(32)),
        #                         'semprefix': '/mp'}
        self._config = _current_process._config.copy()
        self._parent_pid = os.getpid()
        self._popen = None
        self._closed = False
        self._target = target
        self._args = tuple(args)
        self._kwargs = dict(kwargs)
        self._name = name or type(self).__name__ + '_' + \
                    ':'.join(str(i) for i in self._identity)

        if daemon is not None:
            self.daemon = daemon
        _dangling.add(self)

    def _check_closed(self):
        if self._closed:
            raise ValueError("process object is closed")

    def run(self):
        """
        Method to be run in sub-process; can be overridden in sub-class
        """
        if self._target:
            self._target(*self._args, **self._kwargs)


    def start(self):
        """
        Start child process
        """
        # check flag가 True이면 ValueError
        self._check_closed()
        # popen은 child process를 create함. low-level의 모듈(os library)을 사용하여
        # child process를 생성하는 object
        # popen을 통해 process 생성 후에는 None이 아니다
        assert self._popen is None, "cannot start a process twice"
        assert self._parent_pid == os.getpid(), "can only start a process object created by current process"
        # daemonic process means that if parent process terminated then child process is terminated automatically
        assert not _current_process._config.get("daemon"), "daemonic processes are not allowed to have children"
        _cleanup()
        # def __init__(self, process_obj):
        self._popen = self._Popen(self)
        self._sentinel = self._popen.sentinel
        # Avoid a refcycle if the target function holds an indirect
        # reference to the process object
        del self._target, self._args, self._kwargs
        # _children은 set 자료구조이다.
        _children.add(self)

    def _bootstrap(self):
        """
        In software development, bootstrapping refers to the process of starting a complex
        In software development, bootstraping refers to the process of starting a complex
        system with limited resources, gradually building upon itself until it becomes fully functional.
        system with limited resources, gradually building upon itself until it becomes fully functional
        """
        from multiprocessing import util, context
        global _current_process, _process_counter, _children

        try:
            # _start_method는 run method에서 popen 호출
            # -> popen launch method에서 bootstrap 호출
            if self._start_method is not None:
                context._force_start_method(self._start_method)
            _process_counter = itertools.count(1)
            _children = set()
            util._close_stdin()
            old_process = _current_process
            _current_process = self
            try:
                util._finalizer_registry.clear()
                util._run_after_forkers()
            finally:
                # delay finalization of the old process object until after
                # _run_after_forkers() is executed
                del old_process
            util.info("child process calling self.run()")
            try:
                # target function을 실행한다.
                self.run()
                exitcode = 0
            finally:
                util._exit_function()
        except SystemExit as e:
            if not e.args:
                exitcode = 1
            elif isinstance(e.args[0], int):
                exitcode = e.args[0]
            else:
                sys.stderr.write(str(e.args[0] + '\n'))
                exitcode = 1

        except:
            exitcode = 1
            import traceback
            sys.stderr.write("Process %s:\n" % self.name)
            traceback.print_exc()
        finally:
            threading._shutdown()
            util.info("process exiting with exitcode %d" % exitcode)
            util._flush_std_streams()

        return exitcode


#
# class BaseProcess(object):
#     """
#     Process objects represent activity that is run in a separate process
#
#     The class is analogous to 'threading.Thread'
#     """
#
#     def _Popen(self):
#         raise NotImplementedError
#
#     def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, *, daemon=None):
#         # group parameter의 argument가 None이 아니면
#         assert group is None, 'group argument must be None for now'
#         # itertools.count를 사용하여 숫자 1부터 시작하여 1 step씩 next 함수를 통해 iterator가 값을 리턴.
#         count = next(_process_counter)
#         self._identity = _current_process._identity + (count,)
#         self._config = _current_process._config_copy()
#         self._parent_pid = os.getpid()
#         self._popen = None
#         self._closed = False
#         self._target = target
#         # 빈 튜플을 다시 tuple instance를 만들면 빈 튜플이 된다.
#         self._args = tuple(args)
#         self._kwargs = dict(kwargs)
#         self._name = name or type(self).__name__ + '-' + \
#                      ':'.join(str(i) for i in self._identity)
#         if daemon is not None:
#             self.daemon = daemon
#         _dangling.add(self)
#
#     def _check_closed(self):
#         if self._closed:
#             raise ValueError("process object is closed")
#
#     def run(self):
#         """
#         Method to be run in sub-process; can be overidden in sub-class
#         """
#         if self._target:
#             self._target(*self._args, **self._kwargs)
#
#     def start(self):
#         """
#         Start child process
#         """
#         self._check_closed()
#         assert self._popen is None, "cannot start a process twice"
#         # 현재 process fork, spawn, forkserver 하지 않은 상황의 현재 process에서 fork 되어야만 한다.
#         assert self._parent_pid == os.getpid(), "can only start a process object created by current process"
#         # 중요 daemomn=True process는 threading module과 마찬가지로 join을 실행하지 않았기 때문에 parent process가 종료되면 적절한
#         # resource들에 대한 정리(gracefull exit)가 되지 않은 채로 끝나기 때문에 daemon=True인 process라면 child process를
#         # 가지지 않게 한다고 추정된다.
#         assert not _current_process._config.get("daemon"), \
#             "daemonic processes are not allowed to have children"
#         _cleanup()
#         # Popen method는 BasePorcess를 상속하는 class에서 구현되어야 할 것!
#         self._popen = self._Popen(self)
#         self._sentinel = self._popen.sentinel
#         # Avoid a refcycle if the target function holds an indirect
#         # reference to the process object
#         del self._target, self._args, self._kwargs
#         _children.add(self)
#
#     def terminate(self):
#         """
#         Terminate process; sends SIGTERM signal or uses TerminateProcess()
#         """
#         self._check_closed()
#         self._popen.terminate()
#
#     def kill(self):
#         """
#         Terminate process; sends SIGKILL signal or uses TerminateProcess()
#         """
#         self._check_closed()
#         self._popen.kill()
#
#     def join(self, timeout=None):
#         """
#         Wait until child process terminates
#         """
#         self._check_closed()
#         assert self._parent_pid == os.getpid(), 'can only join a child process'
#         assert self._popen is not None, "can only join a started process"
#         res = self._popen.wait(timeout)
#         if res is not None:
#             _children.discard(self)
#
#     def _bootstrap(self):
#         """
#         In software development, bootstrapping refers to the process of starting a complex
#          system with limited resources, gradually building upon itself until it becomes fully functional.
#         """
#         from . import util, context
#         # _current_process = _MainProcess()
#         # _process_counter = itertools.count(1)
#         # _children = set()
#         global _current_process, _process_counter, _children
#
#         try:
#             if self._start_method is not None:
#                 context._force_start_method(self._start_method)
#             _process_counter = itertools.count(1)
#             _children = set()
#             util._close_stdin()
#             old_process = _current_process
#             _current_process = self
#
#             try:
#                 util._finalizer_registry.clear()
#                 util._run_after_forkers()
#             finally:
#                 del old_process
#             util.info("child process calling self.run()")
#             try:
#                 self.run()
#                 exitcode = 0
#
#
#
# class _MainProcess(BaseProcess):
#
#     def __init__(self):
#         self._identity = ()
#         self._name = 'MainProcess'
#         self._parent_pid = None
#         self._popen = None
#         self._closed = False
#         self._config = {'authkey': AuthenticationString(os.urandom(32)),
#                         'semprefix': '/mp'}
