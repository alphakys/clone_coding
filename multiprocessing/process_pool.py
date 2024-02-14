# import inspect
import _thread
import io
import marshal
import multiprocessing
import os
import pickle
import select
import signal
# import queue
import subprocess
import sys
import threading
import time
import random
import types
import fcntl

from queue import Queue
# from setproctitle import setproctitle


class MultiProcessingPool(object):

    def __init__(self, processes, max_tasks):

        self.maxtasks = max_tasks
        self.read_pipes = []
        self.write_pipes = []
        self.pid = None
        self._create_multiple_process(processes, max_tasks)

        print(self.read_pipes)
        print(self.write_pipes)
        self.run_main_loop()

    def _create_multiple_process(self, process_num, max_tasks):

        for i in range(process_num):
            self._fork_process(max_tasks, i)
            # child process의 program이 실행되는 시간을 만들어준다.
            # sleep를 하지 않으면 parent process의 program이 연속적으로 실행될 때도 있기 때문에.
            time.sleep(0.1 * random.random())

    def _fork_process(self, max_tasks, num):
        worker = WorkerProcess(max_tasks)
        parent_read, child_write = os.pipe()
        child_read, parent_write = os.pipe()

        self.open_fds()

        pid = os.fork()

        if pid != 0:
            os.close(child_write)
            os.close(child_read)
            self.open_fds()
            self.pid = pid
            # [memo] 중요
            #  file descriptor set Non-Blocking mode
            # fcntl.fcntl(parent_write, fcntl.F_SETFL, os.O_NONBLOCK)
            # fcntl.fcntl(parent_read, fcntl.F_SETFL, os.O_NONBLOCK)

            self.read_pipes.append(parent_read)
            self.write_pipes.append(parent_write)

            return pid

        # Process Child
        self.open_fds()
        worker.parent_pid = self.pid
        os.close(parent_write)
        os.close(parent_read)
        self.open_fds()
        worker.read_pipe.append(child_read)
        worker.write_pipe.append(child_write)
        try:
            worker.init_process()
            sys.exit(0)
        except SystemExit:
            raise

    def init_signals(self):
        pass

    def run_main_loop(self):
        threading.Thread(target=self.wait_response)

    def wait_response(self):
        while True:
            try:
                print("Parent process waiting response from child", file=sys.stdout)
                ret = select.select(self.read_pipes, [], [], 10)
                print("ret : ", ret, file=sys.stdout)
                # print("Parse response from child", file=sys.stdout)
                ret_pipe = ret[0]
                if ret_pipe:
                    try:
                        read_fd = os.fdopen(ret_pipe[0], 'r', encoding='utf-8')
                        print(read_fd.read())
                    except Exception as e:
                        print(e)
            except select.error as e:
                print(e)
                break

    def open_fds(self):
        cmd = subprocess.run(["ls", "-al", f"/proc/{os.getpid()}/fd"], capture_output=True, text=True)
        print(f"{cmd.stdout}\n {os.getpid()} \n", file=sys.stdout)


    def apply_func(self, func, args=()):
        if not hasattr(args, '__iter__'):
            raise TypeError("args must be iterable")

        # https://stackoverflow.com/questions/64095346/pickle-how-does-it-pickle-a-function
        # how to pickle a user defined function in python

        # [memo] 아주 중요한 부분 지금까지 locals.func 속성을 가진 function들이 pickling 되지 않았던 이유가
        #   __import__ 함수에서 생기는 문제였다!!!

        # func_bytecode = marshal.dumps(func.__code__)
        write_pipe = self.write_pipes[0]
        write_fd = os.fdopen(write_pipe, 'wb')

        try:
            marshal.dump(func.__code__, write_fd)
        except OSError as e:
            print(e)
        except Exception as e:
            print(e)

    def notify(self):

        while True:
            pass


class WorkerProcess(object):

    def __init__(self, max_tasks):
        self.max_tasks = max_tasks
        self.read_pipe = []
        self.write_pipe = []
        self.parent_pid = None

    def wakeup_master(self):
        pass
        # self.write_pipe

    def init_process(self):
        print(f"init process {os.getpid()}", file=sys.stdout)
        self.wait_task()

    def wait_task(self):
        # child process의 main loop
        try:
            while True:
                print(f"waiting task from {os.getpid()}", file=sys.stdout)
                ret = select.select(self.read_pipe, [], [], 10)
                print("worker ret : ", ret)
                conn = ret[0]
                if conn:
                    print("read success", file=sys.stdout)
                    fd = os.fdopen(conn[0], 'rb')

                    raw_bytecode = marshal.load(fd)
                    apply_function = types.FunctionType(raw_bytecode, globals())

                    self.apply_func(apply_function, args=(10, ))

        except select.error as e:
            print("worker select error: ", e)

    def apply_func(self, func, args=()):

        try:
            ret = func(*args)
            write_fd = os.fdopen(self.write_pipe[0], 'w', encoding='utf-8')
            write_fd.write(str(ret))
        except Exception as e:
            print(e)
        except OSError as e:
            print("os error:", e)
