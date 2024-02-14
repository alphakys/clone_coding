import os


class Popen(object):
    method = 'fork'
    def __init__(self, process_obj):
        util._flush_std_streams()
        self.returncode = None
        self.finalizer = None
        self._launch(process_obj)

    def _launch(self, process_obj):
        code = 1
        # pipe를 one pair를 사용한다는 것은 unidirectional한 communication을 하겠다는 의미이다.
        parent_r, child_w = os.pipe()
        self.pid = os.fork()
        if self.pid == 0:
            # child process에서 실행한 program
            try:
                os.close(parent_r)
                code = process_obj._bootstrap()
            finally:
                # Note The standard way to exit is sys.exit(n). _exit()
                # should normally only be used in the child process after a fork().
                os._exit(code)