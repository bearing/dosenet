import os
import functools
import errno
import os
import signal

class TimeoutError(Exception):
    pass

class timeout:
    def __init__(self, seconds=10, error_message=os.strerror(errno.ETIME)):
        self.seconds = seconds
        self.error_message = error_message
    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)
    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)
    def __exit__(self, type, value, traceback):
        signal.alarm(0)

def mkdir(path):
    if not os.path.isdir(path):
        print('MAKING DIRECTORY:', path)
        os.makedirs(path)
    else:
        pass

def get_console_width():
    try:
        rows, columns = os.popen('stty size', 'r').read().split()
    except ValueError:
        # happens within cron because there's no TTY
        return 0
    return int(columns)


def print_divider():
    print('-' * get_console_width())
