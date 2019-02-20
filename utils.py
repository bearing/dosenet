import os
import functools
import errno
import os
import signal

class TimeoutError(Exception):
    pass

def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            except TimeoutError:
                pass
            finally:
                signal.alarm(0)
            return result

        return wrapper

    return decorator

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
