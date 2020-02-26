import os
import functools
import errno
import signal

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
