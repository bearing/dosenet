import os

def get_console_width():
    rows, columns = os.popen('stty size', 'r').read().split()
    return int(columns)

def print_divider():
    print('-' * get_console_width())
