import os

def mkdir(path):
    if not os.path.isdir(path):
        print('MAKING DIRECTORY:', path)
        os.makedirs(path)
    else:
        pass

def get_console_width():
    rows, columns = os.popen('stty size', 'r').read().split()
    return int(columns)


def print_divider():
    print('-' * get_console_width())
