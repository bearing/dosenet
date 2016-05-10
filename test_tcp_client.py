#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import socket
import time
from contextlib import closing      # for the 'with' statement

server_address = 'dosenet.dhcp.lbl.gov'
server_port = 5100

# commons.lbl.gov/pages/viewpage.action?spaceKey=cpp&title=Perimeter+Protection
#   for open TCP ports on an LBL wired DHCP computer.
#   doesn't have to be 6900


def main():

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        print('Connecting...')
        s.connect((server_address, server_port))
        print('Connected')
        message1 = 'this is a message to send'
        print('  Sending message: {}'.format(message1))
        s.sendall(message1)
        print('...done')

    time.sleep(3)

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        print('Connecting...')
        s.connect((server_address, server_port))
        print('Connected')
        message2 = 'I have something else to say'
        print('  Sending message: {}'.format(message2))
        s.sendall(message2)
        print('...done')

    time.sleep(3)

if __name__ == '__main__':
    main()
