#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import socket
import time


def get_connection(
        server_address='dosenet.dhcp.lbl.gov',
        server_port=6900):

    # commons.lbl.gov/pages/viewpage.action?spaceKey=cpp&title=Perimeter+Protection
    #   for open TCP ports on an LBL wired DHCP computer.
    #   doesn't have to be 6900

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    s.connect((server_address, server_port))

    return s


def main():
    print('Connecting...')
    s = get_connection()
    print('Connected')

    message1 = 'this is a message to send'
    print('Sending message: {}'.format(message1))
    s.sendall(message1)
    print('...done')

    time.sleep(5)

    message2 = "I want to send another message"
    print('Sending message: {}'.format(message2))
    s.sendall(message2)
    print('...done')

    time.sleep(10)

if __name__ == '__main__':
    main()
