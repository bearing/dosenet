#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import socket
import time


server_address = 'dosenet.dhcp.lbl.gov'
server_port = 6900
# commons.lbl.gov/pages/viewpage.action?spaceKey=cpp&title=Perimeter+Protection
#   for open TCP ports on an LBL wired DHCP computer. doesn't have to be 6900

buffer_size = 1024

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((server_address, server_port))

# listen() sets the socket up as serverside
#   the number is the max number of connections.
s.listen(25)

while True:
    print('Waiting for connections...')

    # accept() takes a single connection, returning a new socket representing
    #   that specific connection.
    # accept() blocks until a connection is made.
    conn, addr = s.accept()
    print('Got a connection from {}'.format(addr))

    # This only handles a single connection! ~~~> see SocketServer
    try:
        while True:
            print('Waiting for data...')
            data, addr2 = conn.recvfrom(buffer_size)
            # conn.recvfrom blocks execution until data arrives - IF connection
            #   is still alive
            if data:
                print('  Received data from {}: {}'.format(addr2, data))
            else:
                # Apparently this means the connection was closed
                break

    finally:
        conn.close()
