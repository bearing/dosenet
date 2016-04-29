#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import socket
import SocketServer
import time


server_address = 'dosenet.dhcp.lbl.gov'
server_port = 6898
# commons.lbl.gov/pages/viewpage.action?spaceKey=cpp&title=Perimeter+Protection
#   for open TCP ports on an LBL wired DHCP computer. doesn't have to be 6900


class MyHandler(SocketServer.BaseRequestHandler):
    """Define the handle() method to handle requests"""

    def handle(self):
        data = self.request.recv(1024)
        print(self.client_address, data)


if __name__ == '__main__':
    try:
        server = SocketServer.TCPServer(
            (server_address, server_port), MyHandler)
        server.serve_forever()
    finally:
        server.socket.close()
        server.server_close()
