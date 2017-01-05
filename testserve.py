from __future__ import print_function
import SocketServer
import socket
from contextlib import closing
import time


HOSTNAME = ''
PORT = 5102

return_msg = 'Return!'
buffer_size = 16


class MyTcpServer(SocketServer.TCPServer):
    pass


class TcpHandler(SocketServer.StreamRequestHandler):

    def handle(self):
        read_method = self.read4
        write_method = self.write1

        data = read_method()
        print('{}:   Got data of length {}: {}'.format(
            time.time(), len(data), data))
        write_method()
        print('{}:   Done'.format(time.time()))

    def read1(self):
        print('{}: read1'.format(time.time()))
        data = self.rfile.read()
        return data

    def read2(self):
        print('{}: read2'.format(time.time()))
        data = self.request.recv(buffer_size)
        return data

    def read3(self):
        print('{}: read3'.format(time.time()))
        data = []
        databuf = self.request.recv(buffer_size)
        while databuf:
            data.append(databuf)
            databuf = self.request.recv(buffer_size)
        return ''.join(data)

    def read4(self):
        print('{}: read4'.format(time.time()))
        while True:
            data = self.request.recv(buffer_size)
            if not data:
                break
            return data

    def write1(self):
        print('{}: write1'.format(time.time()))
        self.request.sendall(return_msg)


def serve():
    tcp_server = MyTcpServer((HOSTNAME, PORT), TcpHandler)
    try:
        tcp_server.serve_forever()
    except socket.error as e:
        print(e)
    except KeyboardInterrupt:
        print('exiting')
        tcp_server.shutdown()
        tcp_server.server_close()


def sendall_data(data):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.settimeout(5)
        s.connect((HOSTNAME, PORT))
        print('{}: sending len {}: {}'.format(time.time(), len(data), data))
        s.sendall(data)
        print('{}:   finished sending'.format(time.time()))
        try:
            received = s.recv(1024)
        except socket.timeout:
            print('{}:   timeout!'.format(time.time()))
        else:
            print('{}: received len {}: {}'.format(
                time.time(), len(received), received))
