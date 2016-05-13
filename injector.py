#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Run on DoseNet Server!

Authors:
    Brian Plimley (around 2016-05-10)
    Joseph Curtis (after 2016-04-10)
    Navrit Bal (after 2015-06-15)
    Ryan Pavlovsky (until 2015-06-15)
Affiliation:
    DoseNet
    Applied Nuclear Physics Division
    Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
Adapted from:
    udp_injector.py (Ryan Pavlovsky)
Last updated:
    2016-04-10
"""
from __future__ import print_function

import sys
import os
import time
import argparse
import socket
import SocketServer
import datetime
from collections import OrderedDict
import multiprocessing

# Extensible way for adding future imports
import_list = ['crypt', 'mysql', 'udp']
for el in import_list:
    sys.path.append(os.path.abspath(os.path.join(os.getcwd(), el)))
from crypt import cust_crypt as ccrypt
# from udp.udp_tools import custSocket
from mysql.mysql_tools import SQLObject

PRIVATE_KEY = os.path.expanduser('~/.ssh/id_rsa_lbl')

UDP_PORT = 5006     # testing!
TCP_PORT = 5100


class Injector(object):
    """
    Inserts decrypted and validated data from the Raspberry Pi's
    into a MySQL database.

    Args:
        -v (Optional): Is more verbose.

    Attributes:
        db (SQLObject): Custom MySQL database object for injecting into.
        cursor (db.cursor): Used for accessing database returns.
    """

    def __init__(self, verbose=False, testing=False,
                 private_key=PRIVATE_KEY,
                 udp_port=UDP_PORT, tcp_port=TCP_PORT,
                 ip=None, **kwargs):
        """
        Initialise decryption & database objects.

        Attributes:
            verbose (bool) - add verbosity. Default: False
            testing (bool) - run in test mode with artificial packets.
                Default: False
            private_key (str) - Fully qualified static path of private key
                Default: '~/.ssh/id_rsa_lbl'
            udp_port (int) - Which port to listen for UDP data on.
                Default: 5005
            tcp_port (int) - which port to listen for TCP data on.
                Default: 5100
            ip (String) - Which address to listen on.
                Default: dynamically determined
        """
        self.verbose = verbose
        self.testing = testing
        self.private_key = private_key
        self.udp_port = udp_port
        self.tcp_port = tcp_port
        self.test_packet = None

        # Connect to database
        self.db = SQLObject()
        print('\tDataBase:', self.db)
        print('\tDataBase Cursor:', self.db.cursor)

        # Decryptor
        print('\tPrivate Key:', self.private_key)
        de = ccrypt.public_d_encrypt(key_file_lst=[self.private_key])
        print('\tDecryptor:', de)
        self.de = de

        # Get ip information
        if ip is None:
            # Gets actual IP address on the internet
            self.ip = self.get_external_ip()
        else:
            assert isinstance(ip, str), 'IP is not a string: {}'.format(ip)
            self.ip = ip
        print('\tIP:', self.ip)
        print('\tPort:', self.port)

        if not self.testing:
            # Uses 1 private key
            # self.socket = custSocket(ip=self.ip, port=self.port, decrypt=de)
            self.udp_server = DosenetUdpServer(
                (self.ip, self.udp_port), UdpHandler, injector=self)
            self.tcp_server = DosenetTcpServer(
                (self.ip, self.tcp_port), TcpHandler, injector=self)
        else:
            self.socket = None
            print('\tTesting Mode! Not setting up UDP socket')

    @staticmethod
    def get_external_ip():
        """
        'Hey Google, what's my IP address?'
        """

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip_addr = s.getsockname()[0]
        s.close()
        return ip_addr

    def test(self):
        """
        Test packet handling with make_test_packet() and handle().

        Blocks execution.
        """

        while True:
            time.sleep(0.5)
            packet = self.make_test_packet()
            # handle(packet)....

    def listen(self):
        """
        Gather, decrpyt, validate and then inject data until keyboard interrupt
        or system exit.

        Attributes:
            data (socket): Gathered, decrypted, validated in custom socket
              object.

        Raises:
            KeyboardInterrupt: User interrupt to exit the infinite loop.
            SystemExit: System interrupt to exit the infinite loop.
        """

        print('\n\t\t\t ~~~~ Listening ~~~~')
        self.udp_process = multiprocessing.Pool(
            target=self.udp_server.serve_forever)
        self.tcp_process = multiprocessing.Pool(
            target=self.tcp_server.serve_forever)

    def make_test_packet(self):
        """
        Put together a test message
        """

        if self.test_packet is None:
            inj_stat = self.db.getInjectorStation()
            test_hash = inj_stat['IDLatLongHash']
            test_id = inj_stat.name
            test_cpm = 1.
            test_cpm_error = 0.5
            test_error_flag = 0
            self.test_packet = '{},{},{},{},{}'.format(
                test_hash, test_id, test_cpm, test_cpm_error, test_error_flag)
        return self.test_packet


class DosenetUdpServer(SocketServer.UDPServer):
    """
    Server object to handle UDP requests.

    The Dosenet*Server classes add allow_reuse_address = True
    (which should force binding to a port, even if the past server hasn't
    released it yet).

    Also override the verify_request() method, in order to validate the data.
    """

    allow_reuse_address = True
    # http://stackoverflow.com/questions/3911009/python-socketserver-baserequesthandler-knowing-the-port-and-use-the-port-already

    def __init__(self, injector=None, *args):
        super(DosenetUdpServer, self).__init__(*args)
        self.injector = injector

    def verify_request():
        pass


class DosenetTcpServer(SocketServer.TCPServer):
    """
    Server object to handle TCP requests.

    The Dosenet*Server classes add allow_reuse_address = True
    (which should force binding to a port, even if the past server hasn't
    released it yet).

    Also override the verify_request() method, in order to validate the data.
    """

    allow_reuse_address = True

    def __init__(self, injector=None, *args):
        super(DosenetUdpServer, self).__init__(*args)
        self.injector = injector

    def verify_request():
        pass


class UdpHandler(SocketServer.DatagramRequestHandler):
    """
    Contains the code to handle one request.
    https://docs.python.org/2/library/socketserver.html#request-handler-objects

    Keep this lightweight: It's instantiated for every request the server
    receives. (The handling code is ultimately the same, but let's not create
    a pile of object attributes.)
    """

    def handle(self):
        data = self.rfile.readline()
        # or try self.request.recv(1024)

        self.server.injector.handle(
            data, client_address=self.client_address, request=self.request,
            mode='udp')


class TcpHandler(SocketServer.StreamRequestHandler):
    """
    Contains the code to handle one request.
    https://docs.python.org/2/library/socketserver.html#request-handler-objects

    Keep this lightweight: It's instantiated for every request the server
    receives. (The handling code is ultimately the same, but let's not create
    a pile of object attributes.)
    """

    def handle(self):
        data = self.rfile.readline()

        self.server.injector.handle(
            data, client_address=self.client_address, request=self.request,
            mode='tcp')


##################
    def handle(self):
        """
        """

        # Print message
        self.print_status(packet)
        # Parse data
        try:
            data = self.parse_packet(packet, verbose=self.verbose)
        except Exception as e:
            print('Parsing error:', e)
            continue
        # Check CPM
        if data['cpm'] > 100:
            print('CPM > 100 (assuming noise) NOT INJECTING')
            continue
        # Inject into database
        if self.verbose:
            self.print_status('Trying to inject')
        try:
            self.db.inject(data)
        except Exception as e:
            print('Injection error:', e)
            continue
        self.print_status('Successfully injected!')

    def print_status(self, s):
        print('[{}] {}'.format(str(datetime.datetime.now()), s))


def parse_packet(packet, verbose=False):
    # Commas are our delimiters for the decrypted message
    packet = packet.split(',')
    assert len(packet) == 5, 'Packet does not have exactly 5 fields: {}'.format(packet)
    data = OrderedDict()
    data['hash'] = packet[0]
    assert len(data['hash']) == 32, 'Hash not len 32: {}'.format(data['hash'])
    # All is good. Cast as known data types
    data['stationID']   = int(packet[1])
    data['cpm']         = float(packet[2])
    data['cpm_error']   = float(packet[3])
    data['error_flag']  = int(packet[4])
    if verbose:
        for k, v in data.items():
            print('    {:20}: {}'.format(k, v))
    return data


def main(verbose=False, **kwargs):
    inj = Injector(**kwargs)
    try:
        inj.main()
    except KeyboardInterrupt:
        print('KeyboardInterrupt shutdown!')
    except SystemExit:
        print('SystemExit shutdown!')
    except Exception as e:
        print(e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-t', '--testing', action='store_true',
        help='Run in test mode (do not connect to UDP socket)')
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='\n\t Verbosity level 1')
    parser.add_argument(
        '--ip', type=str, default=None,
        help='\n\t Force a custom listening IP address for the server.')
    main(**vars(parser.parse_args()))
