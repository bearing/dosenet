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
import Crypto.Random

# Extensible way for adding future imports
import_list = ['crypt', 'mysql', 'udp']
for el in import_list:
    sys.path.append(os.path.abspath(os.path.join(os.getcwd(), el)))
from crypt import cust_crypt as ccrypt
from mysql.mysql_tools import SQLObject

PRIVATE_KEY = os.path.expanduser('~/.ssh/id_rsa_lbl')

UDP_PORT = 5006     # testing!
TCP_PORT = 5100

ANSI_RESET = '\033[37m' + '\033[22m'    # white and not bold
ANSI_BOLD = '\033[1m'
ANSI_RED = '\033[31m' + ANSI_BOLD
ANSI_GR = '\033[32m' + ANSI_BOLD
ANSI_YEL = '\033[33m' + ANSI_BOLD
ANSI_CYAN = '\033[36m' + ANSI_BOLD
ANSI_MG = '\033[35m' + ANSI_BOLD


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

    def __init__(self,
                 verbose=False,
                 test_inject=False,
                 test_serve=False,
                 private_key=PRIVATE_KEY,
                 udp_port=UDP_PORT,
                 tcp_port=TCP_PORT,
                 **kwargs):
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
        self.test_inject = test_inject
        self.test_serve = test_serve
        self.private_key = private_key
        self.udp_port = udp_port
        self.tcp_port = tcp_port
        self.test_packet = None

        # Connect to database
        self.db = SQLObject()

        # Decrypter
        print('\tPrivate Key:', self.private_key)
        de = ccrypt.public_d_encrypt(key_file_lst=[self.private_key])
        self.decrypter = de

        # Get ip information
        self.ip = self.get_external_ip()
        print('\tIP:', self.ip)
        print('\tUDP Port:', self.udp_port)
        print('\tTCP Port:', self.tcp_port)

        if self.test_serve:
            print('Server test mode! I can listen for packets, but nothing ' +
                  'will be injected to the database.')
        if not self.test_inject:
            # set up socket servers
            self.udp_server = DosenetUdpServer(
                (self.ip, self.udp_port), UdpHandler, injector=self)
            self.tcp_server = DosenetTcpServer(
                (self.ip, self.tcp_port), TcpHandler, injector=self)
        else:
            self.udp_server = None
            self.tcp_server = None
            print('Injection test mode! I can only inject test packets. ' +
                  'No socket servers.')

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

        print('\n')
        if self.test_inject:
            # no socket servers. instead, run the test mode
            self.test()
            # self.test blocks, but for clarity:
            return None
        else:
            self.udp_process = multiprocessing.Process(
                target=self.udp_server.serve_forever)
            self.tcp_process = multiprocessing.Process(
                target=self.tcp_server.serve_forever)
            self.udp_process.start()
            self.tcp_process.start()

    def make_test_packet(self):
        """
        Put together a test message
        """

        if self.test_packet is None:
            # packet contents
            inj_stat = self.db.getInjectorStation()
            test_hash = inj_stat['IDLatLongHash']
            test_id = inj_stat.name
            test_cpm = 1.
            test_cpm_error = 0.5
            test_error_flag = 0
            raw_packet = '{},{},{},{},{}'.format(
                test_hash, test_id, test_cpm, test_cpm_error, test_error_flag)
            # encrypter
            publickey = '/home/dosenet/id_rsa.pub'
            en = ccrypt.public_d_encrypt(key_file_lst=[publickey])
            # encrypted packet
            self.test_packet = en.encrypt_message(raw_packet)[0]
        return self.test_packet

    def test(self):
        """
        Test packet handling with make_test_packet() and handle().

        Blocks execution.
        """

        while True:
            test_packet = self.make_test_packet()
            self.handle(test_packet, mode='test')
            time.sleep(1.1)

    def handle(self, encrypted_packet,
               client_address=None, request=None, mode=None):
        """
        Handle one request from either UDP or TCP.

        Gets called in UdpHandler.handle() or TcpHandler.handle().
        """

        try:
            packet = self.decrypt_packet(encrypted_packet)
        except UnencryptedPacket:
            # print to screen. this could be a test message
            print_status(
                'Unencrypted {} packet: {}'.format(
                    mode.upper(), encrypted_packet),
                ansi=ANSI_CYAN)
            return None
        except BadPacket:
            print_status(
                'Bad {} packet (cannot resolve into standard ASCII)'.format(
                    mode.upper()),
                ansi=ANSI_RED)
            return None

        try:
            data = self.parse_packet(packet)
        except PacketLengthError as e:
            # encrypted test message
            print_status(
                '{} PacketLengthError: {}. Packet={}'.format(
                    mode.upper(), e, packet),
                ansi=ANSI_GR)
            return None
        except HashLengthError as e:
            print_status(
                '{} HashLengthError: {}. Packet={}'.format(
                    mode.upper(), e, packet),
                ansi=ANSI_MG)
            return None

        try:
            self.check_countrate(data)
        except ExcessiveCountrate:
            print_status(
                '{} Excessive Countrate! {}'.format(mode.upper(), packet),
                ansi=ANSI_YEL)
            return None

        # Still here? now inject into database if appropriate.
        if self.test_serve:
            print_status('Not injecting {}: {}'.format(mode.upper(), packet))
        else:
            print_status('Injecting {}: {}'.format(mode.upper(), packet))
            try:
                self.db.inject(data)
            except Exception as e:
                print('Injection error:', e)
                return None

        return None

    def decrypt_packet(self, encrypted):
        """
        Decrypt packet using private key.

        Also check for the case of an unencrypted packet
        """

        # In standard text, all character values should be <128.
        # Encrypted text, or text decrypted with the wrong key, will have
        #   character values up to 255.
        # Also, the length of encrypted stuff should be 256 characters.
        length_encrypted = len(encrypted)
        ascii_values_encrypted = [ord(c) for c in encrypted]

        decrypted = self.decrypter.decrypt_message((encrypted,))

        length_decrypted = len(decrypted)
        ascii_values_decrypted = [ord(c) for c in decrypted]

        if (length_encrypted != 256 and length_decrypted == 256 and
                all(v < 128 for v in ascii_values_encrypted)):
            raise UnencryptedPacket(
                'Packet lengths suggest that the packet was not encrypted')
        elif (all(v < 128 for v in ascii_values_encrypted) and
                any(v > 127 for v in ascii_values_decrypted)):
            raise UnencryptedPacket(
                'Character codes suggest that the packet was not encrypted')
        elif any(v > 127 for v in ascii_values_decrypted):
            raise BadPacket(
                'Bad character values in decrypted packet (>127): {}'.format(
                    ascii_values_decrypted))

        return decrypted

    def parse_packet(self, packet):
        """
        Split packet into fields
        First stage of data verification: # fields, hash length
        Assign fields into a dict object
        """

        sep = ','
        data_length_old = 5
        data_length_new = 6
        hash_length = 32

        fields = packet.split(sep)

        if len(fields) != data_length_old and len(fields) != data_length_new:
            raise PacketLengthError(
                'Found {} fields instead of {} or {}'.format(
                    len(fields), data_length_old, data_length_new))

        data = OrderedDict()
        data['hash'] = fields[0]
        if len(data['hash']) != hash_length:
            raise HashLengthError('Hash length is not {}: {}'.format(
                hash_length, data['hash']))
        # The value of the hash gets checked in mysql_tools.SQLObject.inject()

        data['stationID'] = int(fields[1])
        if len(fields) == data_length_old:
            data['cpm'] = float(fields[2])
            data['cpm_error'] = float(fields[3])
            data['error_flag'] = int(fields[4])
        if len(fields) == data_length_new:
            data['deviceTime'] = float(fields[2])
            data['cpm'] = float(fields[3])
            data['cpm_error'] = float(fields[4])
            data['error_flag'] = int(fields[5])
        if self.verbose:
            for k, v in data.items():
                print('    {:20}: {}'.format(k, v))

        return data

    def check_countrate(self, data):
        """
        Check for countrate that is too high.
        """

        cpm_error_threshold = 100

        if data['cpm'] > cpm_error_threshold:
            raise ExcessiveCountrate(
                'Countrate {} CPM is greater than threshold of {} CPM'.format(
                    data['cpm'], cpm_error_threshold))


def print_status(status_text, ansi=None):
    """
    Print a status message:
    [datetime] string
    using ANSI color code, if provided.
    """

    if ansi is None:
        ansi = ''
    print_text = (ansi + '[{}] '.format(datetime.datetime.now()) +
                  status_text + ANSI_RESET)
    print(print_text)


class DosenetUdpServer(SocketServer.UDPServer):
    """
    Server object to handle UDP requests.

    The Dosenet*Server classes add allow_reuse_address = True
    (which should force binding to a port, even if the past server hasn't
    released it yet).
    """

    allow_reuse_address = True
    # http://stackoverflow.com/questions/3911009/python-socketserver-baserequesthandler-knowing-the-port-and-use-the-port-already

    # don't use the verify_request method, because this happens before the
    # packet is unpacked in UdpHandler.handle()

    def __init__(self, server_address, req_handler_class, injector=None,
                 **kwargs):
        """
        Save the injector instance, in addition to the
        SocketServer.UDPServer __init__ method
        """

        SocketServer.UDPServer.__init__(
            self, server_address, req_handler_class, **kwargs)
        self.injector = injector

    def serve_forever(self, *args, **kwargs):
        """
        Add a Crypto.Random.atfork() call, to allow decryption from
        this process.
        """

        Crypto.Random.atfork()
        SocketServer.UDPServer.serve_forever(self, *args, **kwargs)


class DosenetTcpServer(SocketServer.TCPServer):
    """
    Server object to handle TCP requests.

    The Dosenet*Server classes add allow_reuse_address = True
    (which should force binding to a port, even if the past server hasn't
    released it yet).
    """

    allow_reuse_address = True
    # don't use the verify_request method, because this happens before the
    # packet is unpacked in TcpHandler.handle()

    def __init__(self, server_address, req_handler_class, injector=None,
                 **kwargs):
        """
        Save the injector instance, in addition to the
        SocketServer.TCPServer __init__ method
        """

        SocketServer.TCPServer.__init__(
            self, server_address, req_handler_class, **kwargs)
        self.injector = injector

    def serve_forever(self, *args, **kwargs):
        """
        Add a Crypto.Random.atfork() call, to allow decryption from
        this process.
        """

        Crypto.Random.atfork()
        SocketServer.TCPServer.serve_forever(self, *args, **kwargs)


class UdpHandler(SocketServer.DatagramRequestHandler):
    """
    Contains the code to handle one request.
    https://docs.python.org/2/library/socketserver.html#request-handler-objects

    Keep this lightweight: It's instantiated for every request the server
    receives. (The handling code is ultimately the same, but let's not create
    a pile of object attributes.)
    """

    def handle(self):
        # UDP request is a tuple of (data, socket) instead of just socket
        data = self.request[0]

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
        data = self.request.recv(1024)

        self.server.injector.handle(
            data, client_address=self.client_address, request=self.request,
            mode='tcp')


class InjectorError(Exception):
    pass


class PacketLengthError(InjectorError):
    pass


class HashLengthError(InjectorError):
    pass


class UnencryptedPacket(InjectorError):
    pass


class BadPacket(InjectorError):
    pass


class ExcessiveCountrate(InjectorError):
    pass


def main(verbose=False, **kwargs):
    inj = Injector(**kwargs)
    try:
        inj.listen()
    except KeyboardInterrupt:
        print('KeyboardInterrupt shutdown!')
    except SystemExit:
        print('SystemExit shutdown!')
    except Exception as e:
        print(e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-t', '--test-inject', action='store_true',
        help='Injection test mode: no sockets, inject test data to database')
    parser.add_argument(
        '-s', '--test-serve', action='store_true',
        help='Server test mode: connect to TCP & UDP sockets and ' +
        'do everything EXCEPT inject to database')
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='\n\t Verbosity level 1')
    parser.add_argument(
        '--ip', type=str, default=None,
        help='\n\t Force a custom listening IP address for the server.')
    main(**vars(parser.parse_args()))
