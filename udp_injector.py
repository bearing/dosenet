#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Run on DoseNet Server!

Authors:
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
# Extensible way for adding future imports
import_list = ['crypt', 'mysql', 'udp']
for el in import_list:
    sys.path.append(os.path.abspath(os.path.join(os.getcwd(), el)))
from crypt import cust_crypt as ccrypt
from udp.udp_tools import custSocket
from mysql.mysql_tools import SQLObject
import socket
import datetime
from collections import OrderedDict
# import email_message


PRIVATE_KEY = os.path.expanduser('~/.ssh/id_rsa_lbl')
PORT = 5005

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

    def __init__(self, verbose=False, private_key=PRIVATE_KEY, port=5005,
                 ip=None, testing=False, **kwargs):
        """
        Initialise decryption & database objects.

        Attributes:
            privateKey (List[str]) - Fully qualified static path of private key
                (between GRIM and the Raspberry Pis)
                Default: ['/home/dosenet/.ssh/id_rsa_dosenet']
            port (int) - Which port to listen for any traffic on.
                Depends on what Ryan chooses to open for us on the 1110C
                subnet. Default: 5005
            IP (String) - Which address is the database (GRIM) at -
                dynamically determined.
                Default: '192.168.1.105'
                Note: this address is not necessarily static
            socket (custSocket) - Refer to the udp_tools.py in the udp folder.
                Sets up UDP only socket to listen on given IP, port and a
                decryption object.
        """
        self.verbose = verbose
        self.testing = testing
        self.private_key = private_key
        self.port = port
        self.test_packet = None
        # Connect to database
        self.db = SQLObject()
        print('\tDataBase:', self.db)
        print('\tDataBase Cursor:', self.db.cursor)
        # Decryptor
        print('\tPrivate Key:', self.private_key)
        de = ccrypt.public_d_encrypt(key_file_lst=[self.private_key])
        print('\tDecryptor:', de)
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
            self.socket = custSocket(ip=self.ip, port=self.port, decrypt=de)
        else:
            self.socket = None
            print('\tTesting Mode! Not setting up UDP socket')

    @staticmethod
    def get_external_ip():
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip_addr = s.getsockname()[0]
        s.close()
        return ip_addr

    @staticmethod
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

    def main(self):
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
        if not self.testing:
            print('\n\t\t\t ~~~~ Listening ~~~~')
        while True:
            if self.testing:
                time.sleep(0.5)
                packet = self.make_test_packet()
            else:
                try:
                    packet = self.socket.listen()
                except Exception as e:
                    print(e)
                    print('Failed getting data from listening to the socket.')
                    continue
                if self.verbose:
                    self.print_status('Message received on {}:{}'.format(
                        self.ip, self.port))
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

    def print_status(self, s):
        print('[{}] {}'.format(str(datetime.datetime.now()), s))


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
