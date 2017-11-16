#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Run on DoseNet Server!

Authors:
    Brian Plimley (after 2016-05-01)
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
    2016-11-23
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
import ast
import multiprocessing
import Crypto.Random
from Crypto.Cipher import AES

# Extensible way for adding future imports
import_list = ['crypt', 'mysql', 'udp']
for el in import_list:
    sys.path.append(os.path.abspath(os.path.join(os.getcwd(), el)))
from crypt import cust_crypt as ccrypt
from mysql.mysql_tools import SQLObject

PRIVATE_KEY = os.path.expanduser('~/.ssh/id_rsa_lbl')
PUBLIC_KEY = os.path.expanduser('~/.ssh/id_rsa_lbl.pub')
SYMMETRIC_KEY = os.path.expanduser('~/secret.aes')

UDP_PORT = 5005
TEST_UDP_PORT = 5006    # for -s mode

TCP_PORT = 5100
TEST_TCP_PORT = 5101    # for -s mode

HASH_LENGTH = 32
PREPEND_LENGTH = 5      # for AES encryption from D3S

ANSI_RESET = '\033[0m'    # reset / default
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
                 test_ports=False,
                 private_key=PRIVATE_KEY,
                 symmetric_key=SYMMETRIC_KEY,
                 udp_port=None,
                 tcp_port=None,
                 test_device = None,
                 **kwargs):
        """
        Initialise decryption & database objects.

        Attributes:
            verbose (bool) - add verbosity. Default: False
            test_inject (bool) - do not listen on sockets, but test
                injection behavior with artificial packets. Default: False
            test_serve (bool) - listen on sockets and process packets,
                but do not inject anything to database. implies test_ports
                Default: False
            test_ports (bool) - use the testing UDP and TCP ports, rather than
                the real ones. Data WILL be injected to database unless
                test_serve is also True. Default: False
            private_key (str) - Fully qualified static path of private key
                Default: '~/.ssh/id_rsa_lbl'
            symmetric_key (str) - Fully qualified static path of AES key
                Default: '~/secret.aes'
            udp_port (int) - Which port to listen for UDP data on.
                Default: 5005 (or 5006 for testing)
            tcp_port (int) - which port to listen for TCP data on.
                Default: 5100 (or 5101 for testing)
            ip (String) - Which address to listen on.
                Default: dynamically determined
        """

        self.test_device = test_device
        self.verbose = verbose
        self.test_inject = test_inject
        self.test_serve = test_serve
        if self.test_serve:
            self.test_ports = True
        else:
            self.test_ports = test_ports
        self.private_key = private_key
        self.symmetric_key = symmetric_key

        if udp_port is None:
            if self.test_ports:
                self.udp_port = TEST_UDP_PORT
            else:
                self.udp_port = UDP_PORT
        else:
            self.udp_port = udp_port
        if tcp_port is None:
            if self.test_ports:
                self.tcp_port = TEST_TCP_PORT
            else:
                self.tcp_port = TCP_PORT
        else:
            self.tcp_port = tcp_port

        self.test_packet = None

        # Connect to database
        self.db = SQLObject()

        # Decrypter
        print('\tPrivate Key:', self.private_key)
        de = ccrypt.public_d_encrypt(key_file_lst=[self.private_key])
        self.decrypter = de

        # AES decryption
        print('\tSymmetric Key:', self.symmetric_key)
        with open(self.symmetric_key, 'r') as keyfile:
            key = keyfile.read()
            assert len(key) == 32
            self.aes = AES.new(key, mode=AES.MODE_ECB)

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
            # test injection
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
            test_cpm = 1.1
            test_cpm_error = 0.1
            test_error_flag = 0
            raw_packet = '{},{},{},{},{}'.format(
                test_hash, test_id, test_cpm, test_cpm_error, test_error_flag)
            # encrypter
            en = ccrypt.public_d_encrypt(key_file_lst=[PUBLIC_KEY])
            # encrypted packet
            self.test_packet = en.encrypt_message(raw_packet)[0]
        return self.test_packet

    def make_test_packet_AQ(self):
        """
        Put together a test message for AQ.
        """
        if self.test_packet is None:
            inj_stat = self.db.getInjectorStation()
            test_hash = inj_stat['IDLatLongHash']
            test_id = inj_stat.name
            test_time = time.time()
            test_data = [169.01, 229.01, 331.01, 428324.01, 11223.01, 4142.01, 522.01, 84.01, 24.01]
            new_test_data = str(test_data).replace(',', ';')
            test_error_flag = 0
            raw_packet = '{},{},{},{},{}'.format(
                test_hash, test_id, test_time, new_test_data, test_error_flag)
            en = ccrypt.public_d_encrypt(key_file_lst=[PUBLIC_KEY])
            self.test_packet = en.encrypt_message(raw_packet)[0]
        return self.test_packet

    def make_test_packet_CO2(self):
        """
        Put together a test message for CO2.
        """
        if self.test_packet is None:
            inj_stat = self.db.getInjectorStation()
            test_hash = inj_stat['IDLatLongHash']
            test_id = inj_stat.name
            test_time = time.time()
            test_data = [500.01, 3.01]
            new_test_data = str(test_data).replace(',', ';')
            test_error_flag = 0
            raw_packet = '{},{},{},{},{}'.format(
                test_hash, test_id, test_time, new_test_data, test_error_flag)
            en = ccrypt.public_d_encrypt(key_file_lst=[PUBLIC_KEY])
            self.test_packet = en.encrypt_message(raw_packet)[0]
        return self.test_packet

    def make_test_packet_Weather(self):
        """
        Put together a test message for Weather.
        """
        if self.test_packet is None:
            inj_stat = self.db.getInjectorStation()
            test_hash = inj_stat['IDLatLongHash']
            test_id = inj_stat.name
            test_time = time.time()
            test_data = [21.52, 999.81, 36.01]
            new_test_data = str(test_data).replace(',', ';')
            test_error_flag = 0
            raw_packet = '{},{},{},{},{}'.format(
                test_hash, test_id, test_time, new_test_data, test_error_flag)
            en = ccrypt.public_d_encrypt(key_file_lst=[PUBLIC_KEY])
            self.test_packet = en.encrypt_message(raw_packet)[0]
        return self.test_packet

    def test(self):
        """
        Test packet handling with make_test_packet() and handle().

        Blocks execution.
        """

        while True:
            if self.test_device == "AQ":
                test_packet = self.make_test_packet_AQ()
            if self.test_device == 'CO2':
                test_packet = self.make_test_packet_CO2()
            if self.test_device == "Pocket":
                test_packet = self.make_test_packet()
            if self.test_device == 'Weather':
                test_packet = self.make_test_packet_Weather()
            else:
                test_packet = self.make_test_packet()
            self.handle(test_packet, mode='test')
            time.sleep(1.1)

    def handle(self, encrypted_packet, is_aes=False,
               client_address=None, request=None, mode=None):
        """
        Handle one request from either UDP or TCP.

        Gets called in UdpHandler.handle() or TcpHandler.handle().

        Error handling can get messy. Here is the structure of function calls:

        handle()
            handle_decryption()
                decrypt_packet()
            get_fields()
            handle_request_type()
                classify_request()
            handle_parsing()
                parse_packet()
                check_countrate() [if applicable]
            handle_injection()
                db.inject() OR db.injectLog()
            handle_return_packet()
                db.getStationReturnInfo()
        """

        packet = self.handle_decryption(
            encrypted_packet, is_aes=is_aes, mode=mode)
        if packet is None:
            return

        field_list = self.get_fields(packet)

        request_type = self.handle_request_type(
            field_list, mode=mode, packet=packet)
        if request_type is None:
            return

        field_dict = self.handle_parsing(field_list, request_type, mode=mode)
        if field_dict is None:
            return

        self.handle_injection(field_dict, request_type,
                              mode=mode, client_address=client_address)

        self.handle_return_packet(field_dict, request)

    def handle_decryption(self, encrypted, is_aes=False, mode=None):
        """
        Decrypt packet and handle errors.
        """

        if is_aes:
            decrypt = self.decrypt_packet_aes
        else:
            decrypt = self.decrypt_packet_rsa

        try:
            packet = decrypt(encrypted)
        except UnencryptedPacket:
            # print to screen. this could be a test message
            print_status(
                'Unencrypted {} packet: {}'.format(
                    mode.upper(), encrypted),
                ansi=ANSI_CYAN)
            return None
        except BadPacket:
            print_status(
                'Bad {} packet (cannot resolve into standard ASCII)'.format(
                    mode.upper()),
                ansi=ANSI_RED)
            return None
        except ValueError:
            # AES: message length not a multiple of block size
            print_status('Bad AES TCP packet (blocksize error)', ansi=ANSI_RED)
            return None

        return packet

    def decrypt_packet_rsa(self, encrypted):
        """
        Decrypt packet using RSA private key.

        May raise UnencryptedPacket or BadPacket.
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

    def decrypt_packet_aes(self, encrypted):
        """
        Decrypt packet using AES symmetric key. For D3S packets.

        May raise BadPacket.
        """

        decrypted = self.aes.decrypt(encrypted)
        ascii_values_decrypted = [ord(c) for c in decrypted]

        if any(v > 127 for v in ascii_values_decrypted):
            raise BadPacket(
                'Bad character values in decrypted AES packet: {}'.format(
                    ascii_values_decrypted))

        return decrypted

    def get_fields(self, packet):
        """
        Split packet into fields. No verification or error checking.
        """
        sep = ','
        return packet.split(sep)

    def handle_request_type(self, field_list, mode=None, packet=None):
        """
        Classify request type and verify data length and hash length.
        """

        try:
            request_type = self.classify_request(field_list)
        except PacketLengthError as e:
            # encrypted test message
            print_status(
                '{} PacketLengthError: {}. Packet={}'.format(
                    mode.upper(), e, packet),
                ansi=ANSI_GR)
            return None
        except UnknownRequestType:
            print_status('{} UnknownRequestType. Packet={}'.format(
                mode.upper(), packet))
            return None
        else:
            return request_type

    def classify_request(self, field_list):
        """
        Classify request as data or log.

        May raise PacketLengthError or UnknownRequestType.
        """

        num_log_fields = 5
        num_data_fields_old = 5
        num_data_fields_new = 6
        num_d3s_fields = 5
        num_AQ_fields = 5
        num_co2_fields = 5
        num_weather_fields = 5

        if (len(field_list) != num_log_fields and
                len(field_list) != num_data_fields_old and
                len(field_list) != num_data_fields_new and
                len(field_list) != num_d3s_fields and
                len(field_list) != num_AQ_fields and
                len(field_list) != num_co2_fields and
                len(field_list) != num_weather_fields):
            raise PacketLengthError(
                'Found {} fields instead of {}, {}, {}, {}, or {}'.format(
                    len(field_list),
                    num_log_fields, num_data_fields_old, num_data_fields_new,
                    num_d3s_fields, num_co2_fields, num_weather_fields))
        elif field_list[2] == 'LOG' and len(field_list) == num_log_fields:
            request_type = 'log'
        elif (len(field_list) == num_AQ_fields and
                field_list[3].startswith('[') and
                len(field_list[3]) >= 54 and
                len(field_list[3]) <= 77):
            request_type = 'AQ'
        elif (len(field_list) == num_d3s_fields and
                field_list[3].startswith('[') and
                len(field_list[3]) > 4096):
            request_type = 'd3s'
        elif (len(field_list) == num_co2_fields and
                field_list[3].startswith('[') and
                len(field_list[3]) >= 12 and
                len(field_list[3]) <= 19):
            request_type = 'co2'
        elif (len(field_list) == num_weather_fields and
                field_list[3].startswith('[') and
                len(field_list[3]) >= 20 and
                len(field_list[3]) <= 25):
            request_type = 'weather'
        elif len(field_list) == num_data_fields_old:
            request_type = 'data_old'
        elif len(field_list) == num_data_fields_new:
            request_type = 'data'
        else:
            # currently, this is impossible
            #   unless num_log_fields is different than num_data_fields_*,
            #   and the log is submitted without 'LOG' identifier in position 2
            raise UnknownRequestType()

        return request_type

    def handle_parsing(self, field_list, request_type, mode=None):
        """
        Parse data or log packet. Handle errors.
        """

        try:
            field_dict = self.parse_packet(field_list, request_type)
        except HashLengthError as e:
            packet = ','.join(field_list)
            print_status(
                '{} HashLengthError: {}. Packet={}'.format(
                    mode.upper(), e, packet),
                ansi=ANSI_MG)
            return None
        except ExcessiveCountrate:
            packet = ','.join(field_list)
            print_status(
                '{} Excessive Countrate! {}'.format(mode.upper(), packet),
                ansi=ANSI_YEL)
            return None
        else:
            return field_dict

    def parse_packet(self, field_list, request_type):
        """
        Extract data from fields into an OrderedDict.

        May raise HashLengthError or ExcessiveCountrate.
        """

        ind_hash = 0
        ind_ID = 1
        if request_type == 'data_old':
            ind_deviceTime = None
            ind_cpm = 2
            ind_cpm_error = 3
            ind_error_flag = 4
        elif request_type == 'data':
            ind_deviceTime = 2
            ind_cpm = 3
            ind_cpm_error = 4
            ind_error_flag = 5
        elif request_type == 'log':
            # position 2 is "LOG" - already checked in classify_request()
            ind_msgCode = 3
            ind_msgText = 4
        elif request_type == 'd3s':
            ind_deviceTime = 2
            ind_spectrum = 3
            ind_error_flag = 4
        elif request_type == 'AQ':
            ind_deviceTime = 2
            ind_average_data = 3
            ind_conc_one = 0
            ind_conc_twopointfive = 1
            ind_conc_ten = 2
            ind_error_flag = 4
        elif request_type == 'co2':
            ind_deviceTime = 2
            ind_average_data = 3
            ind_co2_ppm = 0
            ind_noise = 1
            ind_error_flag = 4
        elif request_type == 'weather':
            ind_deviceTime = 2
            ind_average_data = 3
            ind_temp = 0
            ind_pres = 1
            ind_humid = 2
            ind_error_flag = 4

        field_dict = OrderedDict()

        field_dict['hash'] = field_list[ind_hash]
        if len(field_dict['hash']) != HASH_LENGTH:
            raise HashLengthError('Hash length is not {}: {}'.format(
                HASH_LENGTH, field_dict['hash']))
        # The value of the hash gets checked in mysql_tools.SQLObject.inject()

        field_dict['stationID'] = int(field_list[ind_ID])
        if request_type.startswith('data'):
            if ind_deviceTime:
                field_dict['deviceTime'] = float(field_list[ind_deviceTime])
            field_dict['cpm'] = float(field_list[ind_cpm])
            field_dict['cpm_error'] = float(field_list[ind_cpm_error])
            field_dict['error_flag'] = int(field_list[ind_error_flag])

            self.check_countrate(field_dict)

        elif request_type == 'd3s':
            field_dict['deviceTime'] = float(field_list[ind_deviceTime])
            spectrum_str = str(field_list[ind_spectrum]).replace(';', ',')
            field_dict['spectrum'] = ast.literal_eval(spectrum_str)
            field_dict['error_flag'] = int(field_list[ind_error_flag])

        elif request_type == 'log':
            field_dict['msgCode'] = int(field_list[ind_msgCode])
            field_dict['msgText'] = field_list[ind_msgText]

        elif request_type == 'AQ':
            field_dict['deviceTime'] = float(field_list[ind_deviceTime])
            tmp = ast.literal_eval(str(field_list[ind_average_data]).replace(';', ','))
            field_dict['oneMicron'] = tmp[ind_conc_one]
            field_dict['twoPointFiveMicron'] = tmp[ind_conc_twopointfive]
            field_dict['tenMicron'] = tmp[ind_conc_ten]
            field_dict['error_flag'] = int(field_list[ind_error_flag])

        elif request_type == 'co2':
            field_dict['deviceTime'] = float(field_list[ind_deviceTime])
            tmp = ast.literal_eval(str(field_list[ind_average_data]).replace(';', ','))
            field_dict['co2_ppm'] = tmp[ind_co2_ppm]
            field_dict['noise'] = tmp[ind_noise]
            field_dict['error_flag'] = int(field_list[ind_error_flag])

        elif request_type == 'weather':
            field_dict['deviceTime'] = float(field_list[ind_deviceTime])
            tmp = ast.literal_eval(str(field_list[ind_average_data]).replace(';', ','))
            field_dict['temperature'] = tmp[ind_temp]
            field_dict['pressure'] = tmp[ind_pres]
            field_dict['humidity'] = tmp[ind_humid]
            field_dict['error_flag'] = int(field_list[ind_error_flag])

        return field_dict

    def check_countrate(self, data):
        """
        Check for countrate that is too high.
        """

        cpm_error_threshold = 100

        if data['cpm'] > cpm_error_threshold:
            raise ExcessiveCountrate(
                'Countrate {} CPM is greater than threshold of {} CPM'.format(
                    data['cpm'], cpm_error_threshold))

    def handle_injection(self, data, request_type,
                         mode=None, client_address=None):
        """
        Inject data into SqlObject. Handle errors.
        """

        if self.test_serve:
            print_status('Not injecting {}: {}'.format(
                mode.upper(), format_packet(data, client_address)))
            return
        elif request_type.startswith('data'):
            print_status('Injecting {}: {}'.format(
                mode.upper(), format_packet(data, client_address)))
            inject_method = self.db.inject
        elif request_type == 'd3s':
            print_status('Injecting D3S {}: {}'.format(
                mode.upper(), format_packet(data, client_address)))
            inject_method = self.db.injectD3S
        elif request_type == 'log':
            print_status('Injecting {} to log: {}'.format(
                mode.upper(), format_packet(data, client_address)))
            inject_method = self.db.injectLog
        elif request_type == 'AQ':
            print_status('Injecting {}: {}'.format(
                mode.upper(), format_packet(data, client_address)))
            inject_method = self.db.injectAQ
        elif request_type == 'co2':
            print_status('Injecting {}: {}'.format(
                mode.upper(), format_packet(data, client_address)))
            inject_method = self.db.injectCO2
        elif request_type == 'weather':
            print_status('Injecting {}: {}'.format(
                mode.upper(), format_packet(data, client_address)))
            inject_method = self.db.injectWeather

        try:
            print(data)
            inject_method(data)
        except Exception as e:
            print('Injection error:', e)
            return None

    def handle_return_packet(self, field_dict, request):
        """
        Send data packet back to device, with needsUpdate and gitBranch info.

        Handles errors.
        """

        stationID = field_dict['stationID']
        try:
            git_branch, needs_update = self.db.getStationReturnInfo(stationID)
        except IndexError:
            # stationID not in stations table, yet. shouldn't happen.
            print_status(
                "Station ID {} missing from `stations` table!", ansi=ANSI_CYAN)
            return

        return_packet = "{},{}".format(git_branch, needs_update)

        try:
            request.sendall(return_packet)
        except socket.error as e:
            print_status("Socket error on TCP return packet: {}".format(e))
        except AttributeError:
            if request is None:
                # test injection mode -t
                pass
            else:
                raise
        else:
            # unset needs_update flag
            # *** takes ~50 ms - only do if needed!
            if needs_update != 0:
                self.db.setSingleStationUpdate(
                    field_dict['stationID'], needs_update=0)


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


def format_packet(data, client_address):
    """
    Format the packet data into a pretty string.
    For "good" packets.
    """

    if 'cpm' in data.keys():
        output = '#{}, CPM {:.1f}+-{:.1f}, err {}'.format(
            data['stationID'], data['cpm'], data['cpm_error'],
            data['error_flag'])
        if 'deviceTime' in data:
            output += ' at {}'.format(
                datetime.datetime.fromtimestamp(data['deviceTime']))
    elif 'msgCode' in data.keys():
        output = '#{}, code {}: {}'.format(
            data['stationID'], data['msgCode'], data['msgText'])
    elif 'spectrum' in data.keys():
        output = '#{}, {} total counts, err {}'.format(
            data['stationID'], sum(data['spectrum']), data['error_flag'])
        if 'deviceTime' in data:
            output += ' at {}'.format(
                datetime.datetime.fromtimestamp(data['deviceTime']))
    elif 'oneMicron' in data.keys():
        output = '#{}, 1 Micron: {}, 2.5 Microns: {}, 10 Microns: {}, err {}'.format(
            data['stationID'], data['oneMicron'], data['twoPointFiveMicron'], data['tenMicron'], data['error_flag'])
        if 'deviceTime' in data:
            output += ' at {}'.format(
                datetime.datetime.fromtimestamp(data['deviceTime']))
    elif 'co2_ppm' in data.keys():
        output = '#{}, CO2 Concentration: {}, UV Index: {}, err {}'.format(
            data['stationID'], data['co2_ppm'], data['noise'], data['error_flag'])
        if 'deviceTime' in data:
            output += ' at {}'.format(
                datetime.datetime.fromtimestamp(data['deviceTime']))
    elif 'temperature' in data.keys():
        output = '#{}, Temperature: {}, Pressure: {}, Humidity: {}, err {}'.format(
            data['stationID'], data['temperature'], data['pressure'], data['humidity'],
            data['error_flag'])
        if 'deviceTime' in data:
            output += ' at {}'.format(
                datetime.datetime.fromtimestamp(data['deviceTime']))
    else:
        # ???
        output = ' [packet type unknown to format_packet()]'
    if client_address is not None:
        output += ' [from {}]'.format(client_address[0])

    return output


class DosenetUdpServer(SocketServer.UDPServer):
    """
    Server object to handle UDP requests.

    The Dosenet*Server classes add allow_reuse_address = True
    (which should force binding to a port, even if the past server hasn't
    released it yet).
    """

    allow_reuse_address = True
    # stackoverflow.com/questions/3911009/
    #   python-socketserver-baserequesthandler-
    #   knowing-the-port-and-use-the-port-already

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
        is_aes = False
        firstdata = self.request.recv(PREPEND_LENGTH)
        try:
            msg_len = int(firstdata)
        except ValueError:
            # not AES. get the remainder of the RSA-encrypted message in one go
            remainder = self.request.recv(256)
            data = firstdata + remainder
        else:
            is_aes = True
            bytes_recvd = 0
            buffer_size = 1024
            datalist = []
            while bytes_recvd < msg_len:
                request_size = min(msg_len - bytes_recvd, buffer_size)
                datalist.append(self.request.recv(request_size))
                bytes_recvd += len(datalist[-1])
            data = ''.join(datalist)

        self.server.injector.handle(
            data,
            is_aes=is_aes,
            client_address=self.client_address,
            request=self.request,
            mode='tcp')


class InjectorError(Exception):
    pass


class PacketLengthError(InjectorError):
    pass


class HashLengthError(InjectorError):
    pass


class UnknownRequestType(InjectorError):
    pass


class UnencryptedPacket(InjectorError):
    pass


class BadPacket(InjectorError):
    pass


class ExcessiveCountrate(InjectorError):
    pass


def main(verbose=False, **kwargs):
    inj = Injector(**kwargs)
    if not inj.test_inject:
        try:
            inj.listen()
        except KeyboardInterrupt:
            print('KeyboardInterrupt shutdown!')
        except SystemExit:
            print('SystemExit shutdown!')
    else:
        # test_inject
        inj.test()


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
        '-p', '--test-ports', action='store_true',
        help='Listen on the default testing ports. Fully functional server')
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='\n\t Verbosity level 1')
    parser.add_argument(
        '--ip', type=str, default=None,
        help='\n\t Force a custom listening IP address for the server.')
    parser.add_argument(
        '-d', '--test_device', type=str, default=None,
        help='\n\t Pick a device to emulate: AQ, CO2, Weather or Pocket.')
    main(**vars(parser.parse_args()))
