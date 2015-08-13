#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Navrit Bal (after Jun 15 2015)
# Ryan Pavlovsky (until Mon Jun 15 2015)
# DoseNet
# Applied Nuclear Physics Division
# Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
# Adapted from udp_injector.py (Ryan Pavlovsky)
# Last updated: Tue 21/07/15
#################
## Run on GRIM ##
#################

import sys
import os
import argparse
import_list = ['crypt','mysql','udp'] # Extensible way for adding future imports
for el in import_list:
    sys.path.append( os.path.abspath(os.path.join(os.getcwd(),el)) )
from crypt import cust_crypt as ccrypt
from udp import udp_tools as udpTool
from mysql import mysql_tools as mySQLTool
import socket

# class Parser:
    # Do this later

class Injector:
    """ Inserts decrypted and validated data from the Raspberry Pis into a MySQL database.

    Args:
        -v (Optional): Is more verbose.

    Attributes:
        db (SQLObject): Custom MySQL database object for injecting into.
        cursor (db.cursor): Used for accessing database returns.
    """
    def parseArguments(self):
        """ Deeply integrated parsing - to be decoupled from the main class later.
        """
        parser = argparse.ArgumentParser()
        parser.add_argument('-v', action='store_true', required = False,
            help = '\n\t Will print: Message received on IP:port... ')
        parser.add_argument('--ip', nargs='?', required=False, type=str,
            help = '\n\t Force a custom listening IP address for the server. \
                    \n Default value: \'192.168.1.105\'')
        self.args = parser.parse_args()
        self.db = mySQLTool.SQLObject()
        print self.db

    def initialise(self):
        """ Effectively __init__ - makes all class attributes (encryption and networking objects).
            Initialise decryption & database objects.

        Attributes:
            privateKey (List[str]) - Fully qualified static path of private key
                (between GRIM and the Raspberry Pis)
                Default: ['/home/dosenet/.ssh/id_rsa_dosenet']
            port (int) - Which port to listen for any traffic on. Depends on what Ryan
                chooses to open for us on the 1110C subnet.
                Default: 5005
            IP (String) - Which address is the database (GRIM) at - dynamically determined.
                Default: '192.168.1.105'
                Note: this address is not necessarily static
            socket (custSocket) - Refer to the udp_tools.py in the udp folder.
                Sets up UDP only socket to listen on given IP, port and a decryption object.
        """
        privateKey = ['/home/dosenet/.ssh/id_rsa.pub']
        de = ccrypt.public_d_encrypt(key_file_lst = privateKey) # Uses 1 private key
        self.port = 5005
        # Gets actual IP address
        self.IP = ([(s.connect(('8.8.8.8', 80)),
                s.getsockname()[0],
                s.close()) for s in [socket.socket(socket.AF_INET,
                                                    socket.SOCK_DGRAM)]][0][1])
        print self.IP # '192.168.1.105' - current GRIM 'Database' IP - default behaviour
        print '~~ GRIM IP should be 192.168.1.105... ~~'
        if self.args.ip:
            self.IP = self.args.ip[0]
            print '~~~~ Using forced IP: ', self.IP, '~~~~'
        print self.IP, self.port, de
        self.socket = udpTool.custSocket(ip = self.IP, port = self.port, decrypt = de)

    def main(self):
        """ Gather, decrpyt, validate and then injects data until keyboard interrupt or system exit.

        Attributes:
            data (socket): Gathered, decrypted, validated in custom socket object.

        Raises:
            KeyboardInterrupt: User interrupt to exit the infinite loop.
            SystemExit: System interrupt to exit the infinite loop.
        """
        print '\n\t\t\t ~~~~ Listening ~~~~'
        while True:
            try:
                data = self.socket.listen()
            except (KeyboardInterrupt, SystemExit):
                print ('\nExit cleaning')
                sys.exit(0)
            except (Exception) as e:
                print str(e)
                print ('Exception: failed getting data from lisetening to the socket.')
            print 'Received message:', data
            if self.args.v:
                print 'Message received on IP:port @ ', self.IP ,':', self.port
            try:
                self.db.inject(data) # Verifying the packets happens in here
            except (KeyboardInterrupt, SystemExit):
                print ('\nExit cleaning')
                sys.exit(0)
            except (Exception) as e:
                print str(e)
                print ('Exception: Cannot decrypt data...')

if __name__=="__main__":
    inj = Injector()
    inj.parseArguments() # Must parse arguments before actually starting everything else
    inj.initialise()
    inj.main()
