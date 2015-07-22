#!/usr/bin/python
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

# class Parser:
    # Do this later

class Injector:
    """ Inserts decrypted and validated data from the Raspberry Pis into a MySQL database.

    Args:
        --test (Optional): Listens on localhost at port 5005 (localhost:5005).
            $ udp_injector.py --test

    Attributes:
        db (SQLObject): Custom MySQL database object for injecting into.
    """
    def parseArguments(self):
        """ Deeply integrated parsing - to be decoupled from the main class later.
        """
        parser = argparse.ArgumentParser()
        parser.add_argument('--test',nargs=0,required=False,
            help='\n\t Listening on localhost:5005')
        self.args = parser.parse_args()
        if self.args.test:
            pass # Cannot import these modules on Macs - test other features.
        else:
            from mysql import mysql_tools as mySQLTool
            self.db = mySQLTool.SQLObject()

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
            IP (String) - Which address is the database (GRIM) at.
                Default: '192.168.1.101'
                Testing: '127.0.0.1'
            socket (custSocket) - Refer to the udp_tools.py in the udp folder.
                Sets up UDP only socket to listen on given IP, port and a decryption object.
        """
        self.privateKey = ['/home/dosenet/.ssh/id_rsa_dosenet']
        de = ccrypt.public_d_encrypt(key_file_lst = self.privateKey) # Uses 1 private key
        self.port = 5005
        if self.args.test:
            self.IP = '127.0.0.1' # Listen on localhost for testing
        else:
            self.IP = '192.168.1.101' #GRIM 'Database' IP - default behaviour
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
                print 'Received message:', data
                if self.args.test:
                    print 'Message received on IP:port @ ', self.IP ,':', self.port
                else:
                    self.db.inject(data) # Verifying the packets happens in here
            except (KeyboardInterrupt, SystemExit):
                print ('Exit cleaning')
                del self.db # Manual garbage collection
                sys.exit(0)
            except (Exception) as e:
                print str(e)
                print ('Exception: Cannot decrypt data...')

if __name__=="__main__":
    inj = Injector()
    inj.parseArguments() # Must parse arguments before actually starting everything else
    inj.initialise()
    inj.main()
