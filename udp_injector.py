#!/usr/bin/python
# -*- coding: utf-8 -*-
# 
# Ryan Pavlovsky (until Mon Jun 15 2015)
# Navrit Bal (after Jun 15 2015)
# DoseNet
# Applied Nuclear Physics Division
# Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
# Adapted from udp_injector.py (Ryan Pavlovsky)
# Last updated: Fri 10/07/15
#################
## Run on GRIM ##
#################

import sys
import os
import argparse
# Extensible way for adding future imports
import_list = ['crypt','mysql','udp']
for el in import_list:
    sys.path.append( os.path.abspath(os.path.join(os.getcwd(),el)) )
from crypt import cust_crypt as ccrypt
from udp import udp_tools as udpTool
# from mysql import mysql_tools as mySQLTool ##### NOTE: Done later depending on the argument

class Injector:
    def parseArguments(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--test',nargs=1,required=False,type=str,
            help='\n\t Listening on localhost:5005')
        self.args = parser.parse_args()
        if self.args.test:
            pass
        else:
            from mysql import mysql_tools as mySQLTool
            self.db = mySQLTool.SQLObject()

    def initialise(self):
        # Initialise decryption & database objects
        self.privateKey = ['/home/dosenet/.ssh/id_rsa_dosenet']
        de = ccrypt.public_d_encrypt(key_file_lst = self.privateKey) # Uses 1 private key ()
        # Set up network information >> points to GRIM's internal static IP address at port 5005
        self.port = 5005
        if self.args.test:
            self.IP = '127.0.0.1' # Listen on localhost for testing
        else:
            self.IP = '192.168.1.101' #GRIM 'Database' IP - default behaviour
        self.socket = udpTool.custSocket(ip = self.IP, port = self.port, decrypt = de)

    def main(self): # Runs until keyboard interrupt or system exit 
        print '\t\t\t ~~~~ Listening ~~~~'
        while True:
            try:
                data = self.socket.listen()
                print ('Received message:', data)
                if self.args.test:
                    print 'Message received on IP:port @ ', self.IP ,':', self.port
                else:
                    # Verifying the packets happens in here
                    self.db.inject(data)
            except (KeyboardInterrupt, SystemExit):
                print ('Exit cleaning')
                del self.db # Manual garbage collection
                sys.exit(0)
            except (Exception) as e:
                print str(e)
                print ('Exception: Cannot decrypt data...')

if __name__=="__main__":
    inj = Injector()
    inj.parseArguments()
    inj.initialise()
    inj.main()
