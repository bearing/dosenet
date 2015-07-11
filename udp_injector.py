#!/usr/bin/python
# 
# Ryan Pavlovsky (until Mon Jun 15 2015)
# Navrit Bal (after Jun 15 2015)
# DoseNet
# Applied Nuclear Physics Division
# Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
# Adapted from udp_injector.py (Ryan Pavlovsky)
# Last updated: Thu 25/06/15
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

parser = argparse.ArgumentParser()
parser.add_argument('--ip',nargs=1,required=False,type=str,
    help='\n\t Listening on localhost:5005')
args = parser.parse_args()

if args.ip:
    pass
else:
    from mysql import mysql_tools as mySQLTool

# Initialise decryption & database objects
privateKey = ['/home/dosenet/.ssh/id_rsa_dosenet']
de = ccrypt.public_d_encrypt(key_file_lst=privateKey) # Uses 1 private key ()
db = mySQLTool.SQLObject()
# Set up network information >> points to GRIM's internal static IP address at port 5005
IP = '192.168.1.101' #GRIM 'Database' IP
port = 5005
if args.ip:
    IP = '127.0.0.1' # Send to localhost for testing

sock = udpTool.custSocket(ip=IP,port=port,decrypt=de)

# Runs until keyboard interrupt or system exit 
while True:
    try:
        data = sock.listen()
        print ('Received message:', data)
        if args.ip:
            print 'Message received on IP: ', IP
        else:
            db.inject(data)
    except (KeyboardInterrupt, SystemExit):
        print ('Exit cleaning')
        del db # Manual garbage collection
        sys.exit(0)
    except (Exception) as e:
        raise e
        print ('Exception: Cannot decrypt data...')
