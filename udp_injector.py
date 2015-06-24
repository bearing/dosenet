#!/usr/bin/python
# 
# Ryan Pavlovsky (until Mon Jun 15 2015)
# Navrit Bal (after Jun 15 2015)
# DoseNet
# Applied Nuclear Physics Division
# Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
# Adapted from udp_injector.py (Ryan Pavlovsky)
# Last updated: Tue 16/06/15
#################
## Run on GRIM ##
#################

import sys
import os
# Extensible way for adding future imports
import_list = ['crypt','mysql','udp']
for el in import_list:
    sys.path.append( os.path.abspath(os.path.join(os.getcwd(),el)) )
from crypt import cust_crypt as ccrypt
from mysql import mysql_tools as mySQLTool
from udp import udp_tools as udpTool

# Initialise decryption & database objects
privateKey = ['/home/dosenet/.ssh/id_rsa_dosenet']
de = ccrypt.public_d_encrypt(key_file_lst=privateKey) # Uses 1 private key ()
db = mySQLTool.SQLObject()
# Set up network information >> points to GRIM's internal static IP address at port 5005
GRIM = "192.168.1.101"
port = 5005
sock = udpTool.custSocket(ip=GRIM,port=port,decrypt=de)

# Runs until keyboard interrupt or system exit 
while True:
    try:
        data = sock.listen()
        print "Received message:", data
        db.inject(data)
    except (KeyboardInterrupt, SystemExit), e:
        print "Exit cleaning"
        raise e
        del db # Manual garbage collection
        sys.exit(0)
    except:
        print "Exception: Cannot decrypt data..."
