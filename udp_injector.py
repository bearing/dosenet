import sys
import os
import_list=['crypt','mysql','udp']
for el in import_list:
    sys.path.append( os.path.abspath(os.path.join(os.getcwd(),el)) )
from crypt import cust_crypt as ccrypt
from mysql import mysql_tools as mysql
from udp import udp_tools as udpt

privateKey = ['/home/dosenet/.ssh/id_rsa_dosenet']
de = ccrypt.public_d_encrypt(key_file_lst = privateKey)

#connect
db = mysql.SQLObject()

GRIM = "192.168.1.101" # GRIM (DB server) local IP
port = 5005 # Arbitrary, non-conflicting port
sock = udpt.custSocket(ip=GRIM,port=port,decrypt=de)

while True:
    try:
        data = sock.listen()
        print "Received: ", data
        db.inject(data)
    except (KeyboardInterrupt, SystemExit):
        print "Exiting cleaning"
        del db
        sys.exit(0)
    except:
        print "Cannot decrypt data..."
