import sys;
import os;
import_list=['crypt','mysql','udp'];
for el in import_list:
    sys.path.append( os.path.abspath(os.path.join(os.getcwd(),el)) );
from crypt import cust_crypt as ccrypt;
from mysql import mysql_tools as msqlt
from udp import udp_tools as udpt

key_file_lst=['/home/dosenet/.ssh/id_rsa_dosenet'];
de=ccrypt.public_d_encrypt(key_file_lst=key_file_lst);

#connect
db=msqlt.SQLObject();
print db;

UDP_IP = "192.168.1.101"
UDP_PORT = 5005
sock=udpt.custSocket(ip=UDP_IP,port=UDP_PORT,decrypt=de);

while True:
    data = sock.listen();
    print "received message:", data
    db.inject(data);

#close
del db;
