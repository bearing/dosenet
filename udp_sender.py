#!/home/pi/miniconda/bin/python
import RPi.GPIO as GPIO;
import numpy as np;
import socket;
import datetime;
import time;
from time import sleep;
from dosimeter import dosimeter
import crypt.cust_crypt as ccrypt;

key_file_lst=['id_rsa_dosenet.pub'];
pe=ccrypt.public_d_encrypt(key_file_lst=key_file_lst);

STATION_ID = 1
UDP_IP = "192.168.1.101"
UDP_PORT = 5005

#print "UDP target IP:", UDP_IP
#print "UDP target port:", UDP_PORT
#print "message:", MESSAGE

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP

det = dosimeter();  #Sets up dosimeter

while True:
    cpm1 = det.get_cpm();
    #mcpm = det.get_mcpm();  #Not sure if we should include this

    cpm = cpm1[0] #returns just the cpm, and not the error
    cpm_err = cpm1[1] #returns error in cpm
    
    ErrorCode = 0; #Default, no error codes
    if len(det.counts) > 0:
	if (datetime.datetime.now()-det.counts[-1]).total_seconds() >= 300: #Sets how long of a period of zero counts until it's considered an error
        	ErrorCode = 12
            
    if cpm >= 1000: #Sets maximum threshold value over which count rate is considered an error
        ErrorCode = 66;
 
    tm = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    package = str(STATION_ID) + "," + tm + "," + str(cpm) + "," + str(cpm_err) + "," + str(ErrorCode)   #2 corresponds to etcheverry
    #print package
    package =pe.encrypt_message(package)[0];
    #print package
    sock.sendto(package, (UDP_IP, UDP_PORT))
    print "Package sent"
    time.sleep(5)



