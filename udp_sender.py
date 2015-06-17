#!/home/pi/miniconda/bin/python
import RPi.GPIO as GPIO
import numpy as np
import socket
import datetime
import time
from time import sleep
from dosimeter import dosimeter
import crypt.cust_crypt as ccrypt

publicKey=['id_rsa_dosenet.pub']
pe = ccrypt.public_d_encrypt(key_file_lst = publicKey)

stationID = 1
GRIM = 'grim.nuc.berkeley.edu'
port = 5005

#print "UDP target IP:", GRIM
#print "UDP target port:", port

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # uses UDP protocol
det = dosimeter();  # Initialise dosimeter object from dosimeter.py

def getDatetime():
    return datetime.datetime.now()

while True:
    cpm, cpmError = det.getCPMWithError()
    errorCode = 0 # Default 'working' state - error code 0
    #if len(det.counts) > 0: # Do not understand the purpose of this line
	if (getDatetime() - det.counts[-1]).total_seconds() >= 300: #Sets how long of a period of zero counts until it's considered an error
        	errorCode = 12
    # ¿ THIS IS REALLY POINTLESS ?
    # MAYBE A MAXIMUM RATE OF INCREASE COULD BE USEFUL?
    #if cpm >= 1000: #Sets maximum threshold value over which count rate is considered an error
    #    errorCode = 66
    time = getDatetime().strftime("%Y-%m-%d %H:%M:%S")
    c = ','
    package = str(stationID) +c+ time +c+ str(cpm) +c+ str(cpmError) +c+ str(errorCode)
    #print packet
    packet = pe.encrypt_message(package)[0]
    #print packet
    sock.sendto(packet, (GRIM, port))
    print "Packet sent"
    time.sleep(120)