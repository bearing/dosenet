#!/home/pi/miniconda/bin/python
import RPi.GPIO as GPIO
import numpy as np
import socket
import datetime
import time
from time import sleep
from dosimeter import dosimeter
import crypt.cust_crypt as ccrypt
##########################
## Run on Raspberry Pis ##
##########################

publicKey = ['id_rsa_dosenet.pub']
pe = ccrypt.public_d_encrypt(key_file_lst = publicKey)

##################################
# LOAD FROM CONFIG FILE          #
stationID = 1                    #
msgHash = ''                     #
##################################
IP = 'grim.nuc.berkeley.edu'
port = 5005
#print "UDP target IP @ port :", IP + ':' + port

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # uses UDP protocol
det = dosimeter()  # Initialise dosimeter object from dosimeter.py

def getDatetime():
    return datetime.datetime.now()

while True:
    try: 
        cpm, cpmError = det.getCPMWithError()
        errorCode = 0 # Default 'working' state - error code 0
        now = getDatetime()
        #if len(det.counts) > 0: # Do not understand the purpose of this line
        if (now - det.counts[-1]).total_seconds() >= 300: #Sets how long of a period of zero counts until it's considered an error
            errorCode = 12
        # THIS IS REALLY POINTLESS ?
        # MAYBE A MAXIMUM RATE OF INCREASE COULD BE USEFUL?
        #if cpm >= 1000: #Sets maximum threshold value over which count rate is considered an error
        #    errorCode = 66
        #time = getDatetime().strftime("%Y-%m-%d %H:%M:%S")
        c = ','
        package = str(msgHash) +c+ str(stationID) +c+ str(cpm) +c+ str(cpmError) +c+ str(errorCode)
        print packet
        packet = pe.encrypt_message(package)[0]
        print packet
        sock.sendto(packet, (IP, port))
        print "Packet sent @ " + now +' - '+ IP +':'+ port
        time.sleep(120)
    except (KeyboardInterrupt, SystemExit):
        print '.... User interrupt ....\n Byyeeeeeeee'
        sys.exit(0)
    except Exception, e:
        raise e