#!/home/pi/miniconda/bin/python
##########################
## Run on Raspberry Pis ##
##########################
import numpy as np
import socket
import datetime
import time
from time import sleep
import crypt.cust_crypt as ccrypt
import os
import csv
import sys
import argparse

def parseArguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', '-t',action='store_true',
        help='\n\t Testing CSV file handling, you should probably use --filename to specify a \
            non-default CSV file. \n Note: CSV - Comma Separated Variable text file')
    #
    parser.add_argument('--filename','-f',nargs=1,type=str,
        help='\n\t Must link to a CSV file with  \n \
            Default is \"config-files/test-onerow.csv\" - no \"')
    #
    parser.add_argument('--led','-l',nargs=1,required=True,type=int, 
        help='\n\t The BCM pin number of the + end of the communications LED\n \
            Make sure a resistor is attached otherwise I expect your LED \
            will blow up soon... \n')
    #
    args = parser.parse_args()
    filePath = args.filename[0]
    LED_pin = args.led[0]

def getDatetime():
    return datetime.datetime.now()

def getContents(filePath='config-files/test-onerow.csv'):
    content = [] #list()
    with open(filePath, 'r') as csvfile: 
        #sniff to find the format #fileDialect = csv.Sniffer().sniff(csvfile.read(1024))
        csvfile.seek(0)
        #read the CSV file into a dictionary
        dictReader = csv.DictReader(csvfile)
        for row in dictReader:
            #do your processing here
            content.append(row)
            if __name__ == '__main__':
                print '\t',type(row),row
    return content # List of dicts

def initialise():
    if args.test:
        print '~ Testing CSV handling\n\n'
        #print a list of available dialects
        print 'CSV Dialects:\t',csv.list_dialects()
        print '- '*64
        print 'Test file:\t',filePath
        file_contents = getContents(filePath)
        print '- '*64
        print '\t',type(file_contents),file_contents
        print '- '*64
        print '\n1st line:\t\t',file_contents[0]
        print 'stationID element:\t',file_contents[0]['stationID']
        print 'message_hash element:\t',file_contents[0]['message_hash']
        print 'lat element:\t\t',file_contents[0]['lat']
        print 'long element:\t\t',file_contents[0]['long']
        print 'cpmtorem element:\t',file_contents[0]['cpmtorem']
        print 'cpmtousv element:\t',file_contents[0]['cpmtousv']
    else:
        print '~ Normal run, loading Raspberry Pi specific modules'
        try:
            import RPi.GPIO as GPIO
            from dosimeter import dosimeter
        except Exception, e:
            print 'Were you looking for the test run? Use the -t or --test flag'
            print '\n\tIs this running on a Raspberry Pi?'
            print '\tIf so, make sure the \'RPi\' package is installed with conda and or pip\n'
            print '------- Importing RPi.GPIO failed -------\n'
            sys.exit(0)

def initVariables():
    ##################################
    # LOAD FROM CONFIG FILE          
    stationID = file_contents[0]['stationID']
    msgHash =  file_contents[0]['message_hash']
    ##################################
    publicKey = ['id_rsa_dosenet.pub']
    pe = ccrypt.public_d_encrypt(key_file_lst = publicKey)
    IP = 'grim.nuc.berkeley.edu'
    port = 5005
    #print'UDP target IP @ port :', IP + ':' + port
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # uses UDP protocol

def main():
    if args.test:
        print 'Testing complete, now go run it on a Raspberry Pi!'
    else:
        det = dosimeter()  # Initialise dosimeter object from dosimeter.py
        while True: # Run until error or KeyboardInterrupt (Ctrl + C)
            try:
                if det.ping(hostname = 'berkeley.edu'):
                    det.activatePin(LED_pin) # LIGHT UP
                else:
                    det.blink(LED_pin,number_of_flashes = 10) # FLASH
                cpm, cpmError = det.getCPM()
                if len(det.counts) > 0: # Only run the next segment after the warm-up phase
                    # GET errorCode from det Object
                    errorCode = 0 # Default 'working' state - error code 0
                    now = getDatetime()
                    #if (now - det.counts[-1]).total_seconds() >= 300: #Sets how long of a period of zero counts until it's considered an error
                    #    errorCode = 12
                    #time = getDatetime().strftime("%Y-%m-%d %H:%M:%S")
                    c = ','
                    package = str(msgHash) +c+ str(stationID) +c+ str(cpm) +c+ str(cpmError) +c+ str(errorCode)
                    print packet
                    packet = pe.encrypt_message(package)[0]
                    print packet
                    sock.sendto(packet, (IP, port))
                    print 'Packet sent @ ' + now +' - '+ IP +':'+ port
                    time.sleep(120)
            except (KeyboardInterrupt, SystemExit):
                print '.... User interrupt ....\n Byyeeeeeeee'
                GPIO.cleanup()
                sys.exit(0)
            except Exception as e:
                GPIO.cleanup()
                raise e
                sys.exit(1)

if __name__ == "__main__":
    parseArguments()
    initialise()
    initVariables()
    main()