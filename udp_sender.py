#!/home/pi/miniconda/bin/python
# -*- coding: utf-8 -*-
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

class Sender:
    def parseArguments(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--test', '-t',action='store_true',
            help='\n\t Testing CSV file handling, you should probably use --filename to specify a \
                non-default CSV file. \n Note: CSV - Comma Separated Variable text file')
        #
        parser.add_argument('--filename','-f',nargs='?',type=str,default='config-files/test-onerow.csv',
            help='\n\t Must link to a CSV file with  \n \
                Default is \"config-files/test-onerow.csv\" - no \"')
        #
        parser.add_argument('--led',nargs='?',required=False,type=int, default=20,
            help='\n\t The BCM pin number of the + end of the communications LED\n \
                Make sure a resistor is attached otherwise I expect your LED \
                will blow up soon... \n')                           # nargs='?' means 0-or-1 arguments
        parser.add_argument('--ip',nargs=1,required=False,type=str)
        #
        self.args = parser.parse_args()
        self.file_path = self.args.filename[0]
        self.LED = self.args.led[0] #Default BCM Pin 7

    def getContents(self,file_path):
        content = [] #list()
        with open(file_path, 'r') as csvfile: 
            csvfile.seek(0)
            dictReader = csv.DictReader(csvfile) #read the CSV file into a dictionary
            for row in dictReader:
                content.append(row)
                if __name__ == '__main__':
                    print '\t',type(row),row
        return content # List of dicts

    def initialise(self):
        if self.args.test:
            print '~ Testing CSV handling\n\n'
            #print a list of available dialects
            print 'CSV Dialects:\t',csv.list_dialects()
            print '- '*64
            print 'Test file:\t',self.file_path
            self.file_contents = self.getContents(self.file_path)
            print '- '*64
            print '\t',type(self.file_contents),self.file_contents
            print '- '*64
            print '\n1st line:\t\t',self.file_contents[0]
            print 'stationID element:\t',self.file_contents[0]['stationID']
            print 'message_hash element:\t',self.file_contents[0]['message_hash']
            print 'lat element:\t\t',self.file_contents[0]['lat']
            print 'long element:\t\t',self.file_contents[0]['long']
            print 'cpmtorem element:\t',self.file_contents[0]['cpmtorem']
            print 'cpmtousv element:\t',self.file_contents[0]['cpmtousv']
        else:
            print '~ Normal run, loading Raspberry Pi specific modules'
            try:
                import RPi.GPIO as GPIO
                from dosimeter import Dosimeter
                self.file_contents = self.getContents(self.file_path)
            except Exception, e:
                print 'Were you looking for the test run? Use the -t or --test flag'
                print '\n\tIs this running on a Raspberry Pi?'
                print '\tIf so, make sure the \'RPi\' package is installed with conda and or pip\n'
                print '------- Importing RPi.GPIO failed -------\n'
                raise e
                sys.exit(0)

    def getDatafromCSV(self):
        # LOAD FROM CONFIG FILE          
        self.stationID = self.file_contents[0]['stationID']
        self.msg_hash =  self.file_contents[0]['message_hash']
        ##################################

    def initVariables(self):    
        public_key = ['id_rsa_dosenet.pub']
        self.pe = ccrypt.public_d_encrypt(key_file_lst = public_key)
        self.IP = 'grim.nuc.berkeley.edu'
        self.port = 5005
        if self.arg.test:
            print 'UDP target IP @ port :', self.IP + ':' + self.port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # uses UDP protocol
        if self.args.test:
            self.IP = args.test[0] #Send to custom IP if testing

    def main(self):
        if self.args.test:
            print 'Testing complete, try a Raspberry Pi!'
        det = dosimeter(LED1=LED)  # Initialise dosimeter object from dosimeter.py
        while True: # Run until error or KeyboardInterrupt (Ctrl + C)
            try:
                if det.ping(hostname = 'berkeley.edu'):
                    cpm, cpm_error = det.getCPM()
                    if self.arg.test:
                        print 'CPM: ',cpm,' - ','CPM Error: ',cpm_error
                        print '\t~~~ Activate LED ~~~\n'
                    else:
                        det.activatePin(self.LED) # LIGHT UP
                    if len(det.counts) > 0: # Only run the next segment after the warm-up phase
                        # GET errorCode from det Object??????
                        error_code = 0 # Default 'working' state - error code 0
                        now = datetime.datetime.now()
                        c = ','
                        package = str(self.msg_hash) +c+ str(self.stationID) +c+ str(cpm) +c+ str(cpm_error) +c+ str(error_code)
                        print package
                        packet = self.pe.encrypt_message(package)[0]
                        print str(packet) # This really screws up Raspberry Pi terminal... without str()
                        self.socket.sendto(packet, (self.IP, self.port))
                        print 'Packet sent @ '+ str(now)+' - '+str(self.IP)+':'+str(self.port) 
                        time.sleep(120)
                else:
                    if self.arg.test:
                        print '\t~~~ Blink LED ~~~\n'
                    else: 
                        det.blink(self.LED, number_of_flashes = 10) # FLASH
            except (KeyboardInterrupt, SystemExit):
                print '.... User interrupt ....\n Byyeeeeeeee'
                GPIO.cleanup()
                sys.exit(0)
            except Exception as e:
                GPIO.cleanup()
                print str(e)
                sys.exit(1)

if __name__ == "__main__"
    sen = Sender()
    sen.parseArguments()
    sen.initialise()
    self.getDatafromCSV()
    sen.initVariables()
    sen.main()