#!/home/pi/miniconda/bin/python
# 
# Ryan Pavlovsky (until Mon Jun 15 2015)
# Navrit Bal (after Jun 15 2015)
# DoseNet
# Applied Nuclear Physics Division
# Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
# Adapted from dosometer.py (Ryan Pavlovsky)
# Last updated: Tue 16/06/15
##########################
## Run on Raspberry Pis ##
##########################
import RPi.GPIO as GPIO;
import numpy as np;
import datetime;
import time;
from time import sleep;

class dosimeter:
    def __init__(self):
        GPIO.setmode(GPIO.BCM) # Use Broadcom GPIO numbers - GPIO numbering system eg. GPIO 23 > pin 16. Not BOARD numbers, eg. 1, 2 ,3 etc.
        GPIO.setup(24,GPIO.IN,pull_up_down=GPIO.PUD_UP) #Sets up radiation detection GPIO; Check if pulled up/need resistor?
        GPIO.setup(23,GPIO.IN,pull_up_down=GPIO.PUD_UP) #Sets up microphonics detection; Check if pulled up
        GPIO.add_event_detect(24,GPIO.RISING,callback=self.UpdateCountCallback)
        self.counts = []
        sleep(1);

    def __del__(self):
        print 'Dosimeter object just died - __del__'
        self.close()

    def __exit__(self):
        print 'Dosimeter object just exited - __exit__'
        self.close()

    def close(self):
        print ''
        GPIO.cleanup()

    def UpdateCountCallback(self,channel):
        microphonics = GPIO.input(23)
        #Checks to see if microphonics detected anything before counting it as a "count"
        if microphonics == GPIO.LOW: # 0V
            self.counts.append(getDatetime())
        if microphonics == GPIO.HIGH: # 3.3V
            print 'Stop shaking meeeeee'

    def resetCounts(self):
        self.counts = self.counts[-120:] #Saves only the last 120 detected events before it resets for reaveraging
        ###########################################################
        # Is this why we get the exponential decrease pattern???? #
        ###########################################################

    def getCounts(self):
        return float(len(self.counts))

    def getCPMWithError(self):
        #########################
        # I need to change this #
        #########################
        counts = self.getCounts()
        if (counts < 2):
            return [0,0]
        diff = (getDatetime() - self.counts[0]).total_seconds()
        CPM = [ counts/diff*60., np.sqrt(counts)/diff*60 ]
        if( counts>300 or diff>200 ):  #Resets the averaging every 300 counts or every 200 seconds
            self.resetCounts()
        return CPM

    def getCPMError(self):
        return np.sqrt(len(self.counts))

    def getDatetime(self):
        return datetime.datetime.now()