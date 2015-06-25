#!/home/pi/miniconda/bin/python
# 
# Ryan Pavlovsky (until Mon Jun 15 2015)
# Navrit Bal (after Jun 15 2015)
# DoseNet
# Applied Nuclear Physics Division
# Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
# Adapted from dosimeter.py (Ryan Pavlovsky)
# Last updated: Tue 16/06/15
#####################################
## Indirectly run on Raspberry Pis ##
#####################################

import RPi.GPIO as GPIO
import numpy as np
import datetime
import time
from time import sleep

# SIG only >> -VE V
# SIG + NS >> +VE V

class dosimeter:
    def __init__(self):
        GPIO.setmode(GPIO.BCM) # Use Broadcom GPIO numbers - GPIO numbering system eg. GPIO 23 > pin 16. Not BOARD numbers, eg. 1, 2 ,3 etc.
        GPIO.setup(24,GPIO.IN,pull_up_down=GPIO.PUD_UP) # SIG Sets up radiation detection; Uses pull up resistor on RPi
        GPIO.setup(23,GPIO.IN,pull_up_down=GPIO.PUD_UP) # NS  Sets up microphonics detection; Uses pull up resistor on RPi
        GPIO.add_event_detect(24,GPIO.FALLING,callback=self.updateCount)
        GPIO.add_event_detect(23,GPIO.RISING, callback=self.updateNoise)
        self.counts = [] # Datetime and errorFlag list
        self.noise  = [] # Datetime list
        self.margin = datetime.timedelta(microseconds=100000) #100ms milliseconds is not an option
        sleep(1)

    def __del__(self):
        print 'Dosimeter object just died - __del__'
        self.close()

    def __exit__(self):
        print 'Dosimeter object just exited - __exit__'
        self.close()

    def close(self):
        print ''
        GPIO.cleanup()

    def updateNoise(self):
        print 'Stop shaking meeeeee'
        now = getDatetime()
        print now
        self.noise.append(now)

    def updateCount(self):
        #microphonics = GPIO.input(23)
        now = getDatetime()
        lastMicrophonics = self.noise[-1] # Last datetime object in the noise list
        #Checks to see if microphonics detected within a 200ms window before deciding whether to change the 
        # errorFlag to 'microphonics was HIGH' or leave as default
        if not (now - self.margin) <= lastMicrophonics <= (now + self.margin):
            self.counts.append(now,0) # Stores counts as a list of datetimes and an errorFlag
                                     # errorFlag = 0 by default (no errror registered)
        else:
            self.counts.append(now,1) # Stores counts as a list of datetimes and an errorFlag
            #print 'Stop shaking meeeeee'
        # Note: GPIO.LOW  - 0V
        #       GPIO.HIGH - 3.3V (RPi rail voltage)
            

    def resetCounts(self):
        self.counts = self.counts[-120:] #Saves only the last 120 detected events before it resets for reaveraging
        # CHANGE this to a timedelta of 2 minutes?
        # Isn't this quite a lot of counts - too many?
        ###########################################################
        # Is this why we get the exponential decrease pattern???? #
        ###########################################################

    def getCounts(self):
        return float(len(self.counts))

        #Discuss logic of this with Ryan - motivation behind numbers?
    def getCPMWithError(self):
        #########################
        # I need to change this #
        #########################
        counts = self.getCounts()
        now = getDatetime()
        if (counts < 2): # ?????????? Ask Ryan
            return [0,0]
        diff = (now - self.counts[0]).total_seconds()
        CPM = [ counts/diff*60., np.sqrt(counts)/diff*60 ]
        # ?????????? Ask Ryan
        if( counts>300 or diff>200 ):  #Resets the averaging every 300 counts or every 200 seconds
            self.resetCounts()
        return CPM

    def getCPMError(self):
        return np.sqrt(len(self.counts))

    def getDatetime(self):
        return datetime.datetime.now()