#!/home/pi/miniconda/bin/python
# -*- coding: utf-8 -*-
#
# Ryan Pavlovsky (until Mon Jun 15 2015)
# Navrit Bal (after Jun 15 2015)
# DoseNet
# Applied Nuclear Physics Division
# Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
# Adapted from dosimeter.py (Ryan Pavlovsky)
# Last updated: Fri 10/07/15
#####################################
## Indirectly run on Raspberry Pis ##
#####################################

import RPi.GPIO as GPIO
import numpy as np
import datetime
import time
from time import sleep
import os
import sys
import random
#import RPIO

# SIG >> float (~3.3V) --> 0.69V --> EXP charge back to float (~3.3V)
# NS  >> ~0V (GPIO.LOW) --> 3.3V (GPIO.HIGH) RPi rail

# Note: GPIO.LOW  - 0V
#       GPIO.HIGH - 3.3V or 5V ???? (RPi rail voltage)

class Dosimeter:
    def __init__(self, LED=20):
        self.counts = [] # Datetime list
        #self.noise  = [] # Datetime list
        start = datetime.datetime.now()
        self.counts.append(start) # Initialise with the starting time so getCPM doesn't get IndexError - needs a 1 item minimum for [0] to work
        """self.noise.append(start) # Initialise with the starting time so updateCount doesn't get IndexError - needs a 1 item minimum for [-1] to work
        self.microphonics = [] # errorFlag list
        self.margin = datetime.timedelta(microseconds = 100000) #100ms milliseconds is not an option
        """
        GPIO.setmode(GPIO.BCM) # Use Broadcom GPIO numbers - GPIO numbering system eg. GPIO 23 > pin 16. Not BOARD numbers, eg. 1, 2 ,3 etc.
        GPIO.setup(24, GPIO.IN, pull_up_down=GPIO.PUD_UP) # SIG Sets up radiation detection; Uses pull up resistor on RPi
        #GPIO.setup(23, GPIO.IN, pull_up_down=GPIO.PUD_UP) # NS  Sets up microphonics detection; Uses pull up resistor on RPi
        GPIO.add_event_detect(24, GPIO.FALLING, callback=self.updateCount_basic, bouncetime=200)
        #GPIO.add_event_detect(23, GPIO.FALLING, callback=self.updateNoise, bouncetime=1000)
        GPIO.setup(LED, GPIO.OUT)
        """RPIO.setmode(GPIO.BCM
        RPIO.setup(24, GPIO.IN, pull_up_down=RPIO.PUD_UP)
        RPIO.add_interrupt_callback(24, callback=testthing, edge='both',
            threaded_callback=False, debounce_timeout_ms=1)"""

    def updateCount_basic(self, channel=24):
        now = datetime.datetime.now()
        self.counts.append(now)         # Update datetime List
        print '  COUNT:',now            # Print to screen
        #self.blink(pin=20, frequency=1) # Blink count LED (#20)

    """def updateNoise(self,channel=23):
        if not self.first_noise:
            #Avoids IndexError from the initialisation issue
            #print 'updateNoise - ', str(datetime.datetime.now())
            self.noise.append(datetime.datetime.now())
            print '\t\t\t\t NOISE only'
        else:
            self.first_noise = False
            print '\t~~ Haven\'t got any noise yet ~~'"""

    """def updateCount(self,channel=24):
        GPIO.setmode(GPIO.BCM)
        #noiseInput = GPIO.input(23)
        now = datetime.datetime.now()
        if noiseInput: # == 1/True
            self.noise.append(now)
            print '\t\t\t\t NOISE only'
        elif not noiseInput: # ==0/False
            lastNoise = self.noise[-1] # Last datetime object in the noise list
            # Checks to see if microphonics detected within a 200ms window before deciding whether to change the
            # errorFlag to 'microphonics was HIGH' or leave as default
            if not (now - self.margin) <= lastNoise <= (now + self.margin):
                print '. #', int(self.getCount())
                self.counts.append(now) # Stores counts as a list of datetimes
                self.blink()
                self.microphonics.append(False) # errorFlag = False by default (no errror registered)
                # Remove later
                cpm, err = self.getCPM(); print cpm
            else:
                self.counts.append(now) # Stores counts as a list of datetimes
                self.blink()
                self.microphonics.append(True)
                print 'counts + ** NOISE **'
                # Remove later
                cpm, err = self.getCPM(); print cpm
                # print 'Stop shaking meeeeee'
        else:
            print '\n\t\t\t NS was not GPIO.HIGH or GPIO.LOW'"""

    def countsToArr(self):
        self.counts = np.array(self.counts, dtype='M8[us]')

    def countsToList(self):
        self.counts = self.counts.tolist()

    def resetCounts(self, seconds = 300):
        try:
            print counts
            self.countsToArr()
            """Saves only the last number of seconds of events
            Moving window
            Will lead to exponential decay behaviour...
            Change to fixed window scheme?"""
            print counts
            self.counts = self.counts[self.counts > self.counts[-1] - np.timedelta64(seconds,'s')] # Courtesy of Joey
            print counts
            self.countsToList()
            print counts
        except Exception as e:
            pass

    def getCount(self):
        return float(len(self.counts))

    def getCPM(self, accumulation_time = 300):
        now = datetime.datetime.now()
        print 'Now:', now
        count = self.getCount()
        print 'Count:', count
        if count < 2:
            return 0, 0
        count_err = np.sqrt(count)
        counting_time = (now - self.counts[0]).total_seconds()
        print 'Counting time:', counting_time
        cpm = count / counting_time * 60
        print 'CPM:', cpm
        cpm_err = count_err / counting_time * 60
        #print '\t\t~~ Count: ',count,' ~~ CPM: ', cpm
        # Resets the averaging every 5 minutes
        if(counting_time > accumulation_time): ########## Last 5 mintues of data
            print '\n\t\t ~~~~ RESET ~~~~\n'
            self.resetCounts(seconds = accumulation_time)
        return cpm, cpm_err

    def ping(self, hostname):
        response = os.system('ping -c 1 '  + hostname + '> /dev/null')
        # and then check the response...
        if response == 0:
          print '~ ', hostname, 'is up!'
          return True
        else:
          print '~ ', hostname, 'is DOWN!'
          return False

    def activatePin(self,pin):
        GPIO.output(pin,True)
        #print 'Pin ON #:',pin,' - ',datetime.datetime.now()

    def deactivatePin(self,pin):
        GPIO.output(pin,False)
        #print 'Pin OFF #:',pin,' - ',datetime.datetime.now()

    def blink(self, pin=20, frequency = 1, number_of_flashes = 1):
        try:
            for i in range(0, number_of_flashes):
                #print 'Blinking on Pin #:',pin,' - ',datetime.datetime.now()
                print '\t\t Flash' # Flash
                self.activatePin(pin)
                time.sleep(frequency/2)
                self.deactivatePin(pin)
                time.sleep(frequency/2)
                #print 'Just blinked Pin #:',pin,' - ',datetime.datetime.now()
        except (KeyboardInterrupt, SystemExit):
            print '.... User interrupt ....\n Byyeeeeeeee'
            GPIO.cleanup()
            sys.exit(0)
        except Exception, e:
            GPIO.cleanup()
            raise e
            sys.exit(1)

    def __del__(self):
        print ('Dosimeter object just died - __del__')
        self.close()
    def __exit__(self):
        print ('Dosimeter object just exited - __exit__')
        self.close()
    def close(self):
        print('Actually closing now')
        GPIO.cleanup()

if __name__ == "__main__":
    det = Dosimeter()
    response = det.ping(hostname='berkeley.edu')
    print 'Ping test berkeley.edu: ',response
    data = det.getCPM()
    print data
    det.updateCount_basic()
    print det.counts
    det.counts = []
    #det.updateNoise()
    #print det.noise
    #
    print '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
    print '~~~~ Basic testing done. Entering while True loop ~~~~'
    print ' Waiting for Ctrl + C'
    MEASURE_TIME = 10
    count = 0
    while True:
        try: # getCPM
            sleep(1)
            cpm, cpm_err = det.getCPM(accumulation_time = MEASURE_TIME)
            print '\t','CPM: ',cpm,u'±',cpm_err,'\n'
        except (KeyboardInterrupt, SystemExit):
            print '.... User interrupt ....\n Byyeeeeeeee'
            GPIO.cleanup()
            sys.exit(0)
        except Exception as e:
            GPIO.cleanup()
            print str(e)
            sys.exit(1)
