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

# SIG >> float (~3.3V) --> 0.69V --> EXP charge back to float (~3.3V)
# NS  >> ~0V (GPIO.LOW) --> 3.3V (GPIO.HIGH) RPi rail

# Note: GPIO.LOW  - 0V
#       GPIO.HIGH - 3.3V or 5V ???? (RPi rail voltage)

class Dosimeter:
    def __init__(self,LED=20):
        self.counts = [] # Datetime list
        self.noise  = [] # Datetime list
        start = datetime.datetime.now()
        self.counts.append(start) # Initialise with the starting time so getCPM doesn't get IndexError - needs a 1 item minimum for [0] to work
        self.noise.append(start) # Initialise with the starting time so updateCount doesn't get IndexError - needs a 1 item minimum for [-1] to work
        self.microphonics = [] # errorFlag list
        self.margin = datetime.timedelta(microseconds = 100000) #100ms milliseconds is not an option
        GPIO.setmode(GPIO.BCM) # Use Broadcom GPIO numbers - GPIO numbering system eg. GPIO 23 > pin 16. Not BOARD numbers, eg. 1, 2 ,3 etc.
        GPIO.setup(24, GPIO.IN, pull_up_down = GPIO.PUD_UP) # SIG Sets up radiation detection; Uses pull up resistor on RPi
        GPIO.setup(23, GPIO.IN, pull_up_down = GPIO.PUD_UP) # NS  Sets up microphonics detection; Uses pull up resistor on RPi
        GPIO.setup(LED , GPIO.OUT)
        GPIO.add_event_detect(24, GPIO.FALLING, callback=self.updateCount, bouncetime=100)
        GPIO.add_event_detect(23, GPIO.FALLING, callback=self.updateNoise, bouncetime=1000)
        self.first_count = True
        self.first_noise = True
        sleep(1)
    
    def updateNoise(self,channel=23):
        if not self.first_noise:
            #Avoids IndexError from the initialisation issue
            #print 'updateNoise - ', str(datetime.datetime.now())
            self.noise.append(datetime.datetime.now())
            print '\t\t\t\t NOISE only'
        else:
            self.first_noise = False
            print '\t~~ Haven\'t got any noise yet ~~'

    def updateCount(self,channel=24):
        GPIO.setmode(GPIO.BCM)
        noiseInput = GPIO.input(23)
        if not self.first_count:
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
                print '\n\t\t\t NS was not GPIO.HIGH or GPIO.LOW'
        else:
            self.first_count = False
            print '\t~~ Haven\'t got any counts yet ~~'

    def countsToArr(self):
        self.counts = np.array(self.counts, dtype='M8[us]') # This dtype is super close to an open bug, this is a workaround?

    def countsToList(self):
        self.counts = self.counts.tolist()
        print '\t\t\t',self.counts

    def resetCounts(self, seconds=10):
        self.countsToArr()
        # Saves only the last number of seconds of events
        print '\t\t\t',self.counts
        # DOES THIS WORK ?????
        self.counts = self.counts[self.counts > self.counts[-1] - np.timedelta64(seconds,'s')] # Courtesy of Joey
        print '\t\t\t',self.counts
        self.countsToList()

    def getCount(self):
        return float(len(self.counts))

    def getCPM(self,accumulation_time=10):
        count = self.getCount()
        if count < 2:
            return 0, 0
        count_err = np.sqrt(count)
        now = datetime.datetime.now()
        counting_time = (now - self.counts[0]).total_seconds()
        cpm = count / counting_time * 60
        cpm_err = count_err / counting_time * 60
        print '\t\t\t\t\t~~~',count, count_err,'~~~'
        print '\t\t\t\t\t~~~',cpm, cpm_err,'~~~'
        # Resets the averaging every 5 minutes
        if(counting_time > accumulation_time): ############## Last 5 mintues of data
            if __name__=='calibrate':
                print '\t ~~~~ RESET ~~~~'
            self.resetCounts()
        return cpm, cpm_err

    def ping(self, hostname):
        response = os.system('ping -c 1 '  + hostname)
        # and then check the response...
        if response == 0:
          print '~ ', hostname, 'is up!'
          return True
        else:
          print '~ ', hostname, 'is down!'
          return False

    def activatePin(self,pin):
        GPIO.output(pin,True)
        #print 'Pin ON #:',pin,' - ',datetime.datetime.now()

    def deactivatePin(self,pin):
        GPIO.output(pin,False)
        #print 'Pin OFF #:',pin,' - ',datetime.datetime.now()

    def blink(self, pin=20, frequency = 0.5, number_of_flashes = 1):
        try:
            for i in range(0, number_of_flashes):
                #print 'Blinking on Pin #:',pin,' - ',datetime.datetime.now()
                print '\t\t *' # Flash
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
    det.updateCount()
    print det.counts
    det.updateNoise()
    print det.noise
    #
    print '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
    print '~~~~ Testing done. Entering while True loop ~~~~'
    print ' Waiting for Ctrl + C'
    MEASURE_TIME = 60
    while True:
        try: # getCPM
            sleep(1)
            if random.random() >= 0.5:
                count += 1
            print count
            cpm, cpm_err = det.getCPM(accumulation_time=MEASURE_TIME)
            print '\t','CPM: ',cpm,u'±',cpm_err,'\n'
        except (KeyboardInterrupt, SystemExit):
            print '.... User interrupt ....\n Byyeeeeeeee'
            GPIO.cleanup()
            sys.exit(0)
        except Exception as e:
            GPIO.cleanup()
            print str(e)
            sys.exit(1)