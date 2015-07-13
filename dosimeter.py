#!/home/pi/miniconda/bin/python
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

# SIG >> float (~3.3V) --> 0.69V --> EXP charge back to float (~3.3V)
# NS  >> ~0V (GPIO.LOW) --> 3.3V (GPIO.HIGH) RPi rail

# Note: GPIO.LOW  - 0V
#       GPIO.HIGH - 3.3V (RPi rail voltage)

class Dosimeter:
    def __init__(self,LED1=7):
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
        #GPIO.setup(LED1 , GPIO.OUT)
        GPIO.add_event_detect(24, GPIO.FALLING, callback=self.updateCount)
        GPIO.add_event_detect(23, GPIO.RISING, callback=self.updateNoise)
    
    def updateNoise(self):
        now = datetime.datetime.now()
        print ('Stop shaking meeeeee - ', str(now))
        self.noise.append(now)

    def updateCount(self):
        now = datetime.datetime.now()
        lastMicrophonics = self.noise[-1] # Last datetime object in the noise list
        # Checks to see if microphonics detected within a 200ms window before deciding whether to change the
        # errorFlag to 'microphonics was HIGH' or leave as default
        if not (now - self.margin) <= lastMicrophonics <= (now + self.margin):
            self.counts.append(now) # Stores counts as a list of datetimes
            self.microphonics.append(False) # errorFlag = False by default (no errror registered)
        else:
            self.counts.append(now) # Stores counts as a list of datetimes
            self.microphonics.append(True)
            # print ('Stop shaking meeeeee')

    def countsToArr(self):
        self.counts = np.array(self.counts, dtype='M8[us]') # This dtype is super close to an open bug, this is a workaround?

    def countsToList(self):
        self.counts = self.counts.tolist()

    def resetCounts(self, seconds=120):
        self.countsToArr()
        counts = self.counts
        # Saves only the last number of seconds of events
        self.counts = counts[counts > counts[-1] - np.timedelta64(seconds,'s')] # Courtesy of Joey
        self.countsToList()

    def getCount(self):
        return len(self.counts)

    def getCPM(self):
        count = self.getCount()
        count_err = np.sqrt(count)
        now = datetime.datetime.now()
        counting_time = (now - self.counts[0]).total_seconds()
        cpm = count / counting_time * 60
        cpm_err = count_err / counting_time * 60
        # Resets the averaging every 300 counts or every 200 seconds
        #if(count > 300 or counting_time > 200):
        # Resets the averaging every 4 minutes
        if(counting_time > 240):
            self.resetCounts()
        return cpm, cpm_err

    def ping(self, hostname):
        response = os.system('ping -c 1'  + hostname)
        # and then check the response...
        if response == 0:
          print '~ ', hostname, 'is up!'
          return True
        else:
          print '~ ', hostname, 'is down!'
          return False

    def activatePin(self,pin):
        GPIO.output(pin,True)
        print 'Pin ON #:',pin,' - ',datetime.datetime.now()

    def deactivatePin(self,pin):
        GPIO.output(pin,False)
        print 'Pin OFF #:',pin,' - ',datetime.datetime.now()

    def blink(self, pin, frequency = 0.2, number_of_flashes = 1):
        try:
            for i in range(0, number_of_flashes):
                print 'Blinking on Pin #:',pin,' - ',datetime.datetime.now()
                activatePin(pin)
                time.sleep(frequency)
                deactivatePin(pin)
                print 'Just blinked Pin #:',pin,' - ',datetime.datetime.now()
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
    det.updateCount()
    print det.counts
    det.updateNoise()
    print det.noise()