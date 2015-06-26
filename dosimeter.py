#!/home/pi/miniconda/bin/python
#
# Ryan Pavlovsky (until Mon Jun 15 2015)
# Navrit Bal (after Jun 15 2015)
# DoseNet
# Applied Nuclear Physics Division
# Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
# Adapted from dosimeter.py (Ryan Pavlovsky)
# Last updated: Thu 25/06/15
#####################################
## Indirectly run on Raspberry Pis ##
#####################################

import RPi.GPIO as GPIO
import numpy as np
import datetime
import time
from time import sleep

# SIG >> float (~3.3V) --> 0.69V --> EXP charge back to float (~3.3V)
# NS  >> ~0V (GPIO.LOW) --> 3.3V (GPIO.HIGH) RPi rail

# Note: GPIO.LOW  - 0V
#       GPIO.HIGH - 3.3V (RPi rail voltage)

class dosimeter:
    def __init__(self):
        GPIO.setmode(GPIO.BCM) # Use Broadcom GPIO numbers - GPIO numbering system eg. GPIO 23 > pin 16. Not BOARD numbers, eg. 1, 2 ,3 etc.
        GPIO.setup(24, GPIO.IN, pull_up_down = GPIO.PUD_UP) # SIG Sets up radiation detection; Uses pull up resistor on RPi
        GPIO.setup(23, GPIO.IN, pull_up_down = GPIO.PUD_UP) # NS  Sets up microphonics detection; Uses pull up resistor on RPi
        GPIO.add_event_detect(24, GPIO.FALLING, callback = self.updateCount)
        GPIO.add_event_detect(23, GPIO.RISING, callback = self.updateNoise)
        self.counts = [] # Datetime list
        self.noise  = [] # Datetime list
        self.microphonics = [] # errorFlag list
        self.margin = datetime.timedelta(microseconds = 100000) #100ms milliseconds is not an option
        sleep(1)

    def __del__(self):
        print ('Dosimeter object just died - __del__')
        self.close()

    def __exit__(self):
        print ('Dosimeter object just exited - __exit__')
        self.close()

    def close(self):
        print('Actually closing now')
        GPIO.cleanup()
    
    def updateNoise(self):
        now = datetime.datetime.now()
        print ('Stop shaking meeeeee', now)
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
        self.counts = np.array(self.counts, dtype='M8[us]')

    def countsToList(self):
        self.counts = self.counts.tolist()

    def resetCounts(self, seconds=120):
        self.countsToArr()
        # Saves only the last number of seconds of events
        self.counts = self.counts[self.counts > self.counts[-1] - datetime.timedelta(seconds=seconds)] # self.counts[-1] to dt.now()?
        self.countsToList()

    def getCounts(self):
        return float(len(self.counts))

    def getCPM(self):
        counts = self.getCounts()
        counts_err = np.sqrt(counts)
        now = datetime.datetime.now()
        counting_time = (now - self.counts[0]).total_seconds()
        cpm = counts / counting_time * 60
        cpm_err = counts_err / counting_time * 60
        # Resets the averaging every 300 counts or every 200 seconds
        if(counts > 300 or counting_time > 200):
            self.resetCounts()
        err_flag = False
        return cpm, cpm_err, err_flag


if __name__ == "__main__":
    dose = dosimeter()
    #TEST HERE