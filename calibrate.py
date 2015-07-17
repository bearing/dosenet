#!/home/pi/miniconda/bin/python
# -*- coding: utf-8 -*-
##########################
## Run on Raspberry Pis ##
##########################

import sys
from time import sleep
import datetime
import numpy as np
import RPi.GPIO as GPIO
from dosimeter import Dosimeter

class Calibrate:
    def __init__(self):
        self.MEASURE_TIME = 10 # seconds
        print '\n\t ~~~~ You\'re running this fun calibration class. \
                \n Now go get your source(s). Maybe that Uranium ore on the bench will be useful...\n'
        print 'Accumulation Time: ', self.MEASURE_TIME
    def __del__(self):
        print ('Dosimeter object just got killed - __del__')
        self.close()
    def __exit__(self):
        print ('Dosimeter object just exited - __exit__')
        self.close()
    def close(self):
        print('Actually closing now')
        GPIO.cleanup()
        sys.exit(0)
    def main(self):
        det = Dosimeter()
        while True:
            try: # getCPM
                sleep(1)
                cpm, cpm_err = det.getCPM(accumulation_time=self.MEASURE_TIME)
            except (KeyboardInterrupt, SystemExit):
                print '.... User interrupt ....\n Byyeeeeeeee'
                GPIO.cleanup()
                sys.exit(0)
            except Exception as e:
                GPIO.cleanup()
                print str(e)
                sys.exit(1)

if __name__ == '__main__':
    cal = Calibrate()
    cal.main()