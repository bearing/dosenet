#!/home/pi/miniconda/bin/python
# -*- coding: utf-8 -*-
##########################
## Run on Raspberry Pis ##
##########################

import sys
from time import sleep
import numpy as np
import RPi.GPIO as GPIO
from dosimeter import Dosimeter

class Calibrate:
    def __init__():
        self.MEASURE_TIME = 5 # seconds
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
    def getCPM(self):
        counts = det.getCount()
        counts_err = np.sqrt(counts)
        now = datetime.datetime.now()
        counting_time = (now - self.counts[0]).total_seconds()
        cpm = counts / counting_time * 60
        cpm_err = counts_err / counting_time * 60
        print ' Counts: ',counts,u'±',counts_err,'\t','CPM: ',cpm,u'±',counts_err,'\n'
        if (counting_time >= self.MEASURE_TIME):
            det.resetCounts(seconds=self.MEASURE_TIME)
            print '\t\t\t ~~~ Reset ~~~'

    def main(self):
        det = dosimeter()
        while True:
            try: # getCPM
                sleep(1)
                self.getCPM()
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