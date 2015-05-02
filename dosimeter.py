#!/home/pi/miniconda/bin/python
import RPi.GPIO as GPIO;
import numpy as np;
import datetime;
import time;
from time import sleep;

class dosimeter:
    def __init__(self):
        GPIO.setmode(GPIO.BCM);
        GPIO.setup(24,GPIO.IN,pull_up_down=GPIO.PUD_UP); #Sets up radiation detection GPIO; Check if pulled up/need resistor?
        GPIO.setup(23,GPIO.IN,pull_up_down=GPIO.PUD_UP); #Sets up microphonics detection; Check if pulled up
        GPIO.add_event_detect(24,GPIO.RISING,callback=self.count_update_hw_callback);
        self.counts=[];
        sleep(1);
    def count_update_hw_callback(self,channel):
        if GPIO.input(23) == GPIO.LOW:  #Checks to see if microphonics detected anything before counting it as a "count"
            self.counts.append(datetime.datetime.now());
    def reset_counts(self):
        self.counts=self.counts[-120:]; #Saves only the last 120 detected events before it resets for reaveraging
    def get_counts(self):
        return float(len(self.counts));
    def get_cpm(self):
        counts = self.get_counts();
        if (counts<2):
            return [0,0];
        diff=(datetime.datetime.now()-self.counts[0]).total_seconds();
        ret_val=[ counts/diff*60., np.sqrt(counts)/diff*60 ];
        if( counts>300 or diff>200 ):  #Resets the averaging every 300 counts or every 200 seconds
            self.reset_counts();
        return ret_val;
    def get_error_cpm(self):
        return np.sqrt(len(self.counts))