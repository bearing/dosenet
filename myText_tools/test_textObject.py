import unittest
from unittest import TestCase

from mytext_tools import *

'''
This is a unit test for mytext_tools.py

Inside the class, TestTextObject, I wrote a method that individually test  
each type of data. 

When you run the class, it run all the methods; then it add a row of testing
data into the text file. If no file exist, it create one.

The row of data is written in the order of device time,1,2,3...(some may float).
'''

class TestTextObject(unittest.TestCase):

    def test_inject(self):
        mydb = TextObject()
        data = {'hash': 'abc', 'stationID': 1, 'cpm': 2.0,
                'cpm_error': 3.0, 'error_flag': 4}
        mydb.inject(data)
        r = open(mydb.Data_Path + "1_Dosimeter.csv")
        d = r.readlines()[-1]
        r.close()
        self.assertTrue(d.endswith(",1,2.0,3.0,4\n"))

    def test_injectAQ(self):
        mydb = TextObject()
        data = {'hash': 'abc', 'stationID': 1, 'oneMicron': 2.0,
                'twoPointFiveMicron': 3.0, 'tenMicron': 4.0, 'error_flag': 5}
        mydb.injectAQ(data)
        r = open(mydb.Data_Path + "1_AQ.csv")
        d = r.readlines()[-1]
        r.close()
        self.assertTrue(d.endswith(",1,2.0,3.0,4.0,5\n"))

    def test_injectCO2(self):
        mydb = TextObject()
        data = {'hash': 'abc', 'stationID': 1, 'co2_ppm': 2.0, 'noise':
                3.0, 'error_flag': 4}
        mydb.injectCO2(data)
        r = open(mydb.Data_Path + "1_CO2.csv")
        d = r.readlines()[-1]
        r.close()
        self.assertTrue(d.endswith(",1,2.0,3.0,4\n"))

    def test_injectWeather(self):
        mydb = TextObject()
        data = {'hash': 'abc', 'stationID': 1, 'temperature': 2.0, 'pressure':
                3.0, 'humidity': 4.0, 'error_flag': 5}
        mydb.injectWeather(data)
        r = open(mydb.Data_Path + '1_Weather.csv')
        d = r.readlines()[-1]
        r.close()
        self.assertTrue(d.endswith(",1,2.0,3.0,4.0,5\n"))

if __name__ == '__main__':
    unittest.main()