#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Navrit Bal
# DoseNet
# Applied Nuclear Physics Division
# Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
# Created:         Mon 15/06/15
# Last updated: Fri 24/06/15
#################
#  Run on GRIM  #
#################

import sys
import argparse
import csv
from tzwhere import tzwhere
import hashlib

USER = 'root'  # set while creating the instance
HOST = 'dosenet-0.cork9lvwvd2g.us-west-1.rds.amazonaws.com'  # obtained AFTER creating the instance
PORT = 3306  # default value (can be changed while creating the instance)
PASSWORD = 'radiationisrad'  # set while creating the instance
DATABASE = 'dosenet'   # set while creating the instance


class Parser:
    def __init__(self):
        parser = argparse.ArgumentParser('use -h for list of arguments')
        parser.add_argument(
            '--ID', type=int, nargs=1, required=False,
            help='Auto generated if not manually set.')
        parser.add_argument(
            'name', type=str, nargs=1, help='full name for plotting', metavar='NAME')
        parser.add_argument(
            'nickname', type=str, nargs=1, help='short name for csv files', metavar='NICKNAME')
        parser.add_argument(
            'lat', type=float, nargs=1, help='', metavar='LATITUDE')
        parser.add_argument(
            'long', type=float, nargs=1, help='', metavar='LONGITUDE')
        parser.add_argument(
            'conv', type=float, nargs=1, help='CPM to mrem', metavar='CONV')
        parser.add_argument(
            'display', type=int, nargs=1, help='display on(1)/off(0)', metavar='DISPLAY')
        parser.add_argument(
            'devices', type=str, nargs=1, help='sensors on(1)/off(0)', metavar='DEVICES')
        self.args = parser.parse_args()


class DBTool:
    def __init__(self, name, nickname, lat, lon, cpmtorem,
                 display, devices, *ID):
        self.data_path = "/home/dosenet/tmp/"
        try:
            self.ID = ID[0]
        except Exception as ex:
            print('Auto generating ID, good choice.')
        self.name = name
        self.nickname = nickname
        self.lat = lat
        self.lon = lon
        tz = tzwhere.tzwhere()
        self.timezone = tz.tzNameAt(lat, lon)
        print('New location at (', lat, ',', lon, ') in', self.timezone, ' timezone')
        self.cpmtorem = cpmtorem
        self.cpmtousv = cpmtorem*10
        self.display = display
        self.devices = devices
        self.gitBranch = "master"
        self.needsUpdate = 0
        self.new_station = ''
        if not ID:
            self.setID()
        self.md5hash = self.generate_hash()
        self.addDosimeterWithID()

    def setID(self):
        try:
            station_file = open(self.data_path + "Station.csv", "r")
            station_file.readline()
            lines = station_file.readlines()
            max_id = 0
            for line in lines:
                data = line.split(",")
                this_id = int(data[0])
                if 10000 > this_id and this_id > max_id:
                    max_id = this_id
            self.ID = max_id + 1
            station_file.close()
        except Exception as ex:
            raise ex

    def runText(self, text):
        try:
            station_file = open(self.data_path + "Station.csv", "a+")
            station_file.write(text)
            station_file.close()
        except Exception as ex:
            raise ex

    def addDosimeterWithID(self):
        if not self.check_unique(self.ID, self.name):
            print( str(self.ID) + " or " + str(self.name) + " already taken")
        else :
            text = "{},{},{},{},{},{},{},{},{},{},{},{},{}\n".format(
                self.ID, self.name, self.lat, self.lon, self.cpmtorem,
                self.cpmtousv, self.display, self.devices, self.gitBranch,
                self.needsUpdate, self.md5hash, self.nickname, self.timezone)
            self.runText(text)
            self.makeCSV()
            self.makeDataCSV()

    def check_unique(self, id, name):
        try:
            station_file = open(self.data_path + "Station.csv", "r")
            lines = station_file.readlines()
            for line in lines:
                data = line.split(",")
                if data[0] == id:
                    return False
                if data[1] == name:
                    return False
            station_file.close()
            return True
        except Exception as ex:
            raise ex

    def getID(self, name):
        try:
            station_file = open(self.data_path + "Station.csv", "r")
            lines = station_file.readlines()
            this_id = -1
            for line in lines:
                data = line.split(",")
                if name == data[1]:
                    this_id = data[0]
                    break
            station_file.close()
            return this_id
        except Exception as ex:
            raise ex

    def generate_hash(self):
        key = "{}{}{}".format(self.ID,self.lat,self.lon)
        return hashlib.md5(key.encode()).hexdigest()

    def makeCSV(self):
        fname = "/home/dosenet/config-files/%s.csv" % (self.nickname)
        with open(fname, 'wb') as csvfile:
            stationwriter = csv.writer(csvfile, delimiter=',')
            stationwriter.writerow(['stationID', 'message_hash', 'lat', 'long'])
            stationwriter.writerow([self.ID, self.md5hash, self.lat, self.lon])

    def makeDataCSV(self):
        sensors = ['','_d3s','_aq','_adc','_weather']
        meta_data = [,
                     []
                    ]
        for i,sensor in enumerate(sensors):
            self.devices[i]=='1':
                fname = "/home/dosenet/tmp/dosenet/%s_%s.csv" % (self.nickname,sensor)
                with open(fname, 'w') as csvfile:
                    fwriter = csv.writer(csvfile, delimeter=',')
                    if i==0:
                        fwriter.writerow(['deviceTime_utc','deviceTime_local','deviceTime_unix','cpm','cpmError','error_flag'])
                    if i==1:
                        meta_data = ['deviceTime_utc','deviceTime_local','deviceTime_unix','cpm','cpmError','keV_per_ch']
                        for i in range(1024):
                            meta_data.append(str(i))
                        meta_data.append('error_flag')
                        fwriter.writerow(meta_data)
                    if i==2:
                        fwriter.writerow(['deviceTime_utc','deviceTime_local','deviceTime_unix','PM1','PM25','PM10','error_flag'])
                    if i==3:
                        fwriter.writerow(['deviceTime_utc','deviceTime_local','deviceTime_unix','co2_ppm','noise','error_flag'])
                    if i==4:
                        fwriter.writerow(['deviceTime_utc','deviceTime_local','deviceTime_unix','temperature','pressure','humidity','error_flag'])


if __name__ == "__main__":
    parse = Parser()
    name = parse.args.name[0]
    nickname = parse.args.nickname[0]
    print(name)
    lat = parse.args.lat[0]
    lon = parse.args.long[0]
    cpmtorem = parse.args.conv[0]
    display = parse.args.display[0]
    devices = parse.args.devices[0]
    if not parse.args.ID:
        dbtool = DBTool(name, nickname, lat, lon, cpmtorem, display, devices)
    else:
        ID = parse.args.ID[0]
        print('Forced ID: ', ID)
        dbtool = DBTool(name, nickname, lat, lon, cpmtorem, display,
                        devices, ID)
    sys.exit(0)
