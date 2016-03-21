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
import MySQLdb as mdb
import argparse
import itertools
import csv

class Parser:
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--ID', type=int, nargs=1, required=False,
            help='Auto generated if not manually set.')
        parser.add_argument(
            '--name', type=str, nargs=2, required=True, help='')
        parser.add_argument(
            '--latlong', type=float, nargs=2, required=True, help='')
        parser.add_argument(
            '--conv', type=float, nargs=1, required=True, help='')
        parser.add_argument(
            '--display', type=int, nargs=1, required=True, help='')
        self.args = parser.parse_args()


class DBTool:
    def __init__(self, name, nickname, lat, lon, cpmtorem, display, *ID):
        # Open database connection
        self.db = mdb.connect(
            "127.0.0.1",
            "ne170group",
            "ne170groupSpring2015",
            "dosimeter_network")
        try:
            self.ID = ID[0]
        except Exception as ex:
            print 'Auto generating ID, good choice.'
        self.name = name
        self.nickname = nickname
        self.lat = lat
        self.lon = lon
        self.cpmtorem = cpmtorem
        self.cpmtousv = cpmtorem*10
        self.display = display
        # prepare a cursor object using cursor() method
        self.cursor = self.db.cursor()
        self.md5hash = ''
        self.new_station = ''
        self.initialState = self.getInitialState()
        if not ID:
            self.addDosimeter()
        else:
            self.addDosimeterWithID()

    def getInitialState(self):
        sql = "SELECT `Name`, IDLatLongHash FROM stations;"
        return self.runSQL(sql, everything=True)

    def addDosimeter(self):
        # Adds a row to dosimeter_network.stations
        sql = ("INSERT INTO stations " +
               "(`Name`,`Lat`,`Long`,`cpmtorem`,`cpmtousv`,`display`,IDLatLongHash,`nickname`) " +
               "VALUES " +
               "('%s','%s','%s','%s','%s','%s','This should not be here :(','%s');"
               % (self.name, self.lat, self.lon, self.cpmtorem, self.cpmtousv, self.display, self.nickname))
        self.runSQL(sql)
        self.main()

    def addDosimeterWithID(self):
        sql = (
            "INSERT INTO stations " +
            "(`ID`,`Name`,`Lat`,`Long`,`cpmtorem`,`cpmtousv`,`display`,IDLatLongHash,`nickname`) " +
            "VALUES " +
            "('%s','%s','%s','%s','%s','%s','%s','This should not be here :(','%s');"
            % (self.ID, self.name, self.lat, self.lon, self.cpmtorem,
               self.cpmtousv, self.display, self.nickname))
        self.runSQL(sql)
        self.main()

    def getID(self, name):
        # The database uses auto-incremented ID numbers so we need to get
        # the ID from the `dosimeter_network.stations` table for when we
        # add the hash
        # RUN "SELECT ID  FROM stations WHERE name = 'SOME NAME';"
        sql = "SELECT ID FROM stations WHERE name = '%s';" % (self.name)
        self.ID = self.runSQL(sql, least=True)
        if 1 <= self.ID <= 3:
            print 'Check the DB (stations) - there\'s probably an ID collision'
        elif self.ID <= 0:
            print 'ID less than 0?? There\'s a problem afoot'
        elif self.ID is None:
            print 'ID is None... Byyeeeeeeee'
            sys.exit(1)
        else:
            print 'ID looks good'

    def getHash(self):
        # RUN "SELECT MD5(CONCAT(`ID`, `Lat`, `Long`))
        #         FROM stations
        #         WHERE `ID` = $$$ ;"
        sql = "SELECT MD5(CONCAT(`ID`, `Lat`, `Long`)) FROM stations \
                WHERE `ID` = '%s' ;" % (self.ID)
        self.md5hash = self.runSQL(sql, least=True)

    def setHash(self):
        # Sets a MD5 hash of the ID, Latitude & for security reasons...

        # RUN "UPDATE stations
        #        SET IDLatLongHash = 'SOME MD5 HASH'
        #         WHERE ID = $$$ ;"
        sql = "UPDATE stations SET IDLatLongHash = '%s' \
                 WHERE ID = '%s';" % (self.md5hash, self.ID)
        self.runSQL(sql)

    def getNewStation(self):
        sql = "SELECT * FROM stations WHERE ID = '%s';" % (self.ID)
        return self.runSQL(sql, less=True)

    def checkIfDuplicate(self):
        # Check for Name, ID, or MD5 hash collision (duplicate entry)
        print 'Checking for duplicates...'
        if any(str(self.name) in i for i in self.initialState):
            print ('ERROR: Duplicate NAME detected, not commiting changes. ' +
                   'Byyeeeeeeee')
            return True
        elif any(str(self.ID) in i for i in self.initialState):
            print ('ERROR: Duplicate ID detected, not commiting changes. ' +
                   'Byyeeeeeeee')
            return True
        elif any(str(self.md5hash) in i for i in self.initialState):
            print ('ERROR: Duplicate HASH detected, not commiting changes. ' +
                   'Byyeeeeeeee')
            return True
        else:
            print 'Good news: no duplicates'
            return False

    def runSQL(self, sql, least=False, less=False, everything=False):
        print '\t\t\t SQL: ', sql
        try:
            self.cursor.execute(sql)
            if least:
                result = self.cursor.fetchall()[0][0]
                return result
            if less:
                result = self.cursor.fetchall()[0]
                return result
            if everything:
                result = self.cursor.fetchall()
                return result
        except (KeyboardInterrupt, SystemExit):
            pass
        except Exception, e:
            print sql
            raise e

    def makeCSV(self):
        fname = "%s.csv" % (self.nickname)
        with open(fname, 'wb') as csvfile:
            stationwriter = csv.writer(csvfile, delimiter=',')
            stationwriter.writerow(['stationID', 'message_hash, lag, long'])
            stationwriter.writerow([self.ID, self.md5hash, self.lat, self.lon])

    def main(self):
        self.duplicate = True
        try:
            print 'GET ID'
            self.getID(self.name)
            print 'GET HASH'
            self.getHash()
            print 'SET HASH'
            self.setHash()
            print 'GET NEW STATION'
            self.new_station = self.getNewStation()
            print self.new_station
            if not self.checkIfDuplicate():
                print 'Good news: Committing changes!'
                self.db.commit()
                print 'Generating csv file for this location'
                self.makeCSV()
                print 'SUCESSSSSS'
        except Exception as ex:
            print '\t ~~~~ FAILED ~~~~'
            raise ex

if __name__ == "__main__":
    parse = Parser()
    name = parse.args.name[0]
    nickname = parse.args.name[1]
    print name
    lat = parse.args.latlong[0]
    lon = parse.args.latlong[1]
    cpmtorem = parse.args.conv[0]
    display = parse.args.display[0]
    if not parse.args.ID:
        dbtool = DBTool(name, nickname, lat, lon, cpmtorem, display)
    else:
        ID = parse.args.ID[0]
        print 'Forced ID: ', ID
        dbtool = DBTool(name, nickname, lat, lon, cpmtorem, display, ID)
