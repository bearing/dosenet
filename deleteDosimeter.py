#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Navrit Bal
# DoseNet
# Applied Nuclear Physics Division
# Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
# Created: 		Mon 15/06/15
# Last updated: Fri 24/06/15

# Delete from row stations table (append log)
    # --station [int]
# Delete all data for a station ID (append log)
    # Before --before [datetime]
    # After  --after [datetime]
    # Range  --range *before[datetime]* *after[datetime]*

# Nuke all data (append log)
    # --dropalldata FORREALSUPERSERIOUSOMGOMGOMG
# Nuke all stations (append log, backup first)
    # --dropallstations FORREALSUPERSERIOUSOMGOMGOMG

import datetime
import sys
import argparse
import MySQLdb as mdb

class Parser:
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--ID',type=int,nargs=1,required=True,\
            help='')
        parser.add_argument('--before',nargs=1,required=False,\
            help='')
        parser.add_argument('--after',nargs=1,required=False,\
            help='')
        parser.add_argument('--daterange',nargs=2,required=False,\
            help='')
        parser.add_argument('--dropalldata',required=False,\
            help='')
        parser.add_argument('--dropallstations',required=False,\
            help='')
        parser.add_argument('--log',required=False,default=True,\
            help='')
        self.args = parser.parse_args()

class DataDestroyer:
    """ API for cleaning up and managing removal of data from the database safely.

    Args:
        --ID (int):
        --before (datetime):
        --after (datetime):
        --range (datetime datetime):
        --dropalldata (str):
        --dropallstations (str):
        --log:

    Attributes:
        db (SQLObject): Custom MySQL database object for injecting into.
        cursor (db.cursor): Used for accessing database returns.
    """
    def __init__(self,log=False):
        self.db = mdb.connect("localhost", # Open database connection
                              "ne170group",
                              "ne170groupSpring2015",
                              "dosimeter_network")
        self.cursor = self.db.cursor() # prepare a cursor object using cursor() method
        self.LOG_NAME = 'deleteDosimeter.log'
        self.limit = 10
        self.secure_password = 'FORREALSUPERSERIOUSOMGOMGOMG'
        self.exit_serious_message = 'Exiting: did not receive confirmation: \
                                    "%s"' % self.secure_password
        self.could_not_append = 'ERROR: Could not append change to log file: ', \
                                self.LOG_NAME, '\n EXITING NOW'
        if log:
            self.log = True
        else:
            self.log = False

    def getArguments(self,ID,arg):
        self.ID = ID
        self.before = False
        self.after = False
        self.dropdata = False
        self.dropstations = False
        if arg == 'before':
            self.before = True
        elif arg == 'after':
            self.after = True
        elif arg == 'dropdata':
            self.dropdata = True
        elif arg == 'dropstations':
            self.dropstations = True
        else:
            print '\t\t ~~~ No extra flags given ~~~'
        try:
            self.name = self.runSQL(("SELECT `Name` FROM stations WHERE ID = %s;") % self.ID, least=True)
            print '\t\t ~~~ Operating on ',self.name, ' ~~~'
        except Exception as e:
            print '\n\tERROR: Could not get name of station. You should stop...'
            print str(e)
            print '\n\t\t\t~~ EXIT? ~~'
            if self.confirm():
                sys.exit(1)
        if self.log:
            print '\t\t ~~~ Logging to ', self.LOG_NAME, ' ~~~'
        self.main()

    def deleteStation(self):
        # Get station row that we're about to delete - append to log
        msg = '\t\t\t ~~~ DELETING A STATION ~~~'
        print msg
        select = ("SELECT * FROM dosnet WHERE stationID = %s LIMIT %s;" % (self.ID, self.limit))
        self.getDataSample(select=select,limit=self.limit)
        print '\nThis will disable authentication of a dosimeter until readded \
separately with addDosimeterToDB.py. \n \
If there was more than one station above, you should QUIT.'
        print '\n\t DEAUTHENICATE STATION?'
        if self.confirm():
            sql = "DELETE FROM stations WHERE ID = %s LIMIT 1;" % self.ID
            self.runSQL(sql)
            if self.log:
                self.appendLog(sql,msg)
        print '\t ~ Delete all the data for this stations? ~'
        if self.confirm():
            sql = "DELETE FROM dosnet WHERE stationID = %s;" % self.ID
        self.runSQL(sql)
        if self.log:
            self.appendLog(sql,msg)

    def deleteDataBefore(self):
        select = "SELECT * FROM dosnet WHERE stationID = %s \
                    AND `receiveTime` < '%s' ORDER BY `receiveTime` DESC LIMIT %s" \
                    % (self.ID, self.before, self.limit)
        self.getDataSample(select=select,limit=self.limit)
        msg = 'DELETING ALL DATA BEFORE: ',self.before, 'for ID: ', self.ID
        print msg
        if self.confirm():
            sql = "DELETE FROM dosnet WHERE stationID = %s \
                    AND `receiveTime` < '%s';" % (self.ID, self.before)
            self.runSQL(sql)
            if self.log:
                self.appendLog(sql,msg)

    def deleteDataAfter(self):
        select = "SELECT * FROM dosnet WHERE stationID = %s \
                            AND `receiveTime` > '%s' ORDER BY `receiveTime` ASC LIMIT %s" \
                            % (self.ID, self.after, self.limit)
        self.getDataSample(select=select,limit=self.limit)
        msg = 'DELETING ALL DATA AFTER: ',after, 'for ID: ', self.ID
        print msg
        if self.confirm():
            sql = "DELETE FROM dosnet WHERE stationID = %s \
                    AND `receiveTime` > '%s';" % (self.ID, self.after)
            self.runSQL(sql)
            if self.log:
                self.appendLog(sql,msg)

    def deleteDataRange(self):
        select = "SELECT * FROM dosnet WHERE stationID = %s \
                    AND (`receiveTime` BETWEEN '%s' AND '%s') LIMIT %s" \
                    % (self.ID, self.before, self.after, self.limit)
        self.getDataSample(select=select,limit=self.limit)
        msg = 'DELETING ALL DATA BEFORE: ',self.before, 'for ID: ', self.ID
        print msg
        if self.confirm():
            sql = "DELETE FROM dosnet WHERE stationID = %s \
                AND (`receiveTime` BETWEEN '%s' AND '%s');" % (self.ID, self.before, self.after)
            self.runSQL(sql)
            if self.log:
                self.appendLog(sql,msg)

    def getDataSample(self,select,limit):
        rows = self.runSQL(select, everything = True)
        print '\t\t\t\t\tSQL:', select
        print 'Sample of data you\'re about to delete (Max: "%s"): \n "%s"' % (limit, rows)

    def deleteAllData(self):
        msg = 'Truncating table dosnet - all data in the table will be gone.'
        print msg
        if self.confirm():
            print 'Enter the super secure secret password: \t $ '
            if raw_input() == self.secure_password:
                sql = "TRUNCATE TABLE dosnet;"
                self.runSQL(sql)
            else:
                print self.exit_serious_message
            if self.log:
                self.appendLog(sql,msg)

    def deleteAllStations(self):
        msg = 'Truncating table stations - all data in the table will be gone.'
        print msg
        if self.confirm():
            print 'Enter the super secure secret password: \t $ '
            if raw_input() == self.secure_password:
                sql = "TRUNCATE TABLE stations;"
                self.runSQL(sql)
            else:
                print self.exit_serious_message
            if self.log:
                self.appendLog(sql,msg)

    def appendLog(self,*some_text):
        for text in some_text:
            try:
                with open(self.LOG_NAME, 'a') as logfile:
                    msg = datetime.datetime.now(),'\n\t',text
                    logfile.write(str(msg))
            except (KeyboardInterrupt, SystemExit):
                print '.... User interrupt ....\n Byyeeeeeeee'
                sys.exit(0)
            except Exception as e:
                print self.could_not_append
                raise e
                sys.exit(1)
            finally:
                logfile.close()

    def confirm(self):
        print '\nType yes to confirm the operation: '
        if raw_input() == 'yes':
            return True
        else:
            print '~~ Exiting last prompt: did not read "yes"'
            return False

    def runSQL(self,sql, least=False, less=False, everything=False):
        print '\t\t\t\t\t SQL: ',sql
        self.cursor.execute(sql)
        try:
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
        except Exception as e:
            print sql
            raise e

    def main(self):
        if self.before:
            self.deleteDataBefore()
        elif self.after:
            self.deleteDataAfter()
        elif self.dropdata:
            self.deleteAllData()
        elif self.dropstations:
            self.deleteAllStations()
        else: # default, delete stations
            self.deleteStation()
        self.db.commit()
        # REVIEW
        if self.before:
            pass
        elif self.after:
            pass
        elif self.dropdata:
            pass
        elif self.dropstations:
            pass
        else: # default, delete stations
            remaining = self.runSQL("SELECT Name FROM stations;", everything=True)
            print '~~~ Remaining stations! ~~~'
            print remaining

if __name__ == "__main__":
    print '\n\t\t\t ~~ Hai Joey! ~~'
    print '\tThis script deletes data from the dosimeter network database on GRIM'
    print '\tAre you sure you want to proceed??? (Type yes to proceed)'
    if raw_input() == 'yes':
        par = Parser()
        ID = par.args.ID[0]
        if par.args.log:
            deleter = DataDestroyer(log=True)
        else:
            deleter = DataDestroyer()
        if par.args.daterange:
            print '--before and --after arguments ignored'
            before = par.args.daterange[0]
            after = par.args.daterange[1]
        elif par.args.before:
            before = par.args.before
            print 'Before: ', before
            deleter.getArguments(ID,before)
        elif par.args.after:
            after = par.args.after
            print 'After: ', after
            deleter.getArguments(ID,after)
        elif par.args.dropalldata:
            dropdata = par.args.dropalldata[0]
            deleter.getArguments(ID,dropdata)
        elif par.args.dropallstations:
            dropstations = par.args.dropallstations[0]
            deleter.getArguments(ID,dropstations)
        else:
            deleter.getArguments(ID,'null')
    else:
        print 'You have decided not to delete data, thanks!'
        print 'If this was a mistake, type yes next time...'
