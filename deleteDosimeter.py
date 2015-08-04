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
        print log
        if log:
            self.log = True
        else:
            self.log = False

    def getArguments(self,ID,**kwargs):
        self.ID = ID
        print kwargs
        self.before = False
        self.after = False
        self.dropdata = False
        self.dropstations = False
        try:
            self.before = kwargs['before']
        except:
            pass
        try:
            self.after = kwargs['after']
        except:
            pass
        try:
            self.dropdata = kwargs['dropdata']
        except:
            pass
        try:
            self.dropstations = kwargs['dropstations']
        except:
            pass
        try:
            self.name = self.runSQL(("SELECT `Name` FROM stations WHERE ID = %s;") % self.ID, least=True)
            print 'Operating on ',self.name
        except Exception as e:
            print 'ERROR: Could not get name of station. You should stop...'
            raise e
            sys.exit(1)
        if self.log:
            print 'Logging to ', self.LOG_NAME
        self.main()

    def deleteStation(self):
        # Get station row that we're about to delete - append to log
        msg = 'DELETING A STATION'
        select = ("SELECT * FROM dosnet WHERE stationID = %s;" % self.ID)
        self.getDataSample(select=select,limit=self.limit)
        print 'This will disable authentication of a dosimeter until readded \
separately with addDosimeterToDB.py. \n \
If there was more than one return above, you should QUIT.'
        if self.confirm():
            sql = "DELETE FROM stations WHERE stationID = %s LIMIT 1;" % self.ID
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
                    AND `receiveTime` < '%s' LIMIT %s" \
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
                            AND `receiveTime` > '%s' LIMIT %s" \
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
        rows = self.runSQL(select)
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
            print '~~ Exiting with no changes: did not read "yes"'
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
            remaining = self.runSQL("SELECT Name FROM stations;",less=True)
            print '~~~ Remaining stations! ~~~'
            print remaining

if __name__ == "__main__":
    print '\t\t\t ~~ Hai Joey! ~~'
    print '\tThis script deletes data from the dosimeter network database on GRIM'
    print '\tAre you sure you want to proceed??? (Type yes to proceed) \n$ '
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
        try:
            before = par.args.before[0]
            deleter.getArguments(ID,before)
        except:
            print 'No --before flag'
        try:
            after = par.args.after[0]
            deleter.getArguments(ID,after)
        except:
            print 'No --after flag'
        try:
            dropdata = par.args.dropalldata[0]
            deleter.getArguments(ID,dropdata)
        except:
            print 'No --dropalldata flag'
        try:
            dropstations = par.args.dropallstations[0]
            deleter.getArguments(ID,dropstations)
        except:
            print 'No --dropallstations flag'
        deleter.getArguments(ID)
    else:
        print 'You have decided not to delete data, thanks!'
        print 'If this was a mistake, type yes next time...'
