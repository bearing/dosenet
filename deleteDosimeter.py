#!/usr/bin/env python
#
# Navrit Bal
# DoseNet
# Applied Nuclear Physics Division
# Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
# Created: 		Mon 15/06/15
# Last updated: Thu 23/06/15

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

class Parser(object):
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--ID',type=int,nargs=1,required=True,\
            help='')
        parser.add_argument('--before',type=str,nargs=1,required=False,\
            help='')
        parser.add_argument('--after',type=str,nargs=1,required=False,\
            help='')
        parser.add_argument('--range',type=str,nargs=2,required=False,\
            help='')
        parser.add_argument('--dropalldata',type=str,required=False,\
            help='')
        parser.add_argument('--dropallstations',type=str,required=False,\
            help='')
        parser.add_argument('--log',required=False,default=True,\
            help='')
        self.args = parser.parse_args()

class DataDestroyer(object):
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

	def __init__(self):
		self.db = mdb.connect("localhost", # Open database connection
						      "ne170group",
                              "ne170groupSpring2015,
                              "dosimeter_network")
		self.cursor = db.cursor() # prepare a cursor object using cursor() method
        self.ID = Parser.args.ID
        try:
            self.cursor.execute("SELECT `Name` FROM stations WHERE ID = '%s'") % self.ID
            self.name = self.cursor.fetchall()
            print 'Operating on ',self.name
        except Exception as e:
            print 'ERROR: Could not get name of station. You should stop...'
            raise e
            sys.exit(1)
        if Parser.args.log:
            self.LOG_NAME = 'deleteDosimeter.log'
            print 'Logging to ', LOG_NAME
        self.limit = 10
        self.secure_password = 'FORREALSUPERSERIOUSOMGOMGOMG'
        self.exit_serious_message = 'Exiting: did not receive confirmation: \
                                    "%s"' % self.secure_password
        self.could_not_append = 'ERROR: Could not append change to log file: ', \
                                self.LOG_NAME, '\n EXITING NOW'

    def deleteStation(self):
        # Get station row that we're about to delete - append to log
        select = "SELECT * FROM stations WHERE ID = '%s'" % self.ID
        self.getDataSample(select=select,limit=self.limit)
        print msg
        print 'This will disable authentication of a dosimeter until readded \
                separately with addDosimeterToDB.py. \n \
                If there was more than one return above, you should QUIT.'
        if self.confirm():
            sql = "DELETE FROM stations WHERE ID = '%s';" % self.ID
            self.cursor.execute(sql)
            if Parser.args.log:
                appendLog(sql,msg)

    def deleteDataBefore(self):
        before = Parser.args.before
        select = "SELECT * FROM dosnet WHERE ID = '%s' \
                    AND `receiveTime` < '%s' LIMIT '%s'" \
                    % (self.ID, before, self.limit)
        self.getDataSample(select=select,limit=self.limit)
        msg = 'DELETING ALL DATA BEFORE: ',before, 'for ID: ', self.ID
        print msg
        if self.confirm():
            sql = "DELETE FROM dosnet WHERE ID = '%s' \
                    AND `receiveTime` < '%s';" % (self.ID, before)
            self.cursor.execute(sql)
            if Parser.args.log:
                appendLog(sql,msg)

    def deleteDataAfter(self):
        after = Parser.args.after
        select = "SELECT * FROM dosnet WHERE ID = '%s' \
                            AND `receiveTime` > '%s' LIMIT '%s'" \
                            % (self.ID, after, self.limit)
        self.getDataSample(select=select,limit=self.limit)
        msg = 'DELETING ALL DATA AFTER: ',after, 'for ID: ', self.ID
        print msg
        if self.confirm():
            sql = "DELETE FROM dosnet WHERE ID = '%s' \
                    AND `receiveTime` > '%s';" % (self.ID, after)
            self.cursor.execute(sql)
            if Parser.args.log:
                appendLog(sql,msg)

    def deleteDataRange(self):
        before = Parser.args.before
        after = Parser.args.after
        select = "SELECT * FROM dosnet WHERE ID = '%s' \
                    AND (`receiveTime` BETWEEN '%s' AND '%s') LIMIT '%s'"
                    % (self.ID, before, after, self.limit)
        self.getDataSample(select=select,limit=self.limit)
        msg = 'DELETING ALL DATA BEFORE: ',before, 'for ID: ', self.ID
        print msg
        if self.confirm():
            sql = "DELETE FROM dosnet WHERE ID = '%s' \
                AND (`receiveTime` BETWEEN '%s' AND '%s');" % (self.ID, before, after)
            self.cursor.execute(sql)
            if Parser.args.log:
                appendLog(sql,msg)

    def getDataSample(self,select,limit):
        self.cursor.execute(select)
        rows = self.cursor.fetchall()
        print 'Sample of data you\'re about to delete (Max: "%s"): \n "%s"' % (limit, rows)

    def deleteAllData(self):
        msg = 'Truncating table dosnet - all data in the table will be gone.'
        print msg
        if self.confirm():
            print 'Enter the super secure secret password: \t $ '
            if raw_input() == self.secure_password:
                sql = "TRUNCATE TABLE dosnet;"
                self.cursor.execute(sql)
            else:
                print self.exit_serious_message
            if Parser.args.log:
                appendLog(sql,msg)

    def deleteAllStations(self):
        msg = 'Truncating table stations - all data in the table will be gone.'
        print msg
        if self.confirm():
            print 'Enter the super secure secret password: \t $ '
            if raw_input() == self.secure_password:
                sql = "TRUNCATE TABLE stations;"
                self.cursor.execute(sql)
            else:
                print self.exit_serious_message
            if Parser.args.log:
                appendLog(sql,msg)

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
        print '\nType "yes" without apostrophes to confirm the operation: '
        if raw_input() == 'yes':
            return True
        else:
            print 'Exiting with no changes: did not read "yes"'
            return False

if __name__ == "main":
    print 'Hi Joey!'
    print 'This script deletes data from the dosimeter network database on GRIM'
    print 'Are you sure you want to proceed??? (Type "yes" to proceed) \n$ '
    if raw_input() == 'yes':
        par = Parser()
        deleter = DataDestroyer()
    else:
        print 'You have decided not to delete data, thanks!'
        print 'If this was a mistake, type "yes" next time...'
