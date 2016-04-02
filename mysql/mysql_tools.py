# -*- coding: utf-8 -*-
from __future__ import print_function
import MySQLdb as mdb
import email_message
import pandas as pd
import os
import sys
from dateutil.relativedelta import relativedelta

class SQLObject:
    def __init__(self):
        # NOTE should eventually update names (jccurtis)
        self.db = mdb.connect(
            '127.0.0.1',
            'ne170group',
            'ne170groupSpring2015',
            'dosimeter_network')
        self.verified_stations = []
        self.cursor = self.db.cursor()
        self.getVerifiedStationList()

    def __del__(self):
        self.close()

    def __exit__(self):
        self.close()

    def close(self):
        self.db.close()

    def getVerifiedStationList(self):
        try:
            self.cursor.execute("SELECT `ID`, `IDLatLongHash` FROM dosimeter_network.stations;")
            self.verified_stations = self.cursor.fetchall()
        except Exception as e:
            raise e
            msg = 'Error: Could not get list of stations from the database!'
            print(msg)
            email_message.send_email(
                process=os.path.basename(__file__), error_message=msg)

    def checkHashFromRAM(self, ID):
        # Essentially the same as doing the following in MySQL
        # "SELECT IDLatLongHash FROM stations WHERE `ID` = $$$ ;"
        try:
            for i in range(len(self.verified_stations)):
                if self.verified_stations[i][0] == ID:
                    dbHash = self.verified_stations[i][1]
                    return dbHash
        except Exception as e:
            raise e
            return False
            msg = 'Error: Could not find a station matching that ID'
            print(msg)
            email_message.send_email(
                process=os.path.basename(__file__), error_message=msg)

    def insertIntoDosenet(self, stationID, cpm, cpm_error, error_flag):
        self.cursor.execute(
            "INSERT INTO dosnet(stationID, cpm, cpmError, errorFlag) \
             VALUES ('%s','%s','%s','%s');".format(
                stationID, cpm, cpm_error, error_flag))
        # Time is decided by the MySQL database / DoseNet hence 'receiveTime' field in DB
        self.db.commit()

    def inject(self, data):
        if not self.authenticatePacket(data):
            print('~~ FAILED AUTHENTICATION ~~')
            print('Data: ', data)
            msg = str('FAILED AUTHENTICATION\n', data)
            email_message.send_email(
                process=os.path.basename(__file__), error_message=msg)
        else:
            data = self.parsePacket(data)
            if(data):
                if data[2] > 100:  # if cpm > 100
                    msg = 'CPM more than 100 - assumed to be noise event.\n NOT INJECTING.'
                    print(msg)
                    email_message.send_email(
                        process=os.path.basename(__file__), error_message=msg)
                else:
                    self.insertIntoDosenet(stationID=data[1],
                                           cpm=data[2],
                                           cpm_error=data[3],
                                           error_flag=data[4])
            else:
                msg = '~~ FAILED TO INJECT/PARSE ~~'
                print(msg)
                email_message.send_email(
                    process=os.path.basename(__file__), error_message=msg)

    def getHashList(self):
        return self.verified_stations

    def authenticatePacket(self, data):
        '''
        Checks hash in hash list and compares against ID.
        '''
        hash_list = self.getHashList()
        # Is it the correct hash length?
        msg_hash = data[:32]
        # Verify the hash is in the list
        if not any(str(msg_hash) in i for i in hash_list):
            print('Message Hash:', msg_hash)
            print('Hash list:', hash_list)
            print('Hash is not in list')
            return False
        # Ok, we think this could be a real station
        ID       = int(data.split(',')[1])
        db_hash  = self.checkHashFromRAM(ID)
        # Is it an authenticated station with a matching database entry?
        if db_hash == msg_hash:
            return True
        else:
            print('ID:', ID, 'Hash from database:', db_hash, 'Hash from message', msg_hash)
            return False  # Valid data, not authenticated station.

    def parsePacket(self, data):
        data = data.split(',')  # Commas are our delimiters for the decrypted message
        msg_hash = data[0]
        if len(msg_hash) == 32:  # All is good. Cast as known data types
            stationID   = int(data[1])
            cpm         = float(data[2])
            cpm_error   = float(data[3])
            error_flag  = int(data[4])
            return (msg_hash, stationID, cpm, cpm_error, error_flag)
        else:
            return False
            print('Data:', data)
            print('Message hash', msg_hash)
            print('Station ID: "{}", CPM: "{}", CPM error: "{}", Error flag: "{}"'.format(
                stationID, cpm, cpm_error, error_flag))

    def getHashFromDB(self, ID):
        # RUN "SELECT IDLatLongHash FROM stations WHERE `ID` = $$$ ;"
        try:
            self.cursor.execute("SELECT IDLatLongHash FROM stations \
                            WHERE `ID` = '%s';" % (ID))
            dbHash = self.cursor.fetchall()
            return dbHash
        except (KeyboardInterrupt, SystemExit):
            print('User interrupted for some reason, byeeeee')
            sys.exit(0)
        except (Exception) as e:
            print(e)
            print('Exception: Could not get hash from database. Is GRIM online and running MySQL?')

    def getStations(self):
        df = pd.read_sql(
            "SELECT ID, `Name`, Lat, `Long`, cpmtorem, cpmtousv, display, nickname \
            FROM dosimeter_network.stations;",
            con=self.db)
        df.set_index(df['ID'], inplace=True)
        del df['ID']
        return df

    def getActiveStations(self):
        df = self.getStations()
        df = df[df['display'] == 1]
        del df['display']
        return df

    def getLatestStationData(self, stationID):
        self.cursor.execute(
            "SELECT stationID, Name, receiveTime, cpm, cpmError \
            FROM dosnet \
            INNER JOIN stations \
            ON dosnet.stationID = stations.ID \
            WHERE receiveTime = \
            (SELECT MAX(receiveTime) \
            FROM dosnet \
            WHERE stationID='{0}') \
            AND stationID='{0}';".format(stationID)
        )
        result = self.cursor.fetchall()
        assert len(result) == 1, 'More than one recent result returned for {}'.format(stationID)
        result = result[0]
        data = {}
        data['id'] = result[0]
        data['name'] = result[1]
        data['time'] = result[2]
        data['cpm'] = result[3]
        data['cpm_err'] = result[4]
        return data

    def getDataForStationByRange(self, stationID, timemin, timemax):
        q = "SELECT receiveTime, cpm, cpmError \
        FROM dosnet \
        WHERE `dosnet`.`stationID`='{}' \
        AND receiveTime \
        BETWEEN '{}' \
        AND '{}';".format(stationID, timemin, timemax)
        df = pd.read_sql(q, con=self.db)
        return df

    def getLastDay(self, stationID):
        lastest_dt = self.getLatestStationData(stationID)['time']
        df = self.getDataForStationByRange(
            stationID,
            lastest_dt + relativedelta(days=-1),
            lastest_dt
        )
        return df

    def getLastWeek(self, stationID):
        lastest_dt = self.getLatestStationData(stationID)['time']
        df = self.getDataForStationByRange(
            stationID,
            lastest_dt + relativedelta(days=-7),
            lastest_dt
        )
        return df

    def getLastMonth(self, stationID):
        lastest_dt = self.getLatestStationData(stationID)['time']
        df = self.getDataForStationByRange(
            stationID,
            lastest_dt + relativedelta(months=-1),
            lastest_dt
        )
        return df

    def getLastYear(self, stationID):
        lastest_dt = self.getLatestStationData(stationID)['time']
        df = self.getDataForStationByRange(
            stationID,
            lastest_dt + relativedelta(months=-12),
            lastest_dt
        )
        return df
