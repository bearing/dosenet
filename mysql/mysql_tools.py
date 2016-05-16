# -*- coding: utf-8 -*-
from __future__ import print_function
import MySQLdb as mdb
# import email_message
import pandas as pd
# import os
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
        try:
            self.close()
        except:
            pass

    def __exit__(self):
        try:
            self.close()
        except:
            pass

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
            # email_message.send_email(
            #     process=os.path.basename(__file__), error_message=msg)

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
            # email_message.send_email(
            #     process=os.path.basename(__file__), error_message=msg)

    def insertIntoDosenet(self, stationID, cpm, cpm_error, error_flag, **kwargs):
        self.cursor.execute(
            "INSERT INTO dosnet(stationID, cpm, cpmError, errorFlag) \
             VALUES ('{}','{}','{}','{}');".format(
                stationID, cpm, cpm_error, error_flag))
        # Time is decided by the MySQL database / DoseNet hence 'receiveTime' field in DB
        self.db.commit()

    def inject(self, data):
        auth = self.authenticatePacket(data)
        assert auth is None, auth
        self.insertIntoDosenet(**data)

    def getHashList(self):
        return self.verified_stations

    def authenticatePacket(self, data):
        '''
        Checks hash in hash list and compares against ID. Returns string if
        not authenticated. Otherwise returns None (success)
        '''
        if not isinstance(data, dict):
            return 'Inject data is not a dict: {}'.format(data)
        # Check data for keys
        data_types = {'hash': str, 'stationID': int, 'cpm': float,
                      'cpm_error': float, 'error_flag': int}
        for k in data_types:
            if k not in data:
                return 'No {} in data: {}'.format(k, data)
            if not isinstance(data[k], data_types[k]):
                return 'Incorrect type for {}: {} (should be {})'.format(
                    k, type(data[k]), data_types[k])
        hashes = self.getStations()['IDLatLongHash']
        # Check for this specific hash
        if data['hash'] != hashes[data['stationID']]:
            return 'Data hash ({}) does not match stationID () hash ()'.format(
                data['hash'], data['stationID'], hashes[data['stationID']])
        # Everything checks out
        return None

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
            "SELECT * \
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
        df = pd.read_sql(
            "SELECT * \
             FROM dosnet \
             INNER JOIN stations \
             ON dosnet.stationID = stations.ID \
             WHERE receiveTime = \
                (SELECT MAX(receiveTime) \
                FROM dosnet \
                WHERE stationID='{0}') \
                AND stationID='{0}';".format(stationID),
            con=self.db)
        try:
            df.set_index(df['Name'], inplace=True)
            assert len(df) == 1, 'More than one recent result returned for {}'.format(stationID)
            data = df.iloc[0]
            return data
        except (AssertionError) as e:
            print(e)
            return []

    def getInjectorStation(self):
        return self.getStations().loc[0, :]

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
        lastest_dt = self.getLatestStationData(stationID)['receiveTime']
        df = self.getDataForStationByRange(
            stationID,
            lastest_dt + relativedelta(days=-1),
            lastest_dt
        )
        return df

    def getLastWeek(self, stationID):
        lastest_dt = self.getLatestStationData(stationID)['receiveTime']
        df = self.getDataForStationByRange(
            stationID,
            lastest_dt + relativedelta(days=-7),
            lastest_dt
        )
        return df

    def getLastMonth(self, stationID):
        lastest_dt = self.getLatestStationData(stationID)['receiveTime']
        df = self.getDataForStationByRange(
            stationID,
            lastest_dt + relativedelta(months=-1),
            lastest_dt
        )
        return df

    def getLastYear(self, stationID):
        lastest_dt = self.getLatestStationData(stationID)['receiveTime']
        df = self.getDataForStationByRange(
            stationID,
            lastest_dt + relativedelta(months=-12),
            lastest_dt
        )
        return df
