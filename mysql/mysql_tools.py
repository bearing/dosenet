#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import MySQLdb as mdb
import pandas as pd
import sys
import datetime
import time
import pytz


def datetime_tz(year, month, day, hour=0, minute=0, second=0, tz='UTC'):
    dt_naive = datetime.datetime(year, month, day, hour, minute, second)
    tzinfo = pytz.timezone(tz)
    return tzinfo.localize(dt_naive)


def epoch_to_datetime(epoch, tz='UTC'):
    """Return datetime with associated timezone."""
    dt_utc = datetime_tz(1970, 1, 1, tz='UTC') + datetime.timedelta(seconds=epoch)
    tzinfo = pytz.timezone(tz)
    return dt_utc.astimezone(tzinfo)


class SQLObject:
    def __init__(self, tz='+00:00'):
        # NOTE should eventually update names (jccurtis)
        self.db = mdb.connect(
            '127.0.0.1',
            'ne170group',
            'ne170groupSpring2015',
            'dosimeter_network')
        self.verified_stations = []
        self.cursor = self.db.cursor()
        self.set_session_tz(tz)
        self.getVerifiedStationList()
        self.test_station_ids = [0, 10001, 10002, 10003, 10004, 10005]
        self.test_station_ids_ix = 0

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

    def set_session_tz(self, tz):
        """Sets timezone for this MySQL session.
        This affects the timestamp strings shown by deviceTime and receiveTime.
        Does NOT affect UNIX_TIMESTAMP(deviceTime), UNIX_TIMESTAMP(receiveTime)

        Might not be needed. Depends what you're doing with the data.
        """
        print('[CONFIG] Setting session timezone to: {}'.format(tz))
        self.cursor.execute("SET time_zone='{}';".format(tz))

    def close(self):
        self.db.close()

    def getVerifiedStationList(self):
        """
        this gets run, but the result doesn't seem to be used except in
        unused functions
        """
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
        "unused"
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

    def insertIntoDosenet(self, stationID, cpm, cpm_error, error_flag,
                          deviceTime=None, **kwargs):
        """
        Insert a row of dosimeter data into the dosnet table

        NOTE that the receiveTime is not included since that is assigned my
        the MySQL default value of CURRENT_TIMESTAMP
        """
        if (not isinstance(deviceTime, int) and
                not isinstance(deviceTime, float)):
            if deviceTime is not None:
                print('Warning: received non-numeric deviceTime! Ignoring')
            deviceTime = time.time()
        self.cursor.execute(
            "INSERT INTO dosnet(deviceTime, stationID, cpm, cpmError, errorFlag) \
             VALUES (FROM_UNIXTIME({:.3f}), {}, {}, {}, {});".format(
                deviceTime, stationID, cpm, cpm_error, error_flag))
        self.db.commit()

    def insertIntoLog(self, stationID, msgCode, message):
        """
        Insert a log message into the stationlog table.
        """
        self.cursor.execute(
            "INSERT INTO stationlog(stationID, msgCode, message) " +
            "VALUES ({}, {}, {})".format(stationID, msgCode, message))

    def inject(self, data):
        """Authenticate the data packet and then insert into database"""
        auth = self.authenticatePacket(data)
        assert auth is None, auth
        self.insertIntoDosenet(**data)

    def injectLog(self, data):
        """Authenticate the log packet and then insert into database"""
        auth = self.authenticatePacket(data)
        assert auth is None, auth
        self.insertIntoLog(**data)

    def getHashList(self):
        "unused"
        return self.verified_stations

    def authenticatePacket(self, data):
        '''
        Checks hash in hash list and compares against ID. Returns string if
        not authenticated. Otherwise returns None (success)
        '''
        if not isinstance(data, dict):
            return 'Inject data is not a dict: {}'.format(data)
        if 'stationID' not in data:
            return 'No stationID in data'
        if 'hash' not in data:
            return 'No hash in data'
        hashes = self.getStations()['IDLatLongHash']
        # Check for this specific hash
        if data['hash'] != hashes[data['stationID']]:
            return 'Data hash ({}) does not match stationID ({}) hash ({})'.format(
                data['hash'], data['stationID'], hashes[data['stationID']])
        # Everything checks out
        return None

    def getHashFromDB(self, ID):
        "unused"
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
        """Read the stations table from MySQL into a pandas dataframe."""
        df = pd.read_sql(
            "SELECT * \
            FROM dosimeter_network.stations;",
            con=self.db)
        df.set_index(df['ID'], inplace=True)
        del df['ID']
        return df

    def getActiveStations(self):
        """Read the stations table, but only entries with display==1."""
        df = self.getStations()
        df = df[df['display'] == 1]
        del df['display']
        return df

    def getSingleStation(self, stationID):
        """Read one entry of the stations table into a pandas dataframe."""
        df = pd.read_sql(
            "SELECT * \
            FROM dosimeter_network.stations \
            WHERE `ID` = {};".format(stationID),
            con=self.db)
        df.set_index(df['ID'], inplace=True)
        del df['ID']
        return df

    def getLatestStationData(self, stationID):
        df = pd.read_sql(
            "SELECT UNIX_TIMESTAMP(deviceTime), UNIX_TIMESTAMP(receiveTime), \
             stationID, cpm, cpmError, errorFlag, ID, Name, Lat, `Long`, \
             cpmtorem, cpmtousv, display, nickname, timezone \
             FROM dosnet \
             INNER JOIN stations \
             ON dosnet.stationID = stations.ID \
             WHERE deviceTime = \
                (SELECT MAX(deviceTime) \
                 FROM dosnet \
                 WHERE stationID='{0}') \
             AND stationID='{0}';".format(stationID),
            con=self.db)
        df.set_index(df['Name'], inplace=True)
        # Add timezone columns
        df = self.addTimeColumnsToDataframe(df, stationID=stationID)
        if len(df) == 0:
            print('[SQL WARNING] no data returned for stationID={}'.format(stationID))
            return pd.DataFrame({})
        elif len(df) > 1:
            print('[SQL WARNING] more than one recent result for stationID={}'.format(stationID))
            print(df)
            return df.iloc[0]
        else:
            return df.iloc[0]

    def getInjectorStation(self):
        "The injector est station is station 0."
        return self.getStations().loc[0, :]

    def getNextTestStation(self):
        test_station = self.getStations().loc[self.test_station_ids[self.test_station_ids_ix], :]
        # Cycle!
        if self.test_station_ids_ix == len(self.test_station_ids) - 1:
            self.test_station_ids_ix = 0
        else:
            self.test_station_ids_ix += 1
        return test_station

    def getTimezoneFromID(self, stationID):
        self.cursor.execute(
            "SELECT timezone FROM stations \
            WHERE `ID` = {};".format(stationID))
        tz = self.cursor.fetchall()
        return tz[0][0]

    def getDataForStationByRange(self, stationID, timemin, timemax):
        q = "SELECT UNIX_TIMESTAMP(deviceTime), UNIX_TIMESTAMP(receiveTime), cpm, cpmError \
        FROM dosnet \
        WHERE `dosnet`.`stationID`='{}' \
        AND receiveTime \
        BETWEEN '{}' \
        AND '{}';".format(stationID, timemin, timemax)
        df = pd.read_sql(q, con=self.db)
        return self.addTimeColumnsToDataframe(df)

    def getDataForStationByInterval(self, stationID, intervalStr):
        # Make the query for this station on this interval
        try:
            q = "SELECT UNIX_TIMESTAMP(deviceTime), UNIX_TIMESTAMP(receiveTime), \
            cpm, cpmError\
            FROM dosnet \
            WHERE stationID={} \
            AND deviceTime >= (NOW() - {}) \
            ORDER BY deviceTime DESC;".format(stationID, intervalStr)
            df = pd.read_sql(q, con=self.db)
            return self.addTimeColumnsToDataframe(df, stationID=stationID)
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

    def addTimeColumnsToDataframe(self, df, stationID=None, tz=None):
        """
        Input dataframe from query with UNIX_TIMESTAMP for deviceTime and
        receiveTime. The output has 6 time columns:
            deviceTime_unix (renamed from UNIX_TIMESTAMP(deviceTime))
            deviceTime_utc
            deviceTime_local
            receiveTime_unix (renamed from UNIX_TIMESTAMP(receiveTime))
            receiveTime_utc
            receiveTime_local

        http://stackoverflow.com/questions/17159207/change-timezone-of-date-time-column-in-pandas-and-add-as-hierarchical-index
        """
        # Select timezone to use
        if isinstance(tz, str):
            # If timezone given as string use it
            this_tz = tz
        elif isinstance(stationID, (int, str)):
            # If tz not provided and stationID given as int/str
            this_tz = self.getTimezoneFromID(stationID)
        else:
            # Default
            print('[TZ WARNING] Defaulting to `US/Pacific`')
            this_tz = 'US/Pacific'
        # Sanity check
        assert isinstance(this_tz, str), '[TZ ERROR] Not a tz str: {}'.format(this_tz)
        # Rename exist unix epoch seconds columns
        df.rename(inplace=True, columns={
            'UNIX_TIMESTAMP(deviceTime)': 'deviceTime_unix',
            'UNIX_TIMESTAMP(receiveTime)': 'receiveTime_unix'})
        # Timezones are evil but pandas are fuzzy ...
        deviceTime = pd.Index(pd.to_datetime(df['deviceTime_unix'], unit='s')).tz_localize('UTC')
        receiveTime = pd.Index(pd.to_datetime(df['receiveTime_unix'], unit='s')).tz_localize('UTC')
        df['deviceTime_utc'] = deviceTime
        df['deviceTime_local'] = deviceTime.tz_convert(this_tz)
        df['receiveTime_utc'] = receiveTime
        df['receiveTime_local'] = receiveTime.tz_convert(this_tz)
        # Rearrange the columns (iterate in opposite order of placement)
        new_cols = df.columns.tolist()
        for colname in ['receiveTime_unix', 'receiveTime_local',
                        'receiveTime_utc', 'deviceTime_unix',
                        'deviceTime_local', 'deviceTime_utc']:
            new_cols.insert(0, new_cols.pop(new_cols.index(colname)))
        df = df[new_cols]
        return df

    def getLastDay(self, stationID):
        return self.getDataForStationByInterval(stationID, 'INTERVAL 1 DAY')

    def getLastWeek(self, stationID):
        return self.getDataForStationByInterval(stationID, 'INTERVAL 1 WEEK')

    def getLastMonth(self, stationID):
        return self.getDataForStationByInterval(stationID, 'INTERVAL 1 MONTH')

    def getLastYear(self, stationID):
        return self.getDataForStationByInterval(stationID, 'INTERVAL 1 YEAR')

    def testLastMethods(self, stationID=1):
        print('Testing last data methods with stationID={}\n'.format(stationID))
        for method in [self.getLastDay, self.getLastWeek, self.getLastMonth,
                       self.getLastYear]:
            print('Testing: {}'.format(method))
            df = method(stationID)
            print('Num Entries:', len(df))
            print('HEAD:\n{}'.format(df.head()))
            print()


if __name__ == "__main__":
    sql = SQLObject()
    pd.set_option('display.expand_frame_repr', False)
    sql.testLastMethods(6)
