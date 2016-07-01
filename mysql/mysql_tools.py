# -*- coding: utf-8 -*-
from __future__ import print_function
import MySQLdb as mdb
import pandas as pd
import sys
import datetime
import time
import pytz
from dateutil.relativedelta import relativedelta


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
            return pd.DataFrame({})

    def getInjectorStation(self):
        return self.getStations().loc[0, :]

    def getNextTestStation(self):
        test_station = self.getStations().loc[self.test_station_ids[self.test_station_ids_ix], :]
        # Cycle!
        if self.test_station_ids_ix == len(self.test_station_ids) - 1:
            self.test_station_ids_ix = 0
        else:
            self.test_station_ids_ix += 1
        return test_station

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
        try:
            lastest_dt = self.getLatestStationData(stationID)['receiveTime']
            df = self.getDataForStationByRange(
                stationID,
                lastest_dt + relativedelta(days=-1),
                lastest_dt
            )
            return df
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

    def getLastWeek(self, stationID):
        try:
            lastest_dt = self.getLatestStationData(stationID)['receiveTime']
            df = self.getDataForStationByRange(
                stationID,
                lastest_dt + relativedelta(days=-7),
                lastest_dt
            )
            return df
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

    def getLastMonth(self, stationID):
        try:
            lastest_dt = self.getLatestStationData(stationID)['receiveTime']
            df = self.getDataForStationByRange(
                stationID,
                lastest_dt + relativedelta(months=-1),
                lastest_dt
            )
            return df
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

    def getLastYear(self, stationID):
        try:
            lastest_dt = self.getLatestStationData(stationID)['receiveTime']
            df = self.getDataForStationByRange(
                stationID,
                lastest_dt + relativedelta(months=-12),
                lastest_dt
            )
            return df
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})
