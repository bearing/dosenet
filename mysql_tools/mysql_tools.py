#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import MySQLdb as mdb
import numpy as np
import pandas as pd
import sys
import datetime
import time
import pytz
import os
import errno
import signal
from mysql.connector import MySQLConnection, OperationalError, Error

class TimeoutError(Exception):
    pass

class timeout:
    def __init__(self, seconds=10, error_message=os.strerror(errno.ETIME)):
        self.seconds = seconds
        self.error_message = error_message
    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)
    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)
    def __exit__(self, type, value, traceback):
        signal.alarm(0)

USER = 'root'  # set while creating the instance
HOST = 'dosenet-0.cork9lvwvd2g.us-west-1.rds.amazonaws.com'  # obtained AFTER creating the instance
PORT = 3306  # default value (can be changed while creating the instance)
PASSWORD = 'radiationisrad'  # set while creating the instance
DATABASE = 'dosenet'   # set while creating the instance

def connection_to_remote_db():
    """Returns a connection to the remote database."""
    return MySQLConnection(user=USER, host=HOST, port=PORT,
                           password=PASSWORD, database=DATABASE)

def datetime_tz(year, month, day, hour=0, minute=0, second=0, tz='UTC'):
    dt_naive = datetime.datetime(year, month, day, hour, minute, second)
    tzinfo = pytz.timezone(tz)
    return tzinfo.localize(dt_naive)


def epoch_to_datetime(epoch, tz='UTC'):
    """Return datetime with associated timezone."""
    dt_utc = (datetime_tz(1970, 1, 1, tz='UTC') +
              datetime.timedelta(seconds=epoch))
    tzinfo = pytz.timezone(tz)
    return dt_utc.astimezone(tzinfo)


class SQLObject:
    def __init__(self, tz='+00:00'):
        # NOTE should eventually update names (jccurtis)
        #self.db = mdb.connect(
        #    '127.0.0.1',
        #    'ne170group',
        #    'ne170groupSpring2015',
        #    'dosimeter_network')
        self.db = connection_to_remote_db()
        self.cursor = self.db.cursor(buffered=True)
        self.set_session_tz(tz)
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

    def set_max_query_time(self,time=600000):
        self.cursor.execute("SET SESSION MAX_EXECUTION_TIME=600000")

    def set_session_tz(self, tz):
        """Sets timezone for this MySQL session.
        This affects the timestamp strings shown by deviceTime and receiveTime.
        Does NOT affect UNIX_TIMESTAMP(deviceTime), UNIX_TIMESTAMP(receiveTime)

        Might not be needed. Depends what you're doing with the data.
        """
        print('[CONFIG] Setting session timezone to: {}'.format(tz))
        self.cursor.execute("SET time_zone='{}';".format(tz))
        self.refresh()

    def close(self):
        if(connection.is_connected()):
            self.cursor.close()
            self.db.close()
            print("MySQL connection is closed")

    def refresh(self):
        """Clear the cache of any query results."""
        self.db.commit()

# ---------------------------------------------------------------------------
#       INJECTION-RELATED METHODS
# ---------------------------------------------------------------------------
    def getVerifiedStationList(self):
        """
        this gets run, but the result doesn't seem to be used except in
        unused functions
        """
        try:
            sql_cmd = ("SELECT `ID`, `IDLatLongHash` FROM " +
                       "stations;")
            self.cursor.execute(sql_cmd)
            self.verified_stations = self.cursor.fetchall()
        except Exception as e:
            msg = 'Error: Could not get list of stations from the database!'
            print(msg)
            raise e
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

    def safe_insert(self, sql_cmd):
        attempts = 0
        while attempts < 10:
            try:
                with timeout(100):
                    self.cursor.execute(sql_cmd)
                    self.db.commit()
                    break
            except OperationalError as err:
                print('Could not find SQL database! Reestablishing connection')
                self.close()
                self.db = connection_to_remote_db()
                self.cursor = self.db.cursor(buffered=True)
                #self.cursor.execute("SET SESSION MAX_EXECUTION_TIME=600000")
                attempts = attempts + 1
                pass
            except Error as err:
                print(err)
                print("Error Code:", err.errno)
                print("SQLSTATE", err.sqlstate)
                print("Message", err.msg)
                attempts = attempts + 1
                sleep(1)
                pass
            except Exception:
                print("Error inserting {}}".format(sql_cmd))
                print(e)
                attempts = attempts + 1
                sleep(1)
                pass
        if attempts==10:
            return False
        else:
            return True

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
        sql_cmd = (
            "INSERT INTO " +
            "dosnet(deviceTime, stationID, cpm, cpmError, errorFlag) " +
            "VALUES (FROM_UNIXTIME({:.3f}), {}, {}, {}, {});".format(
                deviceTime, stationID, cpm, cpm_error, error_flag))
        error_code = self.safe_insert(sql_cmd)
        #if not error_code:
            #TODO: Decide what to do when the insert fails...

    def insertIntoAQ(self, stationID, oneMicron, twoPointFiveMicron, tenMicron,
                     error_flag, deviceTime, **kwargs):
        """
        Insert a row of Air Quality data into the Air Quality table
        """
        if (not isinstance(deviceTime, int) and
                not isinstance(deviceTime, float)):
            if deviceTime is not None:
                print('Warning: received non-numeric deviceTime! Ignoring')
            deviceTime = time.time()
        sql_cmd = (
            "INSERT INTO " +
            "air_quality(deviceTime, stationID, PM1, PM25, PM10, errorFlag) " +
            "VALUES (FROM_UNIXTIME({:.3f}), {}, {}, {}, {}, {});".format(
                deviceTime, stationID, oneMicron, twoPointFiveMicron, tenMicron, error_flag))
        error_code = self.safe_insert(sql_cmd)
        #if not error_code:
            #TODO: Decide what to do when the insert fails...

    def insertIntoCO2(self, stationID, co2_ppm, noise, error_flag, deviceTime, **kwargs):
        """
        Insert a row of CO2 data into the CO2 table
        """
        if (not isinstance(deviceTime, int) and
                not isinstance(deviceTime, float)):
            if deviceTime is not None:
                print('Warning: received non-numeric deviceTime! Ignoring')
            deviceTime = time.time()
        sql_cmd = (
            "INSERT INTO " +
            "adc(deviceTime, stationID, co2_ppm, noise, errorFlag) " +
            "VALUES (FROM_UNIXTIME({:.3f}), {}, {}, {}, {});".format(
                deviceTime, stationID, co2_ppm, noise, error_flag))
        error_code = self.safe_insert(sql_cmd)
        #if not error_code:
            #TODO: Decide what to do when the insert fails...

    def insertIntoWeather(self, stationID, temperature, pressure,
                          humidity, error_flag, deviceTime, **kwargs):
        """
        Insert a row of Weather data into the Weather table
        """
        if (not isinstance(deviceTime, int) and
                not isinstance(deviceTime, float)):
            if deviceTime is not None:
                print('Warning: received non-numeric deviceTime! Ignoring')
            deviceTime = time.time()
        sql_cmd = (
            "INSERT INTO " +
            "weather(deviceTime, stationID, temperature, pressure, humidity, errorFlag) " +
            "VALUES (FROM_UNIXTIME({:.3f}), {}, {}, {}, {}, {});".format(
                deviceTime, stationID, temperature, pressure, humidity, error_flag))
        error_code = self.safe_insert(sql_cmd)
        #if not error_code:
            #TODO: Decide what to do when the insert fails...

    def insertIntoD3S(self, stationID, spectrum, error_flag, deviceTime,
                      **kwargs):
        """
        Insert a row of D3S data into the d3s table.
        """
        counts = sum(spectrum)
        spectrum = np.array(spectrum, dtype=np.uint8)
        spectrum_blob = spectrum.tobytes()
        sql_cmd = (
            "INSERT INTO " +
            "d3s(deviceTime, stationID, counts, channelCounts, errorFlag) " +
            "VALUES (FROM_UNIXTIME({:.3f}), {}, {}, {}, {});".format(
                deviceTime, stationID, counts, '%s', error_flag))
        # let MySQLdb library handle the special characters in the blob
        error_code = self.safe_insert(sql_cmd)
        #if not error_code:
            #TODO: Decide what to do when the insert fails...

    def insertIntoLog(self, stationID, msgCode, msgText, **kwargs):
        """
        Insert a log message into the stationlog table.
        """
        sql_cmd = ("INSERT INTO stationlog(stationID, msgCode, message) " +
                   "VALUES ({}, {}, '{}')".format(stationID, msgCode, msgText))
        error_code = self.safe_insert(sql_cmd)
        #if not error_code:
            #TODO: Decide what to do when the insert fails...

    def inject(self, data, verbose=False):
        """Authenticate the data packet and then insert into database"""
        tic = time.time()
        self.authenticatePacket(data, packettype='data')
        toc = time.time()
        if verbose:
            print('authenticatePacket took {} ms'.format((toc - tic) * 1000))
        self.insertIntoDosenet(**data)
        tic = time.time()
        if verbose:
            print('insertIntoDosenet took {} ms'.format((tic - toc) * 1000))

    def injectD3S(self, data, verbose=False):
        """Authenticate the D3S data packet and then insert into database"""
        tic = time.time()
        self.authenticatePacket(data, packettype='d3s')
        toc = time.time()
        if verbose:
            print('authenticatePacket took {} ms'.format((toc - tic) * 1000))
        self.insertIntoD3S(**data)
        tic = time.time()
        if verbose:
            print('insertIntoDosenet took {} ms'.format((tic - toc) * 1000))

    def injectAQ(self, data, verbose=False):
        """Authenticate the AQ data packet and then insert into database"""
        tic = time.time()
        self.authenticatePacket(data, packettype='AQ')
        toc = time.time()
        if verbose:
            print('authenticatePacket took {} ms'.format((toc - tic) * 1000))
        self.insertIntoAQ(**data)
        tic = time.time()
        if verbose:
            print('insertIntoDosenet took {} ms'.format((tic - toc) * 1000))

    def injectCO2(self, data, verbose=False):
        """Authenticate the CO2 data packet and then insert into database"""
        tic = time.time()
        self.authenticatePacket(data, packettype='CO2')
        toc = time.time()
        if verbose:
            print('authenticatePacket took {} ms'.format((toc - tic) * 1000))
        self.insertIntoCO2(**data)
        tic = time.time()
        if verbose:
            print('insertIntoDosenet took {} ms'.format((tic - toc) * 1000))

    def injectWeather(self, data, verbose=False):
        """Authenticate the Weather data packet and then insert into database"""
        tic = time.time()
        self.authenticatePacket(data, packettype='Weather')
        toc = time.time()
        if verbose:
            print('authenticatePacket took {} ms'.format((toc - tic) * 1000))
        self.insertIntoWeather(**data)
        tic = time.time()
        if verbose:
            print('insertIntoDosenet took {} ms'.format((tic - toc) * 1000))

    def injectLog(self, data, verbose=False):
        """Authenticate the log packet and then insert into database"""
        tic = time.time()
        self.authenticatePacket(data, packettype='log')
        toc = time.time()
        if verbose:
            print('authenticatePacket took {} ms'.format((toc - tic) * 1000))
        self.insertIntoLog(**data)
        tic = time.time()
        if verbose:
            print('insertIntoDosenet took {} ms'.format((tic - toc) * 1000))

    def authenticatePacket(self, data, packettype='data'):
        '''
        Checks keys in data.
        Checks hash in hash list and compares against ID.
        Raises error if anything doesn't match.

        packettype can be either "data" (a normal data packet)
        or "log" (a log entry from the device)
        or "d3s" (D3S data).
        '''
        if not isinstance(data, dict):
            raise TypeError('Inject data is not a dict: {}'.format(data))

        # Check data for keys
        if packettype == 'data':
            data_types = {'hash': str, 'stationID': int, 'cpm': float,
                          'cpm_error': float, 'error_flag': int}
        elif packettype == 'd3s':
            data_types = {'hash': str, 'stationID': int, 'spectrum': list,
                          'error_flag': int}
            if len(data['spectrum']) != 4096:
                raise AuthenticationError(
                    'Spectrum length is {}, should be 4096'.format(
                        len(data['spectrum'])))
        elif packettype == 'AQ':
            data_types = {'hash': str, 'stationID': int, 'oneMicron': float, 'twoPointFiveMicron':
                          float, 'tenMicron': float, 'error_flag': int}
        elif packettype == 'CO2':
            data_types = {'hash': str, 'stationID': int, 'co2_ppm': float, 'noise':
                          float, 'error_flag': int}
        elif packettype == 'Weather':
            data_types = {'hash': str, 'stationID': int, 'temperature': float, 'pressure':
                          float, 'humidity': float, 'error_flag': int}
        elif packettype == 'log':
            data_types = {'hash': str, 'stationID': int, 'msgCode': int,
                          'msgText': str}
        else:
            raise ValueError(
                'Unknown packet type {}: should be "data" or "log"'.format(
                    packettype))
        for k in data_types:
            if k not in data:
                raise ValueError('No {} in data: {}'.format(k, data))
            if not isinstance(data[k], data_types[k]):
                raise TypeError(
                    'Incorrect type for {}: {} (should be {})'.format(
                        k, type(data[k]), data_types[k]))

        this_hash = self.getStations()['IDLatLongHash'][data['stationID']]
        # Check for this specific hash
        if data['hash'] != this_hash:
            raise AuthenticationError(
                "Hash mismatch on ID {}: data packet {}; database {}".format(
                    data['stationID'], data['hash'], this_hash))

        # Everything checks out
        return None

    def getStationReturnInfo(self, stationID):
        """Read gitBranch and needsUpdate from stations table."""
        self.refresh()
        col_list = "gitBranch, needsUpdate"
        q = "SELECT {} FROM stations WHERE `ID` = {};".format(
            col_list, stationID)
        df = self.safe_query(q)

        if len(df) > 0:
            needs_update = df['needsUpdate'][0]
            git_branch = df['gitBranch'][0]
        else:
            git_branch = 'master'
            needs_update = 0

        return git_branch, needs_update

# ---------------------------------------------------------------------------
#       STATION-UPDATE-RELATED METHODS
# ---------------------------------------------------------------------------

    def sendSingleStationChange(self, stationID, column, value):
        q = "UPDATE stations SET `{}` = {} WHERE `ID`={}".format(
                column,value,stationID)
        self.cursor.execute(q)
        self.refresh()

    def setSingleStationUpdate(self, stationID, needs_update=0):
        """
        Set needsUpdate = {} for a single station in stations table. Default: 0

        Do this after you tell the device to update and reboot, because after
        that it doesn't need the update.

        (You could also use this to set needsUpdate = 1 for a single station.)
        """

        if (not isinstance(needs_update, int) and
                not isinstance(needs_update, bool)):
            raise AssertionError('needs_update should be a bool or int (0, 1)')

        needs_update = int(needs_update)    # db expects 0 or 1
        q = "UPDATE stations SET needsUpdate={} WHERE `ID`={}".format(
            needs_update, stationID)
        self.cursor.execute(q)
        self.refresh()

    def setAllStationsUpdate(self, needs_update=1):
        """
        Set needsUpdate = {} for all stations in stations table. Default: 1

        Do this if there is a bug in the code such that all stations need to
        update. Of course you have to fix the bug first ;-)

        (You could also use this to set needsUpdate = 0 for all stations.)
        """

        if (not isinstance(needs_update, int) and
                not isinstance(needs_update, bool)):
            raise AssertionError('needs_update should be a bool or int (0, 1)')

        needs_update = int(needs_update)    # db expects 0 or 1
        q = "UPDATE stations SET needsUpdate={}".format(needs_update)
        self.cursor.execute(q)
        self.refresh()

# ---------------------------------------------------------------------------
#       FETCH METHODS
# ---------------------------------------------------------------------------

    def safe_query(self, q, timeout_time = 300):
        attempts = 0
        while attempts < 5:
            try:
                with timeout(timeout_time):
                    df = self.dfFromSql(q)
                    break
            except OperationalError as err:
                print('Could not find SQL database! Reestablishing connection')
                self.close()
                self.db = connection_to_remote_db()
                self.cursor = self.db.cursor(buffered=True)
                #self.cursor.execute("SET SESSION MAX_EXECUTION_TIME=600000")
                attempts = attempts + 1
                pass
            except Error as err:
                print(err)
                print("Error Code:", err.errno)
                print("SQLSTATE", err.sqlstate)
                print("Message", err.msg)
                attempts = attempts + 1
                sleep(1)
                pass
            except Exception as e:
                print("Error inserting {}".format(q))
                print(e)
                attempts = attempts + 1
                sleep(1)
                pass
        if attempts==10:
            return pd.DataFrame({})
        else:
            return df

    def dfFromSql(self, q):
        """Pandas dataframe from SQL query"""
        df = pd.read_sql(q, con=self.db)
        return df

    def rawSql(self, q):
        """Raw result of SQL query"""
        self.cursor.execute(q)
        out = self.cursor.fetchall()
        return out

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
            print('Exception: Could not get hash from database. ' +
                  'Is the DoseNet server online and running MySQL?')

    def getStations(self):
        """Read the stations table from MySQL into a pandas dataframe."""
        q = "SELECT * FROM stations;"
        df = self.safe_query(q)
        if len(df) > 0:
            df.set_index(df['ID'], inplace=True)
            del df['ID']
        return df

    def getActiveStations(self):
        """Read the stations table, but only entries with display==1."""
        df = self.getStations()
        if len(df) > 0:
            df = df[df['display'] == 1]
            del df['display']
        return df

    def getSensorStations(self, ID):
        """Read the stations table, but only entries with display=={ID}."""
        df = self.getActiveStations()
        if len(df) > 0:
            active_list = [x[ID]=="1" for x in df['devices'].tolist()]
            df = df[pd.Series([x[ID]=="1" for x in df['devices'].tolist()],
                            index=df['devices'].index)]
        return df

    def getActiveD3SStations(self):
        """Read the stations table, but only entries with display==1."""
        return self.getSensorStations(1)

    def getActiveAQStations(self):
        """Read the stations table, but only entries with display==2."""
        return self.getSensorStations(2)

    def getActiveWeatherStations(self):
        """Read the stations table, but only entries with display==3."""
        return self.getSensorStations(3)

    def getActiveADCStations(self):
        """Read the stations table, but only entries with display==1."""
        return self.getSensorStations(4)

    def getSingleStation(self, stationID):
        """Read one entry of the stations table into a pandas dataframe."""
        q = "SELECT * FROM stations WHERE `ID` = {};".format(
                stationID)
        df = self.dfFromSql(q)
        df.set_index(df['ID'], inplace=True)
        del df['ID']
        return df

    def getLatestStationData(self, stationID, verbose=False):
        """Return most recent data entry for given station."""
        col_list = ', '.join((
            "UNIX_TIMESTAMP(deviceTime)",
            "UNIX_TIMESTAMP(receiveTime)",
            "stationID",
            "cpm",
            "cpmError",
            "errorFlag",
            "ID",
            "Name",
            "Lat",
            "`Long`",
            "cpmtorem",
            "cpmtousv",
            "display",
            "nickname",
            "timezone"
        ))
        q = ' '.join((
            "SELECT {cols} FROM dosnet".format(cols=col_list),
            "INNER JOIN stations ON dosnet.stationID = stations.ID",
            "WHERE deviceTime = ",
            "(SELECT MAX(deviceTime) FROM dosnet WHERE stationID='{}')".format(
                stationID),
            "AND stationID='{}';".format(stationID)))
        df = self.safe_query(q)

        if len(df) == 0:
            if verbose:
                print('[SQL WARNING] no data returned for stationID={}'.format(
                        stationID))
            return pd.DataFrame({})
        elif len(df) > 1:
            if verbose:
                print('[SQL WARNING] more than one recent result for ' +
                      'stationID={}'.format(stationID))
                print(df)
            return df.iloc[0]
        else:
            df.set_index(df['Name'], inplace=True)
            df = self.addTimeColumnsToDataframe(df, stationID=stationID)
            return df.iloc[0]

    def getLatestD3SStationData(self, stationID, verbose=False):
        col_list = ', '.join((
            "UNIX_TIMESTAMP(deviceTime)",
            "UNIX_TIMESTAMP(receiveTime)",
            "stationID",
            "counts",
            "errorFlag",
            "ID",
            "Name",
            "Lat",
            "`Long`",
            "cpmtorem",
            "display",
            "nickname",
            "timezone"
        ))
        q = ' '.join((
            "SELECT {cols} FROM d3s".format(cols=col_list),
            "INNER JOIN stations ON d3s.stationID = stations.ID",
            "WHERE deviceTime = ",
            "(SELECT MAX(deviceTime) FROM d3s WHERE stationID='{}')".format(
                stationID),
            "AND stationID='{}';".format(stationID)))
        df = self.safe_query(q,500)

        if len(df) == 0:
            if verbose:
                print('[SQL WARNING] no data returned for stationID={}'.format(
                        stationID))
            return pd.DataFrame({})
        elif len(df) > 1:
            if verbose:
                print('[SQL WARNING] more than one recent result for ' +
                      'stationID={}'.format(stationID))
                print(df)
            return df.iloc[0]
        else:
            df.set_index(df['Name'], inplace=True)
            df = self.addTimeColumnsToDataframe(df, stationID=stationID)
            return df.iloc[0]

    def getLatestADCStationData(self, stationID, verbose=False):
        col_list = ', '.join((
            "UNIX_TIMESTAMP(deviceTime)",
            "UNIX_TIMESTAMP(receiveTime)",
            "stationID",
            "co2_ppm",
            "noise",
            "ID",
            "Name",
            "Lat",
            "`Long`",
            "display",
            "nickname",
            "timezone"
        ))
        q = ' '.join((
            "SELECT {cols} FROM adc".format(cols=col_list),
            "INNER JOIN stations ON adc.stationID = stations.ID",
            "WHERE deviceTime = ",
            "(SELECT MAX(deviceTime) FROM adc WHERE stationID='{}')".format(
                stationID),
            "AND stationID='{}';".format(stationID)))
        df = self.safe_query(q,400)

        if len(df) == 0:
            if verbose:
                print('[SQL WARNING] no data returned for stationID={}'.format(
                        stationID))
            return pd.DataFrame({})
        elif len(df) > 1:
            if verbose:
                print('[SQL WARNING] more than one recent result for ' +
                      'stationID={}'.format(stationID))
                print(df)
            return df.iloc[0]
        else:
            df.set_index(df['Name'], inplace=True)
            df = self.addTimeColumnsToDataframe(df, stationID=stationID)
            return df.iloc[0]

    def getLatestAQStationData(self, stationID, verbose=False):
        col_list = ', '.join((
            "UNIX_TIMESTAMP(deviceTime)",
            "UNIX_TIMESTAMP(receiveTime)",
            "stationID",
            "PM25",
            "errorFlag",
            "ID",
            "Name",
            "Lat",
            "`Long`",
            "display",
            "nickname",
            "timezone"
        ))
        q = ' '.join((
            "SELECT {cols} FROM air_quality".format(cols=col_list),
            "INNER JOIN stations ON air_quality.stationID = stations.ID",
            "WHERE deviceTime = ",
            "(SELECT MAX(deviceTime) FROM air_quality WHERE stationID='{}')".format(
                stationID),
            "AND stationID='{}';".format(stationID)))
        df = self.safe_query(q,400)

        if len(df) == 0:
            if verbose:
                print('[SQL WARNING] no data returned for stationID={}'.format(
                        stationID))
            return pd.DataFrame({})
        elif len(df) > 1:
            if verbose:
                print('[SQL WARNING] more than one recent result for ' +
                      'stationID={}'.format(stationID))
                print(df)
            return df.iloc[0]
        else:
            df.set_index(df['Name'], inplace=True)
            df = self.addTimeColumnsToDataframe(df, stationID=stationID)
            return df.iloc[0]

    def getLatestWeatherStationData(self, stationID, verbose=False):
        col_list = ', '.join((
            "UNIX_TIMESTAMP(deviceTime)",
            "UNIX_TIMESTAMP(receiveTime)",
            "stationID",
            "temperature",
            "pressure",
            "humidity",
            "errorFlag",
            "ID",
            "Name",
            "Lat",
            "`Long`",
            "display",
            "nickname",
            "timezone"
        ))
        q = ' '.join((
            "SELECT {cols} FROM weather".format(cols=col_list),
            "INNER JOIN stations ON weather.stationID = stations.ID",
            "WHERE deviceTime = ",
            "(SELECT MAX(deviceTime) FROM weather WHERE stationID='{}')".format(
                stationID),
            "AND stationID='{}';".format(stationID)))
        df = self.safe_query(q,400)

        if len(df) == 0:
            if verbose:
                print('[SQL WARNING] no data returned for stationID={}'.format(
                        stationID))
            return pd.DataFrame({})
        elif len(df) > 1:
            if verbose:
                print('[SQL WARNING] more than one recent result for ' +
                      'stationID={}'.format(stationID))
                print(df)
            return df.iloc[0]
        else:
            df.set_index(df['Name'], inplace=True)
            df = self.addTimeColumnsToDataframe(df, stationID=stationID)
            return df.iloc[0]

    def getInjectorStation(self):
        "The injector est station is station 0."
        return self.getStations().loc[0, :]

    def getNextTestStation(self):
        """What is this useful for?"""
        test_station = self.getStations().loc[
            self.test_station_ids[self.test_station_ids_ix], :]
        # Cycle!
        if self.test_station_ids_ix == len(self.test_station_ids) - 1:
            self.test_station_ids_ix = 0
        else:
            self.test_station_ids_ix += 1
        return test_station

    def getTimezoneFromID(self, stationID):
        """Returns the string representation of a station's timezone."""
        q = "SELECT timezone FROM stations WHERE `ID` = {};".format(stationID)
        tz = self.rawSql(q)
        return str(tz[0][0])

    def getDataForStationByRange(self, stationID, timemin, timemax, verbose=False):
        """
        Get the data from this station between timemin and timemax.
        """
        col_list = ', '.join((
            "UNIX_TIMESTAMP(deviceTime)",
            "cpm",
            "cpmError"))
        q = ' '.join((
            "SELECT {} FROM dosnet".format(col_list),
            "WHERE stationID={}".format(stationID),
            "AND UNIX_TIMESTAMP(deviceTime)",
            "BETWEEN {} AND {}".format(timemin,timemax),
            "ORDER BY deviceTime DESC;"))
        df = self.safe_query(q,400)

        if len(df) == 0:
            if verbose:
                print('[SQL WARNING] no data returned for stationID={}'.format(
                        stationID))
            return pd.DataFrame({})
        else:
            if verbose:
                print('[SQL INFO] returning data for stationID={}'.format(
                        stationID))
            return df

    def getD3SDataForStationByRange(self, stationID, timemin, timemax, verbose=False):
        col_list = ', '.join([
            "UNIX_TIMESTAMP(deviceTime)",
            "counts",
            "channelCounts"])
        q = ' '.join([
            "SELECT {} FROM d3s".format(col_list),
            "WHERE stationID={}".format(stationID),
            "AND UNIX_TIMESTAMP(deviceTime)",
            "BETWEEN {} AND {}".format(timemin,timemax),
            "ORDER BY deviceTime DESC;"])
        df = self.safe_query(q, 500)

        if len(df) == 0:
            if verbose:
                print('[SQL WARNING] no data returned for stationID={}'.format(
                        stationID))
            return pd.DataFrame({})
        else:
            if verbose:
                print('[SQL INFO] returning data for stationID={}'.format(
                        stationID))
            return df

    def getADCDataForStationByRange(self, stationID, timemin, timemax, verbose=False):
        col_list = ', '.join([
            "UNIX_TIMESTAMP(deviceTime)",
            "co2_ppm",
            "noise"])
        q = ' '.join([
            "SELECT {} FROM adc".format(col_list),
            "WHERE stationID={}".format(stationID),
            "AND UNIX_TIMESTAMP(deviceTime)",
            "BETWEEN {} AND {}".format(timemin,timemax),
            "ORDER BY deviceTime DESC;"])
        df = self.safe_query(q,400)

        if len(df) == 0:
            if verbose:
                print('[SQL WARNING] no data returned for stationID={}'.format(
                        stationID))
            return pd.DataFrame({})
        else:
            if verbose:
                print('[SQL INFO] returning data for stationID={}'.format(
                        stationID))
            return df

    def getWeatherDataForStationByRange(self, stationID, timemin, timemax, verbose=False):
        col_list = ', '.join([
            "UNIX_TIMESTAMP(deviceTime)",
            "temperature",
            "pressure",
            "humidity"])
        q = ' '.join([
            "SELECT {} FROM weather".format(col_list),
            "WHERE stationID={}".format(stationID),
            "AND UNIX_TIMESTAMP(deviceTime)",
            "BETWEEN {} AND {}".format(timemin,timemax),
            "ORDER BY deviceTime DESC;"])
        df = self.dfFromSql(q,400)

        if len(df) == 0:
            if verbose:
                print('[SQL WARNING] no data returned for stationID={}'.format(
                        stationID))
            return pd.DataFrame({})
        else:
            if verbose:
                print('[SQL INFO] returning data for stationID={}'.format(
                        stationID))
            return df

    def getAQDataForStationByRange(self, stationID, timemin, timemax, verbose=False):
        col_list = ', '.join([
            "UNIX_TIMESTAMP(deviceTime)",
            "PM1",
            "PM25",
            "PM10"])
        q = ' '.join([
            "SELECT {} FROM air_quality".format(col_list),
            "WHERE stationID={}".format(stationID),
            "AND UNIX_TIMESTAMP(deviceTime)",
            "BETWEEN {} AND {}".format(timemin,timemax),
            "ORDER BY deviceTime DESC;"])
        df = self.safe_query(q,400)

        if len(df) == 0:
            if verbose:
                print('[SQL WARNING] no data returned for stationID={}'.format(
                        stationID))
            return pd.DataFrame({})
        else:
            if verbose:
                print('[SQL INFO] returning data for stationID={}'.format(
                        stationID))
            return df

    def getDataForStationByInterval(self, stationID, intervalStr, verbose=False):
        """
        Get the last (interval) of data from this station.
        intervalStr looks like 'INTERVAL 1 DAY'.
        """
        col_list = ', '.join([
            "UNIX_TIMESTAMP(deviceTime)",
            "cpm",
            "cpmError"])
        q = ' '.join([
            "SELECT {} FROM dosnet".format(col_list),
            "WHERE stationID={}".format(stationID),
            "AND deviceTime >= (NOW() - {})".format(intervalStr),
            "ORDER BY deviceTime DESC;"])
        df = self.safe_query(q,400)

        if len(df) == 0:
            if verbose:
                print('[SQL WARNING] no data returned for stationID={}'.format(
                        stationID))
            return pd.DataFrame({})
        else:
            if verbose:
                print('[SQL INFO] returning data for stationID={}'.format(
                        stationID))
            return df
            #return self.addTimeColumnsToDataframe(df, stationID=stationID)

    def getD3SDataForStationByInterval(self, stationID, intervalStr, verbose=False):
        col_list = ', '.join([
            "UNIX_TIMESTAMP(deviceTime)",
            "counts",
            "channelCounts"])
        q = ' '.join([
            "SELECT {} FROM d3s".format(col_list),
            "WHERE stationID={}".format(stationID),
            "AND deviceTime >= (NOW() - {})".format(intervalStr),
            "ORDER BY deviceTime DESC;"])
        df = self.safe_query(q,500)

        if len(df) == 0:
            if verbose:
                print('[SQL WARNING] no data returned for stationID={}'.format(
                        stationID))
            return pd.DataFrame({})
        else:
            if verbose:
                print('[SQL INFO] returning data for stationID={}'.format(
                        stationID))
            return df
            #return self.addTimeColumnsToDataframe(df, stationID=stationID)

    def getAQDataForStationByInterval(self, stationID, intervalStr, verbose=False):
        col_list = ', '.join([
            "UNIX_TIMESTAMP(deviceTime)",
            "PM1",
            "PM25",
            "PM10"])
        q = ' '.join([
            "SELECT {} FROM air_quality".format(col_list),
            "WHERE stationID={}".format(stationID),
            "AND deviceTime >= (NOW() - {})".format(intervalStr),
            "ORDER BY deviceTime DESC;"])
        df = self.safe_query(q,400)

        if len(df) == 0:
            if verbose:
                print('[SQL WARNING] no data returned for stationID={}'.format(
                        stationID))
            return pd.DataFrame({})
        else:
            if verbose:
                print('[SQL INFO] returning data for stationID={}'.format(
                        stationID))
            return df
            #return self.addTimeColumnsToDataframe(df, stationID=stationID)

    def getWeatherDataForStationByInterval(self, stationID, intervalStr, verbose=False):
        col_list = ', '.join([
            "UNIX_TIMESTAMP(deviceTime)",
            "temperature",
            "pressure",
            "humidity"])
        q = ' '.join([
            "SELECT {} FROM weather".format(col_list),
            "WHERE stationID={}".format(stationID),
            "AND deviceTime >= (NOW() - {})".format(intervalStr),
            "ORDER BY deviceTime DESC;"])
        df = self.safe_query(q,400)

        if len(df) == 0:
            if verbose:
                print('[SQL WARNING] no data returned for stationID={}'.format(
                        stationID))
            return pd.DataFrame({})
        else:
            if verbose:
                print('[SQL INFO] returning data for stationID={}'.format(
                        stationID))
            return df
            #return self.addTimeColumnsToDataframe(df, stationID=stationID)

    def getADCDataForStationByInterval(self, stationID, intervalStr, verbose=False):
        col_list = ', '.join([
            "UNIX_TIMESTAMP(deviceTime)",
            "co2_ppm",
            "noise"])
        q = ' '.join([
            "SELECT {} FROM adc".format(col_list),
            "WHERE stationID={}".format(stationID),
            "AND deviceTime >= (NOW() - {})".format(intervalStr),
            "ORDER BY deviceTime DESC;"])
        df = self.safe_query(q,400)

        if len(df) == 0:
            if verbose:
                print('[SQL WARNING] no data returned for stationID={}'.format(
                        stationID))
            return pd.DataFrame({})
        else:
            if verbose:
                print('[SQL INFO] returning data for stationID={}'.format(
                        stationID))
            return df
            #return self.addTimeColumnsToDataframe(df, stationID=stationID)

    def addTimeColumnsToDataframe(self, df, stationID=None, tz=None):
        """
        Input dataframe from query with UNIX_TIMESTAMP for deviceTime. The output has 3 time columns:
            deviceTime_unix (renamed from UNIX_TIMESTAMP(deviceTime))
            deviceTime_utc
            deviceTime_local

        stackoverflow.com/questions/17159207/change-timezone-of-date-time-
          column-in-pandas-and-add-as-hierarchical-index
        """
        if isinstance(tz, str):
            this_tz = tz
        elif isinstance(stationID, (int, str)):
            this_tz = self.getTimezoneFromID(stationID)
        else:
            print('[TZ WARNING] Defaulting to `US/Pacific`')
            this_tz = 'US/Pacific'
        # Sanity check
        assert isinstance(this_tz, str), '[TZ ERROR] Not a tz str: {}'.format(this_tz)

        # Rename existing unix epoch seconds columns
        df.rename(inplace=True, columns={
            'UNIX_TIMESTAMP(deviceTime)': 'deviceTime_unix'})

        # Timezones are evil but pandas are fuzzy ...
        deviceTime = pd.Index(pd.to_datetime(
            df['deviceTime_unix'], unit='s')).tz_localize('UTC')
        df['deviceTime_utc'] = deviceTime
        df['deviceTime_local'] = deviceTime.tz_convert(this_tz)

        # Rearrange the columns (iterate in opposite order of placement)
        new_cols = df.columns.tolist()
        for colname in ['deviceTime_unix',
                        'deviceTime_local',
                        'deviceTime_utc']:
            new_cols.insert(0, new_cols.pop(new_cols.index(colname)))
        df = df[new_cols]
        return df

    def getLastHour(self, stationID, request_type=None, verbose=False):
        if request_type == 'd3s':
            func = self.getD3SDataForStationByInterval
        elif request_type == 'aq':
            func = self.getAQDataForStationByInterval
        elif request_type == 'weather':
            func = self.getWeatherDataForStationByInterval
        elif request_type == 'adc':
            func = self.getADCDataForStationByInterval
        else:
            func = self.getDataForStationByInterval
        return func(stationID,'INTERVAL 1 HOUR',verbose)

    def getLastDay(self, stationID, request_type=None, verbose=False):
        if request_type == 'd3s':
            func = self.getD3SDataForStationByInterval
        elif request_type == 'aq':
            func = self.getAQDataForStationByInterval
        elif request_type == 'weather':
            func = self.getWeatherDataForStationByInterval
        elif request_type == 'adc':
            func = self.getADCDataForStationByInterval
        else:
            func = self.getDataForStationByInterval
        return func(stationID,'INTERVAL 1 DAY',verbose)

    def getLastWeek(self, stationID, request_type=None, verbose=False):
        if request_type == 'd3s':
            func = self.getD3SDataForStationByInterval
        elif request_type == 'aq':
            func = self.getAQDataForStationByInterval
        elif request_type == 'weather':
            func = self.getWeatherDataForStationByInterval
        elif request_type == 'adc':
            func = self.getADCDataForStationByInterval
        else:
            func = self.getDataForStationByInterval
        return func(stationID,'INTERVAL 1 WEEK',verbose)

    def getLastMonth(self, stationID, request_type=None, verbose=False):
        if request_type == 'd3s':
            func = self.getD3SDataForStationByInterval
        elif request_type == 'aq':
            func = self.getAQDataForStationByInterval
        elif request_type == 'weather':
            func = self.getWeatherDataForStationByInterval
        elif request_type == 'adc':
            func = self.getADCDataForStationByInterval
        else:
            func = self.getDataForStationByInterval
        return func(stationID,'INTERVAL 1 MONTH',verbose)

    def getLastYear(self, stationID, request_type=None, verbose=False):
        if request_type == 'd3s':
            func = self.getD3SDataForStationByInterval
        elif request_type == 'aq':
            func = self.getAQDataForStationByInterval
        elif request_type == 'weather':
            func = self.getWeatherDataForStationByInterval
        elif request_type == 'adc':
            func = self.getADCDataForStationByInterval
        else:
            func = self.getDataForStationByInterval
        return func(stationID,'INTERVAL 1 YEAR',verbose)

    def getAll(self, stationID, request_type=None, verbose=False):
        if request_type == 'd3s':
            func = self.getD3SDataForStationByInterval
        elif request_type == 'aq':
            func = self.getAQDataForStationByInterval
        elif request_type == 'weather':
            func = self.getWeatherDataForStationByInterval
        elif request_type == 'adc':
            func = self.getADCDataForStationByInterval
        else:
            func = self.getDataForStationByInterval
        return func(stationID,'INTERVAL 10 YEAR',verbose)

    def testLastMethods(self, stationID=1):
        """Test SQLObject.getLast* methods"""
        print('Testing last data methods with stationID={}\n'.format(stationID))
        for method in [self.getLastDay, self.getLastWeek, self.getLastMonth,
                       self.getLastYear]:
            print('Testing: {}'.format(method))
            df = method(stationID)
            print('Num Entries:', len(df))
            print('HEAD:\n{}'.format(df.head()))
            print()


class AuthenticationError(Exception):
    pass


if __name__ == "__main__":
    sql = SQLObject()
    pd.set_option('display.expand_frame_repr', False)
    sql.testLastMethods(6)
