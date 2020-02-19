#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import numpy as np
import pandas as pd
import sys
import datetime
import time
import pytz
import traceback

'''
this is really similiar to mysql_tools.py; I changed the insertinto... method for 
sql server into text file format. Each text file has a title of stationID + type
+ txt. So far I did not change the getter and setter, which is written for sql
'''

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


class TextObject:
    def __init__(self, tz='+00:00', Data_Path="../dosenet_data/"):
        self.set_session_tz(tz)
        self.test_station_ids = [0, 10001, 10002, 10003, 10004, 10005]
        self.test_station_ids_ix = 0
        self.Data_Path = Data_Path

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
        #self.refresh()

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
        Create text file with the name of stationID and the type, dosimeter.

        Data are written in the order of device time, stationID, cpm, cpm_error, error flag

        Insert a row of dosimeter data into the dosnet table

        NOTE that the receiveTime is not included since that is assigned my
        the MySQL default value of CURRENT_TIMESTAMP
        """
        if (not isinstance(deviceTime, int) and
                not isinstance(deviceTime, float)):
            if deviceTime is not None:
                print('Warning: received non-numeric deviceTime! Ignoring')
            deviceTime = time.time()
        deviceTimeUTC = epoch_to_datetime(time.time()).strftime('%Y-%m-%d %H:%M:%S%z')
        deviceTimeLocal = epoch_to_datetime(time.time(), self.getStationTZ(stationID)).strftime('%Y-%m-%d %H:%M:%S%z')

    # sql_cmd = (
        #     "INSERT INTO " +
        #     "dosnet(deviceTime, stationID, cpm, cpmError, errorFlag) " +
        #     "VALUES (FROM_UNIXTIME({:.3f}), {}, {}, {}, {});".format(
        #         deviceTime, stationID, cpm, cpm_error, error_flag))
        # self.cursor.execute(sql_cmd)
        # self.db.commit()
        dosimeterfile = open(self.Data_Path + "dosenet/" + self.getStationName(stationID) + ".csv", "a+")
        #dosimeterfile.write(str(deviceTime) + ", " + str(stationID) + ", " + str(cpm) + ", " + str(cpm_error)
        #   + ", " + str(error_flag) + "\n")
        dosimeterfile.write("{},{},{},{},{},{}\n".format(deviceTimeUTC, deviceTimeLocal, deviceTime, cpm, cpm_error, error_flag))
        dosimeterfile.close()



    def insertIntoAQ(self, stationID, oneMicron, twoPointFiveMicron, tenMicron,
                     error_flag, deviceTime = None, **kwargs):
        """
        Create text file with the name of stationID and the type, AQ.

        Data are written in the order of device time, stationID, oneMicron,
        twoPointFiveMicron, TenMicron, error flag

        Insert a row of Air Quality data into the Air Quality table
        """
        if (not isinstance(deviceTime, int) and
                not isinstance(deviceTime, float)):
            if deviceTime is not None:
                print('Warning: received non-numeric deviceTime! Ignoring')
            deviceTime = time.time()
        deviceTimeUTC = epoch_to_datetime(time.time()).strftime('%Y-%m-%d %H:%M:%S%z')
        deviceTimeLocal = epoch_to_datetime(time.time(), self.getStationTZ(stationID)).strftime('%Y-%m-%d %H:%M:%S%z')
        # sql_cmd = (
        #     "INSERT INTO " +
        #     "air_quality(deviceTime, stationID, PM1, PM25, PM10, errorFlag) " +
        #     "VALUES (FROM_UNIXTIME({:.3f}), {}, {}, {}, {}, {});".format(
        #         deviceTime, stationID, oneMicron, twoPointFiveMicron, tenMicron, error_flag))
        # self.cursor.execute(sql_cmd)
        # self.db.commit()
        aqfile = open(self.Data_Path + "dosenet/" + self.getStationName(stationID) + "_aq.csv", "a+")
        #aqfile.write(str(deviceTime) + ", " + str(stationID) + ", " + str(oneMicron) + ", "
        #    + str(twoPointFiveMicron) + ", " + str(tenMicron) + ", " + str(error_flag) + "\n")
        aqfile.write("{},{},{},{},{},{},{}\n".format(deviceTimeUTC, deviceTimeLocal, deviceTime, oneMicron, twoPointFiveMicron,
        tenMicron, error_flag))
        aqfile.close()

    def insertIntoCO2(self, stationID, co2_ppm, noise, error_flag, deviceTime = None, **kwargs):
        """
        Create text file with the name of stationID and the type, CO2.

        Data are written in the order of device time, stationID, co2_ppm, noise, error flag

        Insert a row of CO2 data into the CO2 table
        """
        if (not isinstance(deviceTime, int) and
                not isinstance(deviceTime, float)):
            if deviceTime is not None:
                print('Warning: received non-numeric deviceTime! Ignoring')
            deviceTime = time.time()
        deviceTimeUTC = epoch_to_datetime(time.time()).strftime('%Y-%m-%d %H:%M:%S%z')
        deviceTimeLocal = epoch_to_datetime(time.time(), self.getStationTZ(stationID)).strftime('%Y-%m-%d %H:%M:%S%z')
        # sql_cmd = (
        #     "INSERT INTO " +
        #     "adc(deviceTime, stationID, co2_ppm, noise, errorFlag) " +
        #     "VALUES (FROM_UNIXTIME({:.3f}), {}, {}, {}, {});".format(
        #         deviceTime, stationID, co2_ppm, noise, error_flag))
        # self.cursor.execute(sql_cmd)
        # self.db.commit()
        co2file = open(self.Data_Path + "dosenet/" + self.getStationName(stationID) + "_adc.csv", "a+")
        #co2file.write(str(deviceTime) + ", " + str(stationID) + ", " + str(co2_ppm) + ", "
        #        + str(noise) + ", " + str(error_flag) + "\n")\
        co2file.write("{},{},{},{},{},{}\n".format(deviceTimeUTC, deviceTimeLocal, deviceTime, co2_ppm, noise, error_flag))
        co2file.close()

    def insertIntoWeather(self, stationID, temperature, pressure,
                          humidity, error_flag, deviceTime = None, **kwargs):
        """
        Create text file with the name of stationID and the type, weather.

        Data are written in the order of device time, stationID, temperature, pressure, humidity, error_flag

        Insert a row of Weather data into the Weather table
        """
        if (not isinstance(deviceTime, int) and
                not isinstance(deviceTime, float)):
            if deviceTime is not None:
                print('Warning: received non-numeric deviceTime! Ignoring')
            deviceTime = time.time()
        deviceTimeUTC = epoch_to_datetime(time.time()).strftime('%Y-%m-%d %H:%M:%S%z')
        deviceTimeLocal = epoch_to_datetime(time.time(), self.getStationTZ(stationID)).strftime('%Y-%m-%d %H:%M:%S%z')

        # sql_cmd = (
        #     "INSERT INTO " +
        #     "weather(deviceTime, stationID, temperature, pressure, humidity, errorFlag) " +
        #     "VALUES (FROM_UNIXTIME({:.3f}), {}, {}, {}, {}, {});".format(
        #         deviceTime, stationID, temperature, pressure, humidity, error_flag))
        # self.cursor.execute(sql_cmd)
        # self.db.commit()
        weatherfile = open(self.Data_Path + "dosenet/" + self.getStationName(stationID) + "_weather.csv", "a+")
        #weatherfile.write(str(deviceTime) + ", " + str(stationID) + ", " + str(temperature) + ", "
        #       + str(pressure) + ", " + str(humidity) + ", " + str(error_flag) + "\n")
        weatherfile.write("{},{},{},{},{},{},{}\n".format(deviceTimeUTC, deviceTimeLocal ,deviceTime, temperature, pressure,
        humidity, error_flag))
        weatherfile.close()

    def insertIntoD3S(self, stationID, spectrum, error_flag, deviceTime = None,
                      **kwargs):
        """
        Create text file with the name of stationID and the type, D3S.

        Data are written in the order of device time, stationID, count, spectrum_blob, error flag
        Insert a row of D3S data into the d3s table.
        """
        counts = sum(spectrum)
        spectrum = np.array(spectrum, dtype=np.uint8)
        spectrum_blob = spectrum.tobytes()
        if (not isinstance(deviceTime, int) and
                not isinstance(deviceTime, float)):
            if deviceTime is not None:
                print('Warning: received non-numeric deviceTime! Ignoring')
            deviceTime = time.time()
        deviceTimeUTC = epoch_to_datetime(time.time()).strftime('%Y-%m-%d %H:%M:%S%z')
        deviceTimeLocal = epoch_to_datetime(time.time(), self.getStationTZ(stationID)).strftime('%Y-%m-%d %H:%M:%S%z')
        # sql_cmd = (
        #     "INSERT INTO " +
        #     "d3s(deviceTime, stationID, counts, channelCounts, errorFlag) " +
        #     "VALUES (FROM_UNIXTIME({:.3f}), {}, {}, {}, {});".format(
        #         deviceTime, stationID, counts, '%s', error_flag))
        # # let MySQLdb library handle the special characters in the blob
        # self.cursor.execute(sql_cmd, (spectrum_blob,))
        # self.db.commit()
        d3sfile = open(self.Data_Path + "dosenet/" + self.getStationName(stationID) + "_d3s.csv", "a+")
        #d3sfile.write(str(deviceTime) + ", " + str(counts) + ", " + str(spectrum_blob)
        #       + ", " + str(error_flag) + "\n")
        d3sfile.write("{},{},{},{},{},{}\n".format(deviceTimeUTC, deviceTimeLocal, deviceTime, counts, spectrum_blob, error_flag))
        d3sfile.close()

    def insertIntoLog(self, stationID, msgCode, msgText, **kwargs):
        """
        Create text file with the name of stationID and the type, dosimeter.

        Data are written in the order of magCode, msgText error flag

        Insert a log message into the stationlog table.
        """
        # sql_cmd = ("INSERT INTO stationlog(stationID, msgCode, message) " +
        #            "VALUES ({}, {}, '{}')".format(stationID, msgCode, msgText))
        # self.cursor.execute(sql_cmd)
        # self.db.commit()
        if (not isinstance(deviceTime, int) and
                not isinstance(deviceTime, float)):
            if deviceTime is not None:
                print('Warning: received non-numeric deviceTime! Ignoring')
            deviceTime = time.time()
        deviceTimeUTC = epoch_to_datetime(time.time()).strftime('%Y-%m-%d %H:%M:%S%z')
        deviceTimeLocal = epoch_to_datetime(time.time(), self.getStationTZ(stationID)).strftime('%Y-%m-%d %H:%M:%S%z')
        logfile = open(self.Data_Path + "dosenet/" + self.getStationName(stationID) + "_log.csv", "a+")
        #logfile.write(str(msgCode) + ", " + str(msgText) + "\n")
        logfile.write("{},{},{},{},{}\n".format(deviceTimeUTC, deviceTimeLocal, deviceTime, msgCode, msgText))
        logfile.close()

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

    def getStationReturnInfo(self, stationID):
        """Read gitBranch and needsUpdate from stations table."""
        station = pd.read_csv(self.Data_Path + "Station.csv")
        fil = station['ID'] == stationID
        needs_update = station[fil].iloc[0]["needsUpdate"]
        git_branch = station[fil].iloc[0]["gitBranch"]
        return git_branch, needs_update

    def getStationName(self, stationID):
        """Read gitBranch and needsUpdate from stations table."""
        station = pd.read_csv(self.Data_Path + "Station.csv")
        fil = station['ID'] == stationID
        name = station[fil].iloc[0]["nickname"]
        return name

    def getStationTZ(self, stationID):
        """Read gitBranch and needsUpdate from stations table."""
        station = pd.read_csv(self.Data_Path + "Station.csv")
        fil = station['ID'] == stationID
        name = station[fil].iloc[0]["timezone"]
        return name



        '''
        self.refresh()
        col_list = "gitBranch, needsUpdate"
        q = "SELECT {} FROM stations WHERE `ID` = {};".format(
            col_list, stationID)
        df = self.dfFromSql(q)

        needs_update = df['needsUpdate'][0]
        git_branch = df['gitBranch'][0]

        return git_branch, needs_update
        '''
        '''
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
        #self.refresh()
        col_list = "gitBranch, needsUpdate"
        q = "SELECT {} FROM stations WHERE `ID` = {};".format(
            col_list, stationID)
        df = self.dfFromSql(q)

        needs_update = df['needsUpdate'][0]
        git_branch = df['gitBranch'][0]

        return git_branch, needs_update
    
# ---------------------------------------------------------------------------
#       STATION-UPDATE-RELATED METHODS
# ---------------------------------------------------------------------------

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
        df = self.dfFromSql(q)
        df.set_index(df['ID'], inplace=True)
        del df['ID']
        return df

    def getActiveStations(self):
        """Read the stations table, but only entries with display==1."""
        df = self.getStations()
        df = df[df['display'] == 1]
        del df['display']
        return df

    def getActiveD3SStations(self):
        """Read the stations table, but only entries with display==1."""
        df = self.getActiveStations()
        active_list = [x[1]=="1" for x in df['devices'].tolist()]
        df = df[pd.Series([x[1]=="1" for x in df['devices'].tolist()],
                          index=df['devices'].index)]
        return df

    def getActiveAQStations(self):
        """Read the stations table, but only entries with display==1."""
        df = self.getActiveStations()
        active_list = [x[2]=="1" for x in df['devices'].tolist()]
        df = df[pd.Series([x[2]=="1" for x in df['devices'].tolist()],
                          index=df['devices'].index)]
        return df

    def getActiveWeatherStations(self):
        """Read the stations table, but only entries with display==1."""
        df = self.getActiveStations()
        active_list = [x[3]=="1" for x in df['devices'].tolist()]
        df = df[pd.Series([x[3]=="1" for x in df['devices'].tolist()],
                          index=df['devices'].index)]
        return df

    def getActiveADCStations(self):
        """Read the stations table, but only entries with display==1."""
        df = self.getActiveStations()
        active_list = [x[4]=="1" for x in df['devices'].tolist()]
        df = df[pd.Series([x[4]=="1" for x in df['devices'].tolist()],
                          index=df['devices'].index)]
        return df

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
        try:
            df = self.dfFromSql(q)
            df.set_index(df['Name'], inplace=True)
            df = self.addTimeColumnsToDataframe(df, stationID=stationID)
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

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
        try:
            df = self.dfFromSql(q)
            df.set_index(df['Name'], inplace=True)
            df = self.addTimeColumnsToDataframe(df, stationID=stationID)
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

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
        try:
            df = self.dfFromSql(q)
            df.set_index(df['Name'], inplace=True)
            df = self.addTimeColumnsToDataframe(df, stationID=stationID)
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

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
        try:
            df = self.dfFromSql(q)
            df.set_index(df['Name'], inplace=True)
            df = self.addTimeColumnsToDataframe(df, stationID=stationID)
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

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
        try:
            df = self.dfFromSql(q)
            df.set_index(df['Name'], inplace=True)
            df = self.addTimeColumnsToDataframe(df, stationID=stationID)
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

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
        try:
            df = self.dfFromSql(q)
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

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
        try:
            df = pd.read_sql(q, con=self.db)
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

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
        try:
            df = self.dfFromSql(q)
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

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
        try:
            df = self.dfFromSql(q)
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

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
        try:
            df = self.dfFromSql(q)
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

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
        try:
            df = self.dfFromSql(q)
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

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
        try:
            df = self.dfFromSql(q)
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

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
        try:
            df = self.dfFromSql(q)
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

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
        try:
            df = self.dfFromSql(q)
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

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
        try:
            df = self.dfFromSql(q)
        except (Exception) as e:
            print(e)
            return pd.DataFrame({})

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
        '''