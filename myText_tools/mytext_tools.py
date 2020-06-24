#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import numpy as np
import pandas as pd
import datetime
import time
import pytz
import re
from os import listdir
from os.path import isfile, join

"""
this is really similiar to mysql_tools.py; I changed the insertinto... method for 
sql server into text file format. Each text file has a title of stationID + type
+ txt. So far I did not change the getter and setter, which is written for sql.
"""

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
        try:
            dosimeterfile = open(self.Data_Path + "dosenet/" + self.getStationName(stationID) + ".csv", "a+")
            dosimeterfile.write("{},{},{},{},{},{}\n".format(deviceTimeUTC, deviceTimeLocal, deviceTime, cpm, cpm_error, error_flag))
            dosimeterfile.close()
        except:
            pass

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
        try:
            aqfile = open(self.Data_Path + "dosenet/" + self.getStationName(stationID) + "_aq.csv", "a+")
            aqfile.write("{},{},{},{},{},{},{}\n".format(deviceTimeUTC, deviceTimeLocal, deviceTime, oneMicron, twoPointFiveMicron,
            tenMicron, error_flag))
            aqfile.close()
        except:
            pass

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
        try:
            co2file = open(self.Data_Path + "dosenet/" + self.getStationName(stationID) + "_adc.csv", "a+")
            co2file.write("{},{},{},{},{},{}\n".format(deviceTimeUTC, deviceTimeLocal, deviceTime, co2_ppm, noise, error_flag))
            co2file.close()
        except:
            pass

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
        try:
            weatherfile = open(self.Data_Path + "dosenet/" + self.getStationName(stationID) + "_weather.csv", "a+")
            weatherfile.write("{},{},{},{},{},{},{}\n".format(deviceTimeUTC, deviceTimeLocal ,deviceTime, temperature, pressure,
            humidity, error_flag))
            weatherfile.close()
        except:
            pass

    def insertIntoD3S(self, stationID, spectrum, error_flag, deviceTime = None,
                      **kwargs):
        """
        Create text file with the name of stationID and the type, D3S.

        Data are written in the order of device time, stationID, count, spectrum_blob, error flag

        Insert a row of D3S data into the d3s table.
        """
        spectrum = np.array(spectrum, dtype=np.uint8)
        rebin_array = spectrum.reshape(len(spectrum) // 4, 4).sum(1)
        spectrum_string = ",".join(map(str, rebin_array.tolist()))
        cpm = sum(rebin_array) / 5
        cpm_error = np.sqrt(sum(rebin_array)) / 5
        keV_per_ch = 2.57
        if (not isinstance(deviceTime, int) and
                not isinstance(deviceTime, float)):
            if deviceTime is not None:
                print('Warning: received non-numeric deviceTime! Ignoring')
            deviceTime = time.time()
        deviceTimeUTC = epoch_to_datetime(time.time()).strftime('%Y-%m-%d %H:%M:%S%z')
        deviceTimeLocal = epoch_to_datetime(time.time(), self.getStationTZ(stationID)).strftime('%Y-%m-%d %H:%M:%S%z')
        try:
            d3sfile = open(self.Data_Path + "dosenet/" + self.getStationName(stationID) + "_d3s.csv", "a+")
            d3sfile.write("{},{},{},{},{},{},{},{}\n".format(deviceTimeUTC, deviceTimeLocal, deviceTime, cpm, cpm_error, keV_per_ch, spectrum_string, error_flag))
            d3sfile.close()
        except:
            pass

    def insertIntoLog(self, stationID, msgCode, msgText, deviceTime = None, **kwargs):
        """
        Create text file with the name of stationID and the type, dosimeter.

        Data are written in the order of magCode, msgText error flag

        Insert a log message into the stationlog table.
        """
        if (not isinstance(deviceTime, int) and
                not isinstance(deviceTime, float)):
            if deviceTime is not None:
                print('Warning: received non-numeric deviceTime! Ignoring')
            deviceTime = time.time()
        deviceTimeUTC = epoch_to_datetime(time.time()).strftime('%Y-%m-%d %H:%M:%S%z')
        deviceTimeLocal = epoch_to_datetime(time.time(), self.getStationTZ(stationID)).strftime('%Y-%m-%d %H:%M:%S%z')
        try:
            logfile = open(self.Data_Path + "dosenet/" + self.getStationName(stationID) + "_log.csv", "a+")
            logfile.write("{},{},{},{},{}\n".format(deviceTimeUTC, deviceTimeLocal, deviceTime, msgCode, msgText))
            logfile.close()
        except:
            pass

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

    # ethan: make a getall, read it like the the writing method, for text reading csv with dataframe
    # return panda dataframe.
    def getAll(self, stationID, request_type=None, verbose=False):
        if request_type == 'd3s':
            d3sfile = open(self.Data_Path + "dosenet/" + self.getStationName(stationID) + "_d3s.csv", "r")
            data = pd.read_csv(d3sfile)
            d3sfile.close()
        elif request_type == 'aq':
            aqfile = open(self.Data_Path + "dosenet/" + self.getStationName(stationID) + "_aq.csv", "r")
            data = pd.read_csv(aqfile)
            aqfile.close()
        elif request_type == 'weather':
            weatherfile = open(self.Data_Path + "dosenet/" + self.getStationName(stationID) + "_weather.csv", "r")
            data = pd.read_csv(weatherfile)
            weatherfile.close()
        elif request_type == 'adc':
            co2file = open(self.Data_Path + "dosenet/" + self.getStationName(stationID) + "_adc.csv", "r")
            data = pd.read_csv(co2file)
            co2file.close()
        else:
            dosimeterfile = open(self.Data_Path + "dosenet/" + self.getStationName(stationID) + ".csv", "r")
            data = pd.read_csv(dosimeterfile)
            dosimeterfile.close()
        return data

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

    def getStations(self):
        """Read the stations table from MySQL into a pandas dataframe."""
        q = "SELECT * FROM stations;"
        df = pd.read_csv(join(self.Data_Path, "Station.csv"))
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
        df = df[pd.Series([str(x)[1]=="1" for x in df['devices'].tolist()],
                          index=df['devices'].index)]
        return df

    def getActiveAQStations(self):
        """Read the stations table, but only entries with display==1."""
        df = self.getActiveStations()
        df = df[pd.Series([str(x)[2]=="1" for x in df['devices'].tolist()],
                          index=df['devices'].index)]
        return df

    def getActiveWeatherStations(self):
        """Read the stations table, but only entries with display==1."""
        df = self.getActiveStations()
        df = df[pd.Series([str(x)[3]=="1" for x in df['devices'].tolist()],
                          index=df['devices'].index)]
        return df

    def getActiveADCStations(self):
        """Read the stations table, but only entries with display==1."""
        df = self.getActiveStations()
        df = df[pd.Series([str(x)[4]=="1" for x in df['devices'].tolist()],
                          index=df['devices'].index)]
        return df

    def getLatestStationData(self, stationID, type, verbose=False, time_stamp=None):
        """Return most recent data entry for given station."""
        if type != "adc" and type != "aq" and type != "d3s" and type != "weather" and type != "":
            raise IOError("type given does not exist.")
        if type != "":
            type = "_" + type
        stations_data = pd.read_csv(self.Data_Path + "Station.csv")
        station_row = stations_data.loc[stations_data['ID'] == stationID].iloc[0]
        station_name = station_row[1]
        nick_name = station_row[-2]
        lat = station_row[2]
        long = station_row[3]
        time_zone = station_row[-1]
        cpm_to_rem = station_row[4]
        cpm_to_usv = station_row[5]
        display = station_row[6]
        station_file = pd.read_csv(self.Data_Path + "dosenet/" + nick_name + type + ".csv")
        """checking that station_file has data, else return a dictionary with None(s)"""
        if len(station_file) < 1:
            cols = {"deviceTime_unix": None,
                    "stationID": stationID,
                    "cpm": None,
                    "cpmError": None,
                    "co2_ppm": None,
                    "counts": None,
                    "errorFlag": None,
                    "temperature": None,
                    "pressure": None,
                    "humidity": None,
                    "PM25": None,
                    "ID": stationID,
                    "Name": station_name,
                    "Lat": lat,
                    "`Long`": long,
                    "cpmtorem": cpm_to_rem,
                    "cpmtousv": cpm_to_usv,
                    "display": display,
                    "nickname": nick_name,
                    "timezone": time_zone}
            return cols
        last_data = station_file.iloc[station_file['deviceTime_unix'].idxmax()]
        time_received = last_data[2]
        if type == "":
            cols = {"deviceTime_unix": time_received,
                    "stationID" : stationID,
                    "cpm": last_data[-3],
                    "cpmError": last_data[-2],
                    "errorFlag": last_data[-1],
                    "ID": stationID,
                    "Name": station_name,
                    "Lat": lat,
                    "`Long`": long,
                    "cpmtorem": cpm_to_rem,
                    "cpmtousv": cpm_to_usv,
                    "display": display,
                    "nickname": nick_name,
                    "timezone": time_zone}
            return cols
        if type == "_adc":
            cols = {"deviceTime_unix": time_received,
                    "stationID": stationID,
                    "co2_ppm": last_data[-2],
                    "errorFlag": last_data[-1],
                    "ID": stationID,
                    "Name": station_name,
                    "Lat": lat,
                    "`Long`": long,
                    "cpmtorem": cpm_to_rem,
                    "cpmtousv": cpm_to_usv,
                    "display": display,
                    "nickname": nick_name,
                    "timezone": time_zone}
            return cols
        if type == "_d3s":
            cols = {"deviceTime_unix": time_received,
                    "stationID": stationID,
                    "counts": last_data[3],
                    "errorFlag": last_data[-1],
                    "ID": stationID,
                    "Name": station_name,
                    "Lat": lat,
                    "`Long`": long,
                    "cpmtorem": cpm_to_rem,
                    "cpmtousv": cpm_to_usv,
                    "display": display,
                    "nickname": nick_name,
                    "timezone": time_zone}
            return cols
        if type == "_weather":
            cols = {"deviceTime_unix": time_received,
                    "stationID": stationID,
                    "temperature": last_data[-4],
                    "pressure": last_data[-3],
                    "humidity": last_data[-2],
                    "errorFlag": last_data[-1],
                    "ID": stationID,
                    "Name": station_name,
                    "Lat": lat,
                    "`Long`": long,
                    "cpmtorem": cpm_to_rem,
                    "cpmtousv": cpm_to_usv,
                    "display": display,
                    "nickname": nick_name,
                    "timezone": time_zone}
            return cols
        if type == "_aq":
            cols = {"deviceTime_unix": time_received,
                    "stationID": stationID,
                    "PM25": last_data[-3],
                    "errorFlag": last_data[-1],
                    "ID": stationID,
                    "Name": station_name,
                    "Lat": lat,
                    "`Long`": long,
                    "cpmtorem": cpm_to_rem,
                    "cpmtousv": cpm_to_usv,
                    "display": display,
                    "nickname": nick_name,
                    "timezone": time_zone}
            return cols
        else:
            raise IOError("No type matches")

class AuthenticationError(Exception):
    pass
