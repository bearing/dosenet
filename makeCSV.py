#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from mysql.mysql_tools import SQLObject
from data_transfer import DataFile
import numpy as np
import time
import datetime as dt
import math
import pandas as pd

docstring = """
MYSQL to CSV writer.

Authors:
    2017-3-5 - Ali Hanks
    2015-11-13 - Joseph Curtis
Affiliation:
    DoseNet
    Applied Nuclear Physics Division
    Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
"""

def get_rounded_time(t):
    # set resolution to nearest minute
    td = dt.timedelta(hours=t.hour, minutes=t.minute, seconds=t.second, microseconds=t.microsecond)
    to_min = dt.timedelta(minutes=round(td.total_seconds()/60))
    t = dt.datetime.combine(t,dt.time(0))+to_min
    return time.mktime(t.timetuple())

# integration_time (min) = time to average over for condenced data
# n_intervals = number of intervals to collect (1 day = 60/integration_time * 24)
def get_compressed_data(DB,sid,integration_time,n_intervals):
    interval = dt.timedelta(minutes=integration_time).total_seconds()
    max_time = get_rounded_time(dt.datetime.now())
    min_time = max_time - interval*n_intervals

    comp_data = np.array([['deviceTime_unix','cpm','cpmError']])
    while max_time > min_time:
        df = DB.getDataForStationByRange(sid,max_time - interval,max_time)
        if len(df) > 0:
            cpm = df.loc[:,'cpm'].sum()*5/(len(df)*5)
            cpm_err = math.sqrt(df.loc[:,'cpm'].sum()*5)/(len(df)*5)
            # use time-bin central time
            itime = df.iloc[len(df)/2,0]
            comp_data = np.append(comp_data,[[itime,cpm,cpm_err]],axis=0)
            max_time = max_time - interval

    comp_df = pd.DataFrame(data=comp_data[1:,:], columns=comp_data[0,:])
    comp_df = DB.addTimeColumnsToDataframe(comp_df,sid)
    return comp_df

def main(verbose=False, 
         last_day=False,
         last_week=False,
         last_month=False,
         last_year=False,
         **kwargs):
    if last_year:
        last_month = True
    if last_month:
        last_week = True
    if last_week:
        last_day = True

    start_time = time.time()
    # -------------------------------------------------------------------------
    # Mysql data base interface
    # -------------------------------------------------------------------------
    DB = SQLObject()
    # -------------------------------------------------------------------------
    # Pick active stations
    # -------------------------------------------------------------------------
    stations = DB.getActiveStations()
    print(stations)
    print()
    # -------------------------------------------------------------------------
    # Pull data for each station, save to CSV and transfer
    # -------------------------------------------------------------------------
    for sid, name, nick in zip(stations.index, stations['Name'],
                               stations['nickname']):
        print('(id={}) {}'.format(sid, name))
        df = DB.getAll(sid)
        print('    Loaded raw data')
        csvfile = DataFile.csv_from_nickname(nick)
        csvfile.df_to_file(df)

        df = DB.getLastHour(sid)
        print('    Loaded last hour of data')
        compressed_nick = nick + '_hour'
        csvfile = DataFile.csv_from_nickname(compressed_nick)
        csvfile.df_to_file(df)

        if last_day:
            df = get_compressed_data(DB,sid,30,48)
            print('    Compressed last day of data')
            compressed_nick = nick + '_day'
            csvfile = DataFile.csv_from_nickname(compressed_nick)
            csvfile.df_to_file(df)

        if last_week:
            df = get_compressed_data(DB,sid,60,168)
            print('    Compressed last week of data')
            compressed_nick = nick + '_week'
            csvfile = DataFile.csv_from_nickname(compressed_nick)
            csvfile.df_to_file(df)

        if last_month:
            df = get_compressed_data(DB,sid,240,180)
            print('    Compressed last month of data')
            compressed_nick = nick + '_month'
            csvfile = DataFile.csv_from_nickname(compressed_nick)
            csvfile.df_to_file(df)

        if last_year:
            df = get_compressed_data(DB,sid,2880,183)
            print('    Compressed last year of data')
            compressed_nick = nick + '_year'
            csvfile = DataFile.csv_from_nickname(compressed_nick)
            csvfile.df_to_file(df)

        print()

    print('Total run time: {:.2f} sec'.format(time.time() - start_time))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=docstring)
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='Print more output')
    parser.add_argument('-d', '--last-day', action='store_true',
                        help='get compressed csv for last day')
    parser.add_argument('-w', '--last-week', action='store_true',
                        help='get compressed csv for last week')
    parser.add_argument('-m', '--last-month', action='store_true',
                        help='get compressed csv for last month')
    parser.add_argument('-y', '--last-year', action='store_true',
                        help='get compressed csv for last year')
    args = parser.parse_args()
    main(**vars(args))
