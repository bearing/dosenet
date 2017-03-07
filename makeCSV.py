#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from mysql.mysql_tools import SQLObject
from data_transfer import DataFile
import time
import datetime as dt
import math
import pandas as pd
import multiprocessing

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

get_day = False
get_week = False
get_month = False
get_year = False

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

    comp_df = pd.DataFrame(columns=['deviceTime_unix','cpm','cpmError'])
    for idx in range(n_intervals):
        df = DB.getDataForStationByRange(sid,max_time - interval,max_time)
        if len(df) > 0:
            cpm = df.loc[:,'cpm'].sum()*5/(len(df)*5)
            cpm_err = math.sqrt(df.loc[:,'cpm'].sum()*5)/(len(df)*5)
            # use time-bin central time
            itime = df.iloc[len(df)/2,0]
            comp_df.loc[idx,'deviceTime_unix'] = itime
            comp_df.loc[idx,'cpm'] = cpm
            comp_df.loc[idx,'cpmError'] = cpm_err
            max_time = max_time - interval

    comp_df = DB.addTimeColumnsToDataframe(comp_df,sid)
    return comp_df

def make_station_files(sid,nick):
    DB = SQLObject()
    df = DB.getAll(sid)
    print('    Loaded raw data')
    csvfile = DataFile.csv_from_nickname(nick)
    csvfile.df_to_file(df)

    df = DB.getLastHour(sid)
    print('    Loaded last hour of data')
    compressed_nick = nick + '_hour'
    csvfile = DataFile.csv_from_nickname(compressed_nick)
    csvfile.df_to_file(df)

    if get_day:
        df = get_compressed_data(DB,sid,30,48)
        print('    Compressed last day of data')
        compressed_nick = nick + '_day'
        csvfile = DataFile.csv_from_nickname(compressed_nick)
        csvfile.df_to_file(df)

    if get_week:
        df = get_compressed_data(DB,sid,60,168)
        print('    Compressed last week of data')
        compressed_nick = nick + '_week'
        csvfile = DataFile.csv_from_nickname(compressed_nick)
        csvfile.df_to_file(df)

    if get_month:
        df = get_compressed_data(DB,sid,240,180)
        print('    Compressed last month of data')
        compressed_nick = nick + '_month'
        csvfile = DataFile.csv_from_nickname(compressed_nick)
        csvfile.df_to_file(df)

    if get_year:
        df = get_compressed_data(DB,sid,2880,183)
        print('    Compressed last year of data')
        compressed_nick = nick + '_year'
        csvfile = DataFile.csv_from_nickname(compressed_nick)
        csvfile.df_to_file(df)

def main(verbose=False, 
         last_day=False,
         last_week=False,
         last_month=False,
         last_year=False,
         **kwargs):
    get_year = last_year
    if get_year:
        get_month = True
    else:
        get_month = last_month
    if get_month:
        get_week = True
    else:
        get_week = last_week
    if get_week:
        get_day = True
    else:
        get_day = last_day

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
    all_processes = []
    for sid, name, nick in zip(stations.index, stations['Name'],
                               stations['nickname']):
        print('(id={}) {}'.format(sid, name))
        p = multiprocessing.Process(target=make_station_files,
                                    args=(sid,nick,))
        p.start()
        all_processes.append(p)
        print()

    for p in all_processes:
        p.join()

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
