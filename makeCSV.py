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

def get_rounded_time(t):
    """
    set resolution for input time to nearest minute

    Input: time (sec) to be truncated
    Returns: truncated time (sec)
    """
    rounded_min = round(t.second/60)
    t = dt.datetime(t.year,t.hour,t.minute+rounded_min)
    return time.mktime(t.timetuple())

def get_compressed_data(DB,sid,integration_time,n_intervals):
    """
    get station data from the database for some number of time bins

    Args:
        arg1: database object
        arg2: station ID
        arg3: time bin (min) to average over
        arg4: number of time bins to retreive
    Returns:
        DataFrame with 3 time columns and 2 data columns:
            deviceTime_[utc, local, unix] cpm, cpmError
    """
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

def make_station_files(sid,name,nick,get_data):
    """
    generage all csv files for a station

    Args:
        arg1: station ID
        arg2: station Name
        arg3: station csv file nickname
        arg4: dictionary of booleans for which data ranges to retreive
            determined from command line arguments
    """
    print(get_data)
    DB = SQLObject()
    df = DB.getAll(sid)
    print('    Loaded raw data')
    csvfile = DataFile.csv_from_nickname(nick)
    csvfile.df_to_file(df)

    df = DB.getLastHour(sid)
    csvfile = DataFile.csv_from_nickname(nick+'_hour')
    csvfile.df_to_file(df)

    if get_data['get_day']:
        df = get_compressed_data(DB,sid,30,48)
        csvfile = DataFile.csv_from_nickname(nick + '_day')
        csvfile.df_to_file(df)

    if get_data['get_week']:
        df = get_compressed_data(DB,sid,60,168)
        csvfile = DataFile.csv_from_nickname(nick + '_week')
        csvfile.df_to_file(df)

    if get_data['get_month']:
        df = get_compressed_data(DB,sid,240,180)
        csvfile = DataFile.csv_from_nickname(nick + '_month')
        csvfile.df_to_file(df)

    if get_data['get_year']:
        df = get_compressed_data(DB,sid,2880,183)
        csvfile = DataFile.csv_from_nickname(nick + '_year')
        csvfile.df_to_file(df)

    print('    Loaded compressed data for (id={}) {}'.format(sid, name))

def main(verbose=False, 
         last_day=False,
         last_week=False,
         last_month=False,
         last_year=False,
         **kwargs):
    get_data = {'get_day': last_day or last_week or last_month or last_year,
                'get_week': last_week or last_month or last_year,
                'get_month': last_month or last_year,
                'get_year': last_year}

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
                                    args=(sid,name,nick,get_data,))
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
