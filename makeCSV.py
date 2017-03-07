#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from mysql.mysql_tools import SQLObject
from data_transfer import DataFile
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
def get_compressed_data(DB,sid,column_list,integration_time,n_intervals):
    interval = dt.timedelta(minutes=integration_time).total_seconds()
    max_time = get_rounded_time(dt.datetime.now())

    try:
        compressed_df = pd.DataFrame(columns=['deviceTime_unix','receiveTime_unix','cpm','cpmError'])
        for idx in range(n_intervals):
            proc_time = dt.datetime.now()
            min_time = max_time - interval
            df = DB.getDataForStationByRange(sid,min_time,max_time)
            cpm = df.loc[:,'cpm'].sum()*5/integration_time
            cpm_error = math.sqrt(df.loc[:,'cpm'].sum()*5)/integration_time

            compressed_df.loc[idx,'deviceTime_unix'] = df.loc[len(df)/2,'deviceTime_unix']
            compressed_df.loc[idx,'receiveTime_unix'] = df.loc[len(df)/2,'receiveTime_unix']
            compressed_df.loc[idx,'cpm'] = cpm
            compressed_df.loc[idx,'cpmError'] = cpm_error
            max_time = min_time
            proc_time = dt.datetime.now() - proc_time
            print('interval {} process time = {}'.format(idx,proc_time))

        print(compressed_df.columns.to_list())
        DB.addTimeColumnsToDataframe(compressed_df,sid)
        print(compressed_df.columns.to_list())
        return compressed_df
    except (Exception) as e:
        print(e)
        return pd.DataFrame({})

def main(verbose=False, **kwargs):
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

        df = get_compressed_data(DB,sid,df.columns.tolist(),30,48)
        print('    Compressed last day of data')
        compressed_nick = nick + '_day'
        csvfile = DataFile.csv_from_nickname(compressed_nick)
        csvfile.df_to_file(df)

        df = get_compressed_data(DB,sid,df.columns.tolist(),60,168)
        print('    Compressed last week of data')
        compressed_nick = nick + '_week'
        csvfile = DataFile.csv_from_nickname(compressed_nick)
        csvfile.df_to_file(df)

        df = get_compressed_data(DB,sid,df.columns.tolist(),240,180)
        print('    Compressed last month of data')
        compressed_nick = nick + '_month'
        csvfile = DataFile.csv_from_nickname(compressed_nick)
        csvfile.df_to_file(df)

        df = get_compressed_data(DB,sid,df.columns.tolist(),2880,183)
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
    args = parser.parse_args()
    main(**vars(args))
