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

# integration_time (min) = time to average over for condenced data
# n_intervals = number of intervals to collect (1 day = 60/integration_time * 24)
def get_compressed_data(DB,sid,column_list,integration_time,n_intervals):
    # get current time and set resolution to nearest minute
    t = dt.datetime.now()
    td = dt.timedelta(hours=t.hour, minutes=t.minute, seconds=t.second, microseconds=t.microsecond)
    to_min = dt.timedelta(minutes=round(current_td.total_seconds()/60))
    t = dt.datetime.combine(t,dt.time(0))+to_min
    
    # data interval for full day is 30 minutes
    interval = datetime.timedelta(minutes=integration_time).total_seconds()
    max_time = time.mktime(t.timetuple())
    new_df = pd.DataFrame(columns=column_list)
    for idx in range(n_intervals):
        min_time = max_time - interval
        df = DB.getDataForStationByRange(sid,max_time,min_time)
        count_total = df.loc[:,'cpm'].sum()*5
        cpm = count_total/integration_time
        cpm_error = math.sqrt(count_total)/integration_time
        new_df.loc[idx] = df.loc[len(df)/2].tolist()
        new_df.loc[idx,'cpm'] = cpm
        new_df.loc[idx,'cpmError'] = cpm_error
    return new_df

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
