#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from mysql.mysql_tools import SQLObject
from data_transfer import DataFile
import time
import datetime as dt
import numpy as np
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

def rebin(array,n):
    """
    rebin a 1d numpy array by n (should force to be factor of array length)
    """
    return array.reshape(len(array)/n,n).sum(1)

def get_channels(channels,rebin_factor):
    """
    convert channel counts binary string from the database into a numpy array
    and rebin by rebin_factor
    """
    full_array = np.fromstring(channels,dtype=np.uint8)
    rebin_array = rebin(full_array,rebin_factor)
    return rebin_array

def get_rounded_time(t):
    """
    set resolution for input time to nearest minute

    Input: datetime oject to be truncated
    Returns: truncated time oject
    """
    rounded_min = int(round(t.second/60.0))
    t = dt.datetime(t.year,t.month,t.day,t.hour,t.minute+rounded_min)
    return time.mktime(t.timetuple())

def format_d3s_data(df):
    """
    format raw d3s data from database to format for output csv files
    """
    df.insert(4,'cpmError',df['counts'].apply(lambda x: math.sqrt(x)/5))
    df['counts'] = df['counts']/5
    df.rename(columns = {'counts':'cpm'}, inplace = True)

    df_channels = df['channelCounts'].apply(lambda x: get_channels(x,4))
    # convert one column of list of channel counts to ncolumns = nchannels
    df_channels = pd.DataFrame(
        data=np.array(df_channels.as_matrix().tolist()))
    # append to full df and remove original channelCount column
    del df['channelCounts']
    df = df.join(df_channels)
    return df

def get_compressed_d3s_data(DB,sid,integration_time,n_intervals):
    """
    get d3s station data from the database for some number of time bins

    Args:
        DB: database object
        sid: station ID
        integration_time: time bin (min) to average over
        n_intervals: number of time bins to retreive
    Returns:
        DataFrame with 3 time columns and 2 data columns:
            deviceTime_[utc, local, unix] cpm, cpmError
    """
    interval = dt.timedelta(minutes=integration_time).total_seconds()
    max_time = get_rounded_time(dt.datetime.now())

    comp_df = pd.DataFrame(columns=['deviceTime_unix',
                                    'cpm','cpmError',
                                    'channels'])
    for idx in range(n_intervals):
        df = DB.getD3SDataForStationByRange(sid,max_time - interval,max_time)
        max_time = max_time - interval
        if len(df) > 0:
            comp_df.loc[idx,'channels'] = np.array(
                [get_channels(x,4) for x in df.loc[:,'channelCounts']]).sum(0)
            counts = df.loc[:,'counts'].sum()
            comp_df.loc[idx,'deviceTime_unix'] = df.iloc[len(df)/2,0]
            comp_df.loc[idx,'cpm'] = counts/(len(df)*5)
            comp_df.loc[idx,'cpmError'] = math.sqrt(counts)/(len(df)*5)

    # convert one column of list of channel counts to ncolumns = nchannels
    df_channels = pd.DataFrame(
        data=np.array(comp_df['channels'].as_matrix().tolist()))
    # append to full df and remove original channelCount column
    del comp_df['channels']
    comp_df = comp_df.join(df_channels)
    comp_df = DB.addTimeColumnsToDataframe(comp_df,sid)
    return comp_df

def get_compressed_dosenet_data(DB,sid,integration_time,n_intervals):
    """
    get station data from the database for some number of time bins

    Args:
        DB: database object
        sid: station ID
        integration_time: time bin (min) to average over
        n_intervals: number of time bins to retreive
    Returns:
        DataFrame with 3 time columns and 2 data columns:
            deviceTime_[utc, local, unix] cpm, cpmError
    """
    interval = dt.timedelta(minutes=integration_time).total_seconds()
    max_time = get_rounded_time(dt.datetime.now())

    comp_df = pd.DataFrame(columns=['deviceTime_unix','cpm','cpmError'])
    for idx in range(n_intervals):
        df = DB.getDataForStationByRange(sid,max_time - interval,max_time)
        max_time = max_time - interval
        if len(df) > 0:
            counts = df.loc[:,'cpm'].sum()*5
            comp_df.loc[idx,'deviceTime_unix'] = df.iloc[len(df)/2,0]
            comp_df.loc[idx,'cpm'] = counts/(len(df)*5)
            comp_df.loc[idx,'cpmError'] = math.sqrt(counts)/(len(df)*5)

    comp_df = DB.addTimeColumnsToDataframe(comp_df,sid)
    return comp_df

def get_compressed_aq_data(DB,sid,integration_time,n_intervals):
    """
    get station data from the database for some number of time bins

    Args:
        DB: database object
        sid: station ID
        integration_time: time bin (min) to average over
        n_intervals: number of time bins to retreive
    Returns:
        DataFrame with 3 time columns and 3 data columns:
            deviceTime_[utc, local, unix] pm1.0, pm2.5, pm10
    """
    interval = dt.timedelta(minutes=integration_time).total_seconds()
    max_time = get_rounded_time(dt.datetime.now())

    comp_df = pd.DataFrame(columns=['deviceTime_unix','PM1','PM25','PM10'])
    for idx in range(n_intervals):
        df = DB.getAQDataForStationByRange(sid,max_time - interval,max_time)
        max_time = max_time - interval
        if len(df) > 0:
            comp_df.loc[idx,'deviceTime_unix'] = df.iloc[len(df)/2,0]
            comp_df.loc[idx,'PM1'] = df.loc[:,'PM1'].sum()/len(df)
            comp_df.loc[idx,'PM25'] = df.loc[:,'PM25'].sum()/len(df)
            comp_df.loc[idx,'PM10'] = df.loc[:,'PM10'].sum()/len(df)

    comp_df = DB.addTimeColumnsToDataframe(comp_df,sid)
    return comp_df

def get_compressed_weather_data(DB,sid,integration_time,n_intervals):
    """
    get station data from the database for some number of time bins

    Args:
        DB: database object
        sid: station ID
        integration_time: time bin (min) to average over
        n_intervals: number of time bins to retreive
    Returns:
        DataFrame with 3 time columns and 3 data columns:
            deviceTime_[utc, local, unix] pm1.0, pm2.5, pm10
    """
    interval = dt.timedelta(minutes=integration_time).total_seconds()
    max_time = get_rounded_time(dt.datetime.now())

    comp_df = pd.DataFrame(columns=['deviceTime_unix','temperature',
                                    'pressure','humidity'])
    for idx in range(n_intervals):
        df = DB.getWeatherDataForStationByRange(sid,max_time - interval,max_time)
        max_time = max_time - interval
        if len(df) > 0:
            comp_df.loc[idx,'deviceTime_unix'] = df.iloc[len(df)/2,0]
            comp_df.loc[idx,'temperature'] = df.loc[:,'temperature'].sum()/len(df)
            comp_df.loc[idx,'pressure'] = df.loc[:,'pressure'].sum()/len(df)
            comp_df.loc[idx,'humidity'] = df.loc[:,'humidity'].sum()/len(df)

    comp_df = DB.addTimeColumnsToDataframe(comp_df,sid)
    return comp_df

def get_compressed_adc_data(DB,sid,integration_time,n_intervals):
    """
    get station data from the database for some number of time bins

    Args:
        DB: database object
        sid: station ID
        integration_time: time bin (min) to average over
        n_intervals: number of time bins to retreive
    Returns:
        DataFrame with 3 time columns and 3 data columns:
            deviceTime_[utc, local, unix] pm1.0, pm2.5, pm10
    """
    interval = dt.timedelta(minutes=integration_time).total_seconds()
    max_time = get_rounded_time(dt.datetime.now())

    comp_df = pd.DataFrame(columns=['deviceTime_unix','co2_ppm','noise'])
    for idx in range(n_intervals):
        df = DB.getADCDataForStationByRange(sid,max_time - interval,max_time)
        max_time = max_time - interval
        if len(df) > 0:
            comp_df.loc[idx,'deviceTime_unix'] = df.iloc[len(df)/2,0]
            comp_df.loc[idx,'co2_ppm'] = df.loc[:,'co2_ppm'].sum()/len(df)
            comp_df.loc[idx,'noise'] = df.loc[:,'noise'].sum()/len(df)

    comp_df = DB.addTimeColumnsToDataframe(comp_df,sid)
    return comp_df

def make_station_files(sid,name,nick,get_data,request_type=None):
    """
    generage all csv files for a station

    Args:
        sid: station ID
        name: station Name
        nick: station csv file nickname
        get_data: dictionary of booleans for which data ranges to retreive
            determined from command line arguments
        request type: specify sensor (silicon,d3s,etc)
    """
    DB = SQLObject()

    if request_type == 'd3s':
        get_compressed_data = get_compressed_d3s_data
        nick = nick + '_d3s'
    elif request_type == 'aq':
        get_compressed_data = get_compressed_aq_data
        nick = nick + '_aq'
    elif request_type == 'weather':
        get_compressed_data = get_compressed_weather_data
        nick = nick + '_weather'
    elif request_type == 'adc':
        get_compressed_data = get_compressed_adc_data
        nick = nick + '_adc'
    elif request_type == 'dosenet':
        get_compressed_data = get_compressed_dosenet_data
    else:
        print('No data-type specified')
        return None

    df = DB.getAll(sid,request_type)
    if request_type == 'd3s':
        df = format_d3s_data(df)
    csvfile = DataFile.csv_from_nickname(nick)
    csvfile.df_to_file(df)

    df = DB.getLastHour(sid,request_type)
    if request_type == 'd3s':
        df = format_d3s_data(df)
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

    print('    Loaded {} data for (id={}) {}'.format(request_type, sid, name))

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
    print('Getting active stations')
    stations = DB.getActiveStations()
    print(stations)
    print()

    print('Getting active D3S stations')
    d3s_stations = DB.getActiveD3SStations()
    print(d3s_stations)
    print()

    print('Getting active air quality stations')
    aq_stations = DB.getActiveAQStations()
    print(aq_stations)
    print()

    print('Getting active CO2 stations')
    adc_stations = DB.getActiveADCStations()
    print(adc_stations)
    print()

    print('Getting active weather stations')
    w_stations = DB.getActiveWeatherStations()
    print(w_stations)
    print()
    # -------------------------------------------------------------------------
    # Pull data for each station, save to CSV and transfer
    # -------------------------------------------------------------------------
    all_processes = []
    for sid, name, nick in zip(stations.index, stations['Name'],
                               stations['nickname']):
        print('(id={}) {}'.format(sid, name))
        p = multiprocessing.Process(target=make_station_files,
                                    args=(sid,name,nick,get_data,'dosenet'))
        p.start()
        all_processes.append(p)
        print()

    for sid, name, nick in zip(d3s_stations.index, d3s_stations['Name'],
                               d3s_stations['nickname']):
        print('(id={}) {}'.format(sid, name))
        p = multiprocessing.Process(target=make_station_files,
                                    args=(sid,name,nick,get_data,'d3s'))
        p.start()
        all_processes.append(p)

    for sid, name, nick in zip(aq_stations.index, aq_stations['Name'],
                               aq_stations['nickname']):
        print('(id={}) {}'.format(sid, name))
        p = multiprocessing.Process(target=make_station_files,
                                    args=(sid,name,nick,get_data,'aq'))
        p.start()
        all_processes.append(p)

    for sid, name, nick in zip(w_stations.index, w_stations['Name'],
                               w_stations['nickname']):
        print('(id={}) {}'.format(sid, name))
        p = multiprocessing.Process(target=make_station_files,
                                    args=(sid,name,nick,get_data,'weather'))
        p.start()
        all_processes.append(p)

    for sid, name, nick in zip(adc_stations.index, adc_stations['Name'],
                               adc_stations['nickname']):
        print('(id={}) {}'.format(sid, name))
        p = multiprocessing.Process(target=make_station_files,
                                    args=(sid,name,nick,get_data,'adc'))
        p.start()
        all_processes.append(p)

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
