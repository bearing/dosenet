#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division
from myText_tools.mytext_tools import TextObject
from data_transfer import DataFile
import time
import datetime as dt
import pytz
import numpy as np
import math
import pandas as pd
import multiprocessing

docstring = """
Text to CSV writer.

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

def time_bin_data(df,sample_seconds):
    """
    uses the pandas resample method for applying the time binning
    - need to redo the time columns after this resample
    """
    df = df.copy()
    df.loc[:,'deviceTime_utc'] = df['deviceTime_unix']
    df.set_index(['deviceTime_utc'])
    df.index = pd.to_datetime(df.index, unit='s')
    df_binned = df.resample(str(sample_seconds)+"S", label='right').mean().reset_index()
    df_clean = df_binned.dropna()

    # Add back in the local time-zone column
    tz = pytz.timezone('America/Los_Angeles')
    df_clean.loc[:,'deviceTime_local'] = df_clean['deviceTime_utc']
    df_clean = df_clean[['deviceTime_utc','deviceTime_local','deviceTime_unix']+[c for c in df_clean if c not in ['deviceTime_utc','deviceTime_local','deviceTime_unix']]]

    df_final = df_clean.copy()
    for i in range(len(data1_final)):
        df_final.loc[:,'deviceTime_local'].iloc[i] = df_clean['deviceTime_unix'].iloc[i].replace(tzinfo=pytz.utc).astimezone(tz)    

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

def set_calibration(df,chCounts,all=False):
    if all:
        calib_array = np.ndarray(shape=(12,))
        for i in range(len(chCounts)//12):
            K_index = np.argmax(chCounts[i*12:(i+1)*12].sum(0)[500:700])+500
            if i==0:
                calib_array.fill(1460/K_index)
            else:
                temp = np.ndarray(shape=(12,))
                temp.fill(1460/K_index)
                calib_array = np.append(calib_array,temp)
        if len(calib_array) < len(chCounts):
            temp = np.ndarray(shape=(len(chCounts)-len(calib_array),))
            temp.fill(calib_array[-1])
            calib_array = np.append(calib_array,temp)
        df.insert(5,'keV_per_ch',pd.DataFrame(calib_array))
    else:
        K_index = np.argmax(chCounts.sum(0)[500:700])+500
        df.insert(5,'keV_per_ch',1460/K_index)
    return df

def format_d3s_data(df, all=False):
    """
    format raw d3s data from database to format for output csv files
    """
    df.insert(4,'cpmError',df['counts'].apply(lambda x: math.sqrt(x)/5))
    df['counts'] = df['counts']/5
    df.rename(columns = {'counts':'cpm'}, inplace = True)

    channel_array = np.array(
                    [get_channels(x,4) for x in df.loc[:,'channelCounts']])
    df = set_calibration(df,channel_array,all)
    df_channels = df['channelCounts'].apply(lambda x: get_channels(x,4))
    # convert one column of list of channel counts to ncolumns = nchannels
    df_channels = pd.DataFrame(
        data=np.array(df_channels.as_matrix().tolist()))
    # append to full df and remove original channelCount column
    del df['channelCounts']
    df = df.join(df_channels)
    return df

def get_channel_sum_data(df):
    """
    get the data frame of d3s over a specific period of time
    return the sum of all the each channel over a specific time
    """
    #print("received data fram")
    #print(df)
    channel_df = df.loc[:, "0":"1023"]
    df_sum = channel_df.sum(axis=0)
    array_sum = df_sum.to_numpy()
    return array_sum

def get_compressed_d3s_data(df,integration_time,n_intervals,verbose):
    """
    get d3s station data from the database for some number of time bins

    Args:
        df: DataFrame of data
        integration_time: time bin (min) to average over
        n_intervals: number of time bins to retreive
        verbose: sets verbosity for debugging
    Returns:
        DataFrame with 3 time columns and 2 data columns:
            deviceTime_[utc, local, unix] cpm, cpmError
    """
    interval = dt.timedelta(minutes=integration_time).total_seconds()
    print("interval for d3s: " + str(interval))
    max_time = df['deviceTime_unix'].iloc[0]
    comp_df = pd.DataFrame(columns=['deviceTime_UTC', 'deviceTime_local', 'deviceTime_unix', 'cpm', 'cpmError',
                                    'keV_per_ch', 'channels'])
    if verbose:
        print(comp_df)

    #comp_df = df.resample(str(rebin)+"S", label='right').mean().reset_index()

    for idx in range(n_intervals):
        idf = df[(df['deviceTime_unix']>(max_time-interval))&
                (df['deviceTime_unix']<(max_time))]
        max_time = max_time - interval
        if len(idf) > 0:
            """new method of calculating the channels """
            channels = get_channel_sum_data(idf)
            #print("the return of get_channel_sum_data(comp_df)")
            #print(channels)
            comp_df.loc[idx,'channels'] = channels
            print("total count of the channels: " + str(channels.sum()))
            counts = idf.loc[:, 'cpm'].sum()*5
            print("counts: " + str(counts))
            ndata = len(idf)
            comp_df.loc[idx, 'deviceTime_UTC'] = idf.iloc[ndata // 2, 0]
            comp_df.loc[idx, 'deviceTime_local'] = idf.iloc[ndata // 2, 1]
            comp_df.loc[idx, 'deviceTime_unix'] = idf.iloc[ndata // 2, 2]
            comp_df.loc[idx, 'cpm'] = counts/(len(idf)*5)
            comp_df.loc[idx, 'cpmError'] = math.sqrt(counts)/(len(idf)*5)
            K_index = np.argmax(channels[500:700])+500
            comp_df.loc[idx, 'keV_per_ch'] = 1460.0/K_index

    # convert one column of list of channel counts to ncolumns = nchannels
    df_channels = pd.DataFrame(
        data=np.array(comp_df['channels'].as_matrix().tolist()))
    # append to full df and remove original channelCount column
    del comp_df['channels']
    if verbose:
        print(comp_df)
    comp_df = comp_df.join(df_channels)
    return comp_df

def get_compressed_dosenet_data(df,integration_time,n_intervals,verbose):
    """
    get station data from the database for some number of time bins

    Args:
        df: DataFrame of data
        integration_time: time bin (min) to average over
        n_intervals: number of time bins to retreive
    Returns:
        DataFrame with 3 time columns and 2 data columns:
            deviceTime_[utc, local, unix] cpm, cpmError
    """
    interval = dt.timedelta(minutes=integration_time).total_seconds()
    max_time = df['deviceTime_unix'].iloc[0]
    comp_df = pd.DataFrame(columns=['deviceTime_UTC', 'deviceTime_local', 'deviceTime_unix','cpm','cpmError'])
    for idx in range(n_intervals):
        idf = df[(df['deviceTime_unix']>(max_time-interval))&
                (df['deviceTime_unix']<=(max_time))]

        max_time = max_time - interval
        ndata = len(idf)
        if ndata > 0:
            comp_df.loc[idx, 'deviceTime_UTC'] = idf.iloc[ndata // 2, 0]
            comp_df.loc[idx, 'deviceTime_local'] = idf.iloc[ndata // 2, 1]
            comp_df.loc[idx, 'deviceTime_unix'] = idf.iloc[ndata // 2, 2]
            counts = idf.loc[:,'cpm'].sum()*5
            comp_df.loc[idx,'cpm'] = counts/(ndata*5)
            comp_df.loc[idx,'cpmError'] = math.sqrt(counts)/(ndata*5)

    return comp_df

def get_compressed_aq_data(df,integration_time,n_intervals,verbose):
    """
    get station data from the database for some number of time bins

    Args:
        df: DataFrame of data
        integration_time: time bin (min) to average over
        n_intervals: number of time bins to retreive
    Returns:
        DataFrame with 3 time columns and 3 data columns:
            deviceTime_[utc, local, unix] pm1.0, pm2.5, pm10
    """
    interval = dt.timedelta(minutes=integration_time).total_seconds()
    max_time = df['deviceTime_unix'].iloc[2]
    comp_df = pd.DataFrame(columns=['deviceTime_UTC', 'deviceTime_local', 'deviceTime_unix','PM1','PM25','PM10'])
    for idx in range(n_intervals):
        idf = df[(df['deviceTime_unix'] >= (max_time-interval)) &
                (df['deviceTime_unix']<(max_time))]
        max_time = max_time - interval
        ndata = len(idf)
        if ndata > 0:
            comp_df.loc[idx, 'deviceTime_UTC'] = idf.iloc[ndata // 2, 0]
            comp_df.loc[idx, 'deviceTime_local'] = idf.iloc[ndata // 2, 1]
            comp_df.loc[idx,'deviceTime_unix'] = idf.iloc[ndata//2, 2]
            comp_df.loc[idx,'PM1'] = idf.loc[:,'PM1'].sum()/ndata
            comp_df.loc[idx,'PM25'] = idf.loc[:,'PM25'].sum()/ndata
            comp_df.loc[idx,'PM10'] = idf.loc[:,'PM10'].sum()/ndata

    return comp_df

def get_compressed_weather_data(df,integration_time,n_intervals,verbose):
    """
    get station data from the database for some number of time bins

    Args:
        df: DataFrame of data
        integration_time: time bin (min) to average over
        n_intervals: number of time bins to retreive
    Returns:
        DataFrame with 3 time columns and 3 data columns:
            deviceTime_[utc, local, unix] pm1.0, pm2.5, pm10
    """
    interval = dt.timedelta(minutes=integration_time).total_seconds()
    max_time = df['deviceTime_unix'].iloc[0]
    comp_df = pd.DataFrame(columns=['deviceTime_UTC', 'deviceTime_local', 'deviceTime_unix','temperature',
                                    'pressure','humidity'])
    for idx in range(n_intervals):
        idf = df[(df['deviceTime_unix']>(max_time-interval))&
                (df['deviceTime_unix']<(max_time))]
        max_time = max_time - interval
        ndata = len(idf)
        if ndata > 0:
            comp_df.loc[idx, 'deviceTime_UTC'] = idf.iloc[ndata // 2, 0]
            comp_df.loc[idx, 'deviceTime_local'] = idf.iloc[ndata // 2, 1]
            comp_df.loc[idx, 'deviceTime_unix'] = idf.iloc[ndata // 2, 2]
            comp_df.loc[idx,'temperature'] = idf.loc[:,'temperature'].sum()/ndata
            comp_df.loc[idx,'pressure'] = idf.loc[:,'pressure'].sum()/ndata
            comp_df.loc[idx,'humidity'] = idf.loc[:,'humidity'].sum()/ndata

    return comp_df

def get_compressed_adc_data(df,integration_time,n_intervals,verbose):
    """
    get station data from the database for some number of time bins

    Args:
        df: DataFrame of data
        integration_time: time bin (min) to average over
        n_intervals: number of time bins to retreive
    Returns:
        DataFrame with 3 time columns and 3 data columns:
            deviceTime_[utc, local, unix] pm1.0, pm2.5, pm10
    """
    interval = dt.timedelta(minutes=integration_time).total_seconds()
    max_time = df['deviceTime_unix'].iloc[0]
    comp_df = pd.DataFrame(columns=['deviceTime_UTC', 'deviceTime_local', 'deviceTime_unix', 'co2_ppm','noise'])
    for idx in range(n_intervals):
        idf = df[(df['deviceTime_unix']>(max_time-interval))&
                (df['deviceTime_unix']<(max_time))]
        max_time = max_time - interval
        ndata = len(idf)
        if ndata > 0:
            comp_df.loc[idx, 'deviceTime_UTC'] = idf.iloc[ndata // 2, 0]
            comp_df.loc[idx, 'deviceTime_local'] = idf.iloc[ndata // 2, 1]
            comp_df.loc[idx, 'deviceTime_unix'] = idf.iloc[ndata // 2, 2]
            comp_df.loc[idx,'co2_ppm'] = idf.loc[:,'co2_ppm'].sum()/ndata
            comp_df.loc[idx,'noise'] = idf.loc[:,'noise'].sum()/ndata

    return comp_df

def make_station_files(sid,name,nick,DB,get_data,data_path="",request_type=None,verbose=False):
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

    #ethan: getall? in text
    df_all = DB.getAll(sid,request_type,verbose)
    df_all = df_all[::-1]
    print("")
    print("Starting process for {}".format(name))
    print(df_all[0:10])

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

    intervals = [5,30,60,240]
    nintervals = [12,48,168,180]
    name_sufix = ['_hour','_day','_week','_month']
    if get_data['get_year']:
        intervals = [2880]
        nintervals = [183]
        name_sufix = ['_year']

    if len(df_all)==0:
        print("Warning: No data for {}".format(name))
        return

    if len(data_path) > 0:
        data_path = data_path + "dosenet/"
    for idx in range(len(intervals)):
        df = get_compressed_data(df_all,intervals[idx],nintervals[idx],verbose)
        csvfile = DataFile.csv_from_nickname(data_path+nick+name_sufix[idx])
        csvfile.df_to_file(df)

    print('    Loaded {} data for (id={}) {}'.format(request_type, sid, name))

def make_all_station_files(stations,get_data,db,data_path="",request_type=None,output_path="",verbose=False):
    for sid, name, nick in zip(stations.index, stations['Name'],
                               stations['nickname']):
        print('(id={}) {}'.format(sid, name))
        try:
            make_station_files(sid,name,nick,db,get_data,data_path,request_type,verbose)
        except Exception as e:
            print(e)
            print("ERROR: Failed to generate time data for {}".format(name))

def main(verbose=False,
         last_day=True,
         last_week=True,
         last_month=True,
         last_year=False,
         data_path=None,
         output_path=None,
         **kwargs):
    get_data = {'get_day': last_day,
                'get_week': last_week,
                'get_month': last_month,
                'get_year': last_year}

    start_time = time.time()
    # -------------------------------------------------------------------------
    # My text data base interface
    # -------------------------------------------------------------------------
    if not data_path:
        DB = TextObject()
    else:
        DB = TextObject(Data_Path=data_path)
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
    p = multiprocessing.Process(target=make_all_station_files,
                                args=(stations,get_data,DB,output_path,'dosenet',verbose))
    p.start()
    all_processes.append(p)

    p = multiprocessing.Process(target=make_all_station_files,
                                args=(aq_stations,get_data,DB,output_path,'aq',verbose))
    p.start()
    all_processes.append(p)

    p = multiprocessing.Process(target=make_all_station_files,
                                args=(adc_stations,get_data,DB,output_path,'adc',verbose))
    p.start()
    all_processes.append(p)

    for p in all_processes:
        p.join()

    # Run multiprocessing in two stages rather than spawning 5 processes
    all_processes = []

    p = multiprocessing.Process(target=make_all_station_files,
                                args=(w_stations,get_data,DB,output_path,'weather',verbose))
    p.start()
    all_processes.append(p)

    p = multiprocessing.Process(target=make_all_station_files,
                                args=(d3s_stations,get_data,DB,output_path,'d3s',verbose))

    for p in all_processes:
        p.join()


    print('Total run time: {:.2f} sec'.format(time.time() - start_time))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=docstring)
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='Print more output')
    parser.add_argument('-d', '--last-day', action='store_true', default=False,
                        help='get compressed csv for last day')
    parser.add_argument('-w', '--last-week', action='store_true', default=False,
                        help='get compressed csv for last week')
    parser.add_argument('-m', '--last-month', action='store_true', default=False,
                        help='get compressed csv for last month')
    parser.add_argument('-y', '--last-year', action='store_true', default=False,
                        help='get compressed csv for last year')
    parser.add_argument('-p', '--data_path', type=str, default=None)
    parser.add_argument('-o', '--output_path', type=str, default=None)
    args = parser.parse_args()
    main(**vars(args))
