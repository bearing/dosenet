#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import geojson
from geojson import Point, Feature, FeatureCollection
import time
import datetime
from mysql_tools.mysql_tools import SQLObject
from data_transfer import DataFile, nickname_to_remote_csv_fname
from collections import OrderedDict

docstring = """
Main makeGeoJSON and transfer to KEPLER webserver 

Returns:
    output.geojson -
      GeoJSON file for the web page >> copied to Kepler (web server)
        via SCP (SSH CP) command

Run on DoseNet server!

Authors:
    2017-3-5 - Ali Hanks
    Joseph Curtis
    Navrit Bal
Project:
    DoseNet
Affiliation:
    Applied Nuclear Physics Division
    Lawrence Berkeley National Laboratory, Berkeley, U.S.A.
Created:
    Sun 2015-06-07
Last updated:
    2017-03-14
Originally adapted from dev_makeGeoJSON.py (functional) Sat 09/05/15
"""

def get_stations(DB,data_type):
    print('Getting active {} stations'.format(data_type))
    if data_type=='pocket':
        active_stations = DB.getActiveStations()
    elif data_type=='d3s':
        active_stations = DB.getActiveD3SStations()
    elif data_type=='aq':
        active_stations = DB.getActiveAQStations()
    elif data_type=='adc':
        active_stations = DB.getActiveADCStations()
    elif data_type=='weather':
        active_stations = DB.getActiveWeatherStations()
    print(active_stations)
    print()
    return active_stations

def get_data(DB,ix,data_type,old_data=0.0):
    if data_type=="pocket":
        data_df = DB.getLatestStationData(ix)
        if not data_df.empty:
            return data_df
        else:
            return None
    if data_type=="d3s":
        data_df = DB.getLatestD3SStationData(ix)
        if not data_df.empty:
            return data_df['counts']
        else:
            return None
    if data_type=="aq":
        data_df = DB.getLatestAQStationData(ix)
        if not data_df.empty:
            return data_df['PM25']
        else:
            return None
    if data_type=="adc":
        data_df = DB.getLatestADCStationData(ix)
        if not data_df.empty:
            return data_df['co2_ppm']
        else:
            return None
    if data_type=="weather":
        data_df = DB.getLatestWeatherStationData(ix)
        if not data_df.empty:
            return data_df
        else:
            return None

def main(verbose=False):
    start_time = time.time()
    # -------------------------------------------------------------------------
    # Open database tool
    # -------------------------------------------------------------------------
    DB = SQLObject()
    # -------------------------------------------------------------------------
    # Get dataframe of active stations
    # -------------------------------------------------------------------------
    active_stations = get_stations(DB,'pocket')
    d3s_stations = get_stations(DB,'d3s')
    aq_stations = get_stations(DB,'aq')
    adc_stations = get_stations(DB,'adc')
    w_stations = get_stations(DB,'weather')

    # -------------------------------------------------------------------------
    # Make geojson features and URLs for raw CSV data
    # -------------------------------------------------------------------------
    feature_list = []
    for ix in active_stations.index:
        # Builds a geojson point feature for the station location
        point = Point([active_stations.loc[ix, 'Long'],
                       active_stations.loc[ix, 'Lat']])
        # Get latest dose (CPM) and time to display in exported GeoJSON file
        if ix in active_stations.index.values:
            latest_data = get_data(DB,ix,"pocket")
            print('Station {}: CPM = {}'.format(ix,latest_data))

        if ix in d3s_stations.index.values:
            latest_d3s_data = get_data(DB,"d3s")
            print('Station {}: counts = {}'.format(ix,latest_d3s_data))

        if ix in aq_stations.index.values:
            latest_aq_data = get_data(DB,ix,"aq")
            print('Station {}: AQ = {}'.format(ix,latest_aq_data))

        if ix in adc_stations.index.values:
            latest_co2_data = get_data(DB,ix,"adc")
            print('Station {}: CO2 = {}'.format(ix,latest_co2_data))

        latest_t_data = None
        latest_h_data = None
        latest_p_data = None
        if ix in w_stations.index.values:
            temp_data = get_data(DB,ix,"weather")
            if not temp_data.empty:
                latest_t_data = temp_data['temperature']
                latest_h_data = temp_data['humidity']
                latest_p_data = temp_data['pressure']
                print('Station {}: temp = {}'.format(ix,latest_t_data))

        csv_fname = latest_data['nickname']
        properties = OrderedDict([
            ('Name', latest_data['Name']),
            ('CPM', latest_data['cpm']),
            ('counts', latest_d3s_data),
            ('co2_ppm', latest_co2_data),
            ('PM25', latest_aq_data),
            ('temperature', latest_t_data),
            ('humidity', latest_h_data),
            ('pressure', latest_p_data),
            ('csv_location', csv_fname),
            ('has_d3s', latest_d3s_data is not None),
            ('has_aq', latest_aq_data is not None),
            ('has_co2', latest_co2_data is not None),
            ('has_w', latest_t_data is not None),
            ('Latest measurement', str(latest_data['deviceTime_local']))])
        for k in ['deviceTime_unix', 'deviceTime_utc', 'deviceTime_local',
                  'timezone']:
            properties[k] = str(latest_data[k])
        feature_list.append(Feature(geometry=point, properties=properties))
    # -------------------------------------------------------------------------
    # Close database connection
    # -------------------------------------------------------------------------
    DB.close()
    # -------------------------------------------------------------------------
    # Convert geojson data to json string
    # -------------------------------------------------------------------------
    featureCollection = FeatureCollection(feature_list)
    dump = str(geojson.dumps(featureCollection))
    # -------------------------------------------------------------------------
    # Make geojson file
    # -------------------------------------------------------------------------
    geojsonfile = DataFile.default_geojson()
    geojsonfile.write_to_file(dump)
    # -------------------------------------------------------------------------
    # Finished!
    # -------------------------------------------------------------------------
    print('makeGeoJSON DONE, the time is {}'.format(datetime.datetime.now()))
    print('Total run time: {:.2f} s'.format(time.time() - start_time))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=docstring)
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='Print more output')
    args = parser.parse_args()
    main(**vars(args))
