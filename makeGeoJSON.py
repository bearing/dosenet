#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import geojson
from geojson import Point, Feature, FeatureCollection
import time
import datetime
from mysql.mysql_tools import SQLObject
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
    Navrit Bal
    Joseph Curtis
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

def main(verbose=False):
    start_time = time.time()
    # -------------------------------------------------------------------------
    # Open database tool
    # -------------------------------------------------------------------------
    DB = SQLObject()
    # -------------------------------------------------------------------------
    # Get dataframe of active stations
    # -------------------------------------------------------------------------
    print('Getting active stations')
    active_stations = DB.getActiveStations()
    print(active_stations)
    print()

    print('Getting active d3s stations')
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
    # Make geojson features and URLs for raw CSV data
    # -------------------------------------------------------------------------
    feature_list = []
    for ix in active_stations.index:
        # Builds a geojson point feature for the station location
        point = Point([active_stations.loc[ix, 'Long'],
                       active_stations.loc[ix, 'Lat']])
        # Get latest dose (CPM) and time to display in exported GeoJSON file
        latest_data = DB.getLatestStationData(ix)
        has_d3s = ix in d3s_stations.index.values
        has_aq = ix in aq_stations.index.values
        has_co2 = ix in adc_stations.index.values
        has_w = ix in w_stations.index.values
        if len(latest_data) == 0:
            continue
        dose_mrem = 0.0036 * latest_data['cpm']
        dose_usv = 0.036 * latest_data['cpm']
        csv_fname = latest_data['nickname']
        properties = OrderedDict([
            ('Name', latest_data['Name']),
            ('CPM', latest_data['cpm']),
            ('mREM/hr', dose_mrem),
            ('&microSv/hr', dose_usv),
            ('csv_location', csv_fname),
            ('has_d3s', has_d3s),
            ('has_aq', has_aq),
            ('has_co2', has_co2),
            ('has_w', has_w),
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
