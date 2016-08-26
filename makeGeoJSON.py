#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import geojson
from geojson import Point, Feature, FeatureCollection
import time
import datetime
from mysql.mysql_tools import SQLObject
from data_transfer import GeoJsonForWebserver, CsvForWebserver
from collections import OrderedDict

docstring = """
Main makeGeoJSON and transfer to KEPLER webserver 

Returns:
    Plot.ly graphs -
      Updates dose over time graphs on plot.ly for ALL stations
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
    2016-04-01
Originally adapted from dev_makeGeoJSON.py (functional) Sat 09/05/15
"""

def main(testing=False, verbose=False, **kwargs):
    start_time = time.time()
    # -------------------------------------------------------------------------
    # Open database tool
    # -------------------------------------------------------------------------
    DB = SQLObject()
    # -------------------------------------------------------------------------
    # Get dataframe of active stations
    # -------------------------------------------------------------------------
    active_stations = DB.getActiveStations()
    print(active_stations)
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
        if len(latest_data) == 0:
            continue
        dose_mrem = latest_data['cpmtorem'] * latest_data['cpm']
        dose_usv = latest_data['cpmtousv'] * latest_data['cpm']
        csvfile = CsvForWebserver.from_nickname(latest_data['nickname'])
        properties = OrderedDict([
            ('Name', latest_data['Name']),
            ('CPM', latest_data['cpm']),
            ('mREM/hr', dose_mrem),
            ('&microSv/hr', dose_usv),
            ('csv_location', csvfile.get_fname()),
            ('Latest measurement', str(latest_data['deviceTime_local']))])
        for k in ['deviceTime_unix', 'deviceTime_utc', 'deviceTime_local',
                  'receiveTime_unix', 'receiveTime_utc', 'receiveTime_local',
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
    geojsonfile = GeoJsonForWebserver.from_fname(**kwargs)
    geojsonfile.write_to_file(dump)
    # -------------------------------------------------------------------------
    # Transfer to webserver
    # -------------------------------------------------------------------------
    geojsonfile.send_to_webserver(testing=testing)
    # -------------------------------------------------------------------------
    # Finished!
    # -------------------------------------------------------------------------
    print('makeGeoJSON DONE, the time is {}'.format(datetime.datetime.now()))
    print('Total run time: {:.2f} s'.format(time.time() - start_time))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=docstring)
    parser.add_argument('-t', '--testing', action='store_true', default=False,
                        help='Testing mode to not send data to KEPLER')
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='Print more output')
    args = parser.parse_args()
    main(**vars(args))
