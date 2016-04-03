#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import geojson
from geojson import Point, Feature, FeatureCollection
import time
import datetime
from mysql.mysql_tools import SQLObject
from makeCSV import get_webserver_csv_path

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

def main(testing=False, verbose=False, fname_geojson='output.geojson',
         kepler_username='jccurtis'):
    start_time = time.time()
    # -------------------------------------------------------------------------
    # Open database tool
    # -------------------------------------------------------------------------
    DB = SQLObject()
    # -------------------------------------------------------------------------
    # Get dataframe of active stations
    # Previously getStationInfo() & setStationInfoForAll()
    # -------------------------------------------------------------------------
    active_stations = DB.getActiveStations()
    print(active_stations)
    # -------------------------------------------------------------------------
    # Make geojson features and URLs for raw CSV data
    # Previously plotAll()
    # -------------------------------------------------------------------------
    feature_list = []
    for ix in active_stations.index:
        # Builds a geojson point feature for the station location
        point = Point([active_stations.loc[ix, 'Long'],
                       active_stations.loc[ix, 'Lat']])
        # Get latest dose (CPM) and time to display in exported GeoJSON file
        latest_data = DB.getLatestStationData(ix)
        dose_mrem = latest_data['cpmtorem'] * latest_data['cpm']
        dose_usv = latest_data['cpmtousv'] * latest_data['cpm']
        properties = {'Name': latest_data['Name'],
                      'Latest dose (CPM)': str(latest_data['cpm']),
                      'Latest dose (mREM/hr)': str(dose_mrem),
                      'Latest dose (&microSv/hr)': str(dose_usv),
                      'Latest measurement': str(latest_data['receiveTime']),
                      'CSV_LOCATION': get_webserver_csv_path(latest_data['nickname'])}
        feature_list.append(Feature(geometry=point, properties=properties))
    # -------------------------------------------------------------------------
    # Close database connection
    # Previously closeDB()
    # -------------------------------------------------------------------------
    DB.close()
    # -------------------------------------------------------------------------
    # Make geojson file
    # Previously makeGeoJSON()
    # -------------------------------------------------------------------------
    featureCollection = FeatureCollection(feature_list)
    dump = str(geojson.dumps(featureCollection))
    with open(fname_geojson, 'w') as infile:
        try:
            infile.write(dump)
        except Exception as e:
            print('Cannot write geosjson here:', fname_geojson)
            print(e)
    # -------------------------------------------------------------------------
    # Transfer to webserver (KEPLER)
    # Previously scpToWebServer()
    # Copy geojson file to webserver -> DECF Kepler
    # Must be run under 'dosenet' linux user so that the SSH keypair setup
    # between DOSENET & DECF Kepler works without login
    # Not ideal: uses Joseph Curtis' account (jccurtis) for the SCP
    # -------------------------------------------------------------------------
    command = "scp {} {}@{}:{}".format(
        fname_geojson,
        kepler_username,
        'kepler.berkeley.edu',
        '/var/www/html/htdocs-nuc-groups/radwatch-7.32/sites/default/files/')
    if testing:
        print('\nSCP CMD:\n{}\n'.format(command))
    else:
        try:
            os.system(command)
            print('Successful SCP transfer to KEPLER ({})'.format(kepler_username))
        except Exception as e:
            print('Network Error: Cannot SCP to Kepler')
            raise e
    # -------------------------------------------------------------------------
    # Finished!
    # Previously printEndMessage()
    # -------------------------------------------------------------------------
    print('makeGeoJSON DONE, the time is {}\n'.format(datetime.datetime.now()))
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
