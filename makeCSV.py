#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MYSQL to CSV writer.

2015-11-13
Joseph Curtis
Lawrence Berkeley National Laboratory
"""
from __future__ import print_function
from mysql.mysql_tools import SQLObject
import os
import time
start_time = time.time()
KEPLER_USERNAME = 'jccurtis'
webserver_csv_dir = '/var/www/html/htdocs-nuc-groups/radwatch-7.32/sites/default/files/dosenet/'

def get_webserver_csv_path(nickname):
    return os.path.join(webserver_csv_dir, get_csv_fname(nickname))

def get_csv_fname(nickname):
    return '{}.csv'.format(nickname)

def main():
    # -----------------------------------------------------------------------------
    # Mysql data base interface
    # -----------------------------------------------------------------------------
    DB = SQLObject()

    # -----------------------------------------------------------------------------
    # Pick active stations
    # -----------------------------------------------------------------------------
    stations = DB.getActiveStations()
    print(stations)

    for sid, name, nick in zip(stations.index, stations['Name'], stations['nickname']):
        print('(id={}) {}'.format(name, sid))
        df = DB.getLastYear(sid)
        fname = os.path.join('/home/dosenet/csv_dumps/', get_csv_fname(nick))
        df.to_csv(fname, index=None)
        print('    Saved all data to:', fname)
        cmd = 'scp '
        cmd += '{} '.format(fname)
        cmd += '{}@kepler.berkeley.edu:'.format(KEPLER_USERNAME)
        cmd += get_webserver_csv_path(nick)
        print(cmd)
        try:
            os.system(cmd)
            print('Successful SCP transfer to KEPLER ({})'.format(KEPLER_USERNAME))
        except Exception as e:
            print('Network Error: Cannot SCP to Kepler')
            raise e
    print('Total run time: {:.2f} sec'.format(time.time() - start_time))

if __name__ == "__main__":
    main()
