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
from datetime import datetime
import os
import time
start_time = time.time()
KEPLER_USERNAME = 'jccurtis'
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
    fname = '/home/dosenet/csv_dumps/{}.csv'.format(nick)
    df.to_csv(fname, index=None)
    print('    Saved all data to:', fname)
    cmd = 'scp '
    cmd += '{} '.format(fname)
    cmd += '{}@kepler.berkeley.edu:'.format(KEPLER_USERNAME)
    cmd += '/var/www/html/htdocs-nuc-groups/radwatch-7.32/sites/default/files/dosenet/'
    cmd += os.path.basename(fname)
    print(cmd)
    try:
        os.system(cmd)
        print('Successful SCP transfer to KEPLER ({})'.format(KEPLER_USERNAME))
    except Exception as e:
        print('Network Error: Cannot SCP to Kepler')
        raise e
    time.sleep(10)
print('Total run time: {:.2f} sec'.format(time.time() - start_time))
