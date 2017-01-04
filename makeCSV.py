#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from mysql.mysql_tools import SQLObject
from data_transfer import DataFile
import time

docstring = """
MYSQL to CSV writer.

2015-11-13
Joseph Curtis
Lawrence Berkeley National Laboratory
"""

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
    # Pull last year of data for each station, save to CSV and transfer
    # -------------------------------------------------------------------------
    for sid, name, nick in zip(stations.index, stations['Name'],
                               stations['nickname']):
        print('(id={}) {}'.format(sid, name))
        df = DB.getLastYear(sid)
        print('    Loaded last year of data')
        csvfile = DataFile.csv_from_nickname(nick)
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
