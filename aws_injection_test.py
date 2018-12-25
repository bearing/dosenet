#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File to test connection to AWS RDS MySQL instances.
This file also serves as an example of how to communicate
with the AWS instance.

Unlike the rest of this repo, this file requires Python 3
to work (this is reflected in the shebang).

Author:
    Sagnik Bhattacharya
"""

from mysql.connector import MySQLConnection

from time import time
from datetime import datetime

USER = 'root'  # set while creating the instance
HOST = 'dosenet-55.cork9lvwvd2g.us-west-1.rds.amazonaws.com'  # obtained AFTER creating the instance
PORT = 3306  # default value (can be changed while creating the instance)
PASSWORD = 'dosedose'  # set while creating the instance
DATABASE = 'dosenet'   # set while creating the instance

def connection_to_remote_db():
    """Returns a connection to the remote database."""
    return MySQLConnection(user=USER, host=HOST, port=PORT,
                           password=PASSWORD, database=DATABASE)


def main():
    t = time()
    device_time = datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')
    receive_time = datetime.fromtimestamp(t + 20).strftime('%Y-%m-%d %H:%M:%S')
    station_id = 0
    pm1 = 50.0
    pm25 = 1000.0
    pm10 = 10000.0  # wonder what the AQI is XD
    error_flag = 0

    cnx = connection_to_remote_db()
    cur = cnx.cursor(buffered=True)

    print('These are the last five rows BEFORE injection.')
    cur.execute('SELECT * FROM air_quality ORDER BY receiveTime DESC LIMIT 5;')
    for row in cur.fetchall():
        print(row)

    command = 'INSERT INTO air_quality VALUES ("{}", "{}", {}, {}, {}, {}, {});'.format(
        device_time, receive_time, station_id, pm1, pm25, pm10, error_flag
    )
    print('\n\nCommand to be executed:\n' + command)
    cur.execute(command)

    cnx.commit()

    print('\n\nThese are the last five rows AFTER injection.')
    cur.execute('SELECT * FROM air_quality ORDER BY receiveTime DESC LIMIT 5;')
    for row in cur.fetchall():
        print(row)

    cur.close()
    cnx.close()


if __name__ == '__main__':
    main()
