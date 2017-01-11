"""
Requires official slackclient:
    pip install slackclient
    https://github.com/slackhq/python-slackclient

Requires token to be set for SLACK:
    This has been configured for the Slack Bot named
        dosenet_server

    https://api.slack.com/bot-users
    https://dosenet.slack.com/apps
"""

from __future__ import print_function
import os
import time
import socket
import pandas as pd
from slackclient import SlackClient
from mysql.mysql_tools import SQLObject

SLACK_USER = 'dosenet_server'
ICON = ':radioactive_sign:'
SLACK_CHANNEL = '#dosenet-bot-testing'
TOKEN_PATH = os.path.expanduser('~/')
TOKEN_NAME = 'ucbdosenet_slack_token.txt'

CHECK_INTERVAL_S = 5 * 60
HIGH_THRESH_CPM = 20
HIGH_INTERVAL_STR = 'INTERVAL 1 DAY'
HIGH_SQL = ' '.join(
    "SELECT * FROM dosimeter_network.dosnet",
    "WHERE (stationID = {}".format('{}'),
    "AND cpm > {}".format(HIGH_THRESH_CPM),
    "AND deviceTime > (NOW() - {}))".format(HIGH_INTERVAL_STR),
    "ORDER BY deviceTime DESC;")
OUTAGE_DURATION_THRESH_S = 1 * 60 * 60

MIN_STATION_ID = 1
MAX_STATION_ID = 9999

if socket.gethostname() != 'dosenet':
    raise RuntimeError('Unknown host {}, cannot connect to MySQL db'.format(
        socket.gethostname()))


class DoseNetSlacker(object):

    def __init__(self, tokenfile='~/ucbdosenet_slack_token.txt'):
        self.get_slack(tokenfile)
        self.get_sql()
        self.interval_s = CHECK_INTERVAL_S

    def get_slack(self, tokenfile):
        """Load slack token from file."""

        filename = os.path.join(TOKEN_PATH, TOKEN_NAME)
        with open(filename, 'r') as f:
            slack_token = f.read().rstrip()
        self.slack = SlackClient(slack_token)

    def get_sql(self):
        """Connect the MySQL database."""

        try:
            self.sql = SQLObject()
        except:     # MySQLdb/connections.py _mysql_exceptions.OperationalError
            print('Could not find SQL database! Starting without it')
            self.sql = None

    def run(self):
        """Check SQL database, post messages. Blocks execution."""

        return  # temp
        while True:
            self.get_db_data()
            for stationID in self.stations.index.values:
                pass


            time.sleep(self.interval_s)

    def get_db_data(self):
        """
        Read station data from SQL.
        """
        self.stations = self.sql.getActiveStations()

    def get_elapsed_time(self, stationID):
        """
        Check how long it's been since the device posted data.
        """

        df = self.sql.getLatestStationData(stationID)
        try:
            elapsed_time = time.time() - df['deviceTime_unix']
        except KeyError:
            # no station data
            elapsed_time = None

        return elapsed_time

    def check_for_high_countrates(self, stationID):
        """
        Look for active stations with countrate > xxx.
        """
        df = self.sql.dfFromSql(HIGH_SQL.format(stationID))
        is_high = len(df.index) > 0
        return is_high

    def check_for_new_stations(self, last_day):
        """
        Look for active stations that are posting for the first time.
        """
        pass


class DoseNetAlertSituation(object):
    """
    Represents a specific issue on a single station, as long as the issue
    persists. Gets deleted when issue is resolved.
    """

    pass


class StationOutage(DoseNetAlertSituation):
    """Single station that has older data, no longer has data."""
    pass


class NewStation(DoseNetAlertSituation):
    """Single station that has data for the first time ever."""
    pass


class HighCountrate(DoseNetAlertSituation):
    """Single station with data above a given countrate threshold."""
    pass


class FullOutage(DoseNetAlertSituation):
    """
    All deployed stations are not reporting.

    Could be an injector problem, or a device code bug.
    """
    pass



if __name__ == '__main__':
    slacker = DoseNetSlacker('./ucbdosenet_slack_token')
    print('Running DoseNetSlacker...')
    slacker.run()
