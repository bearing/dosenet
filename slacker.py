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
COUNTRATE_THRESHOLD_CPM = 20
OUTAGE_DURATION_THRESHOLD_S = 1 * 60 * 60

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

        while True:
            self.get_db_data()
            for stationID in self.stations.index.values:
                this_last_day = self.sql.getLastDay(stationID)
                this_last_hour = self.sql.getDataForStationByInterval(
                    stationID, 'INTERVAL 1 HOUR')
                this_out = self.check_for_outages(this_last_day)
                this_high = self.check_for_high_countrates(this_last_day)
                this_new = self.check_for_new_stations(this_last_day)

            self.slack.api_call(
                'chat.postMessage',
                channel=SLACK_CHANNEL,
                username=SLACK_USER,
                icon_emoji=ICON,
                text='Testing')
            time.sleep(self.interval_s)

    def get_db_data(self):
        """
        Read station data from SQL.
        """
        self.stations = self.sql.getActiveStations()

    def check_for_outages(self, last_day):
        """
        Look for active stations that haven't posted data in the last ... time.
        """
        if len(last_day.index) > 0:
            pass
        return out

    def check_for_high_countrates(self, last_day):
        """
        Look for active stations with countrate > xxx.
        """
        pass

    def check_for_new_stations(self, last_day):
        """
        Look for active stations that are posting for the first time.
        """
        pass


if __name__ == '__main__':
    slacker = DoseNetSlacker('./ucbdosenet_slack_token')
    print('Running DoseNetSlacker...')
    slacker.run()
