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
import datetime
import socket
import numpy as np
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
HIGH_SQL = ' '.join((
    "SELECT * FROM dosimeter_network.dosnet",
    "WHERE (stationID = {}".format('{}'),
    "AND cpm > {}".format(HIGH_THRESH_CPM),
    "AND deviceTime > (NOW() - {}))".format(HIGH_INTERVAL_STR),
    "ORDER BY deviceTime DESC;"))
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
        self.initialize_station_status()
        self.post_initial_report()

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

    def initialize_station_status(self):
        """
        Initialize records in memory and check the SQL database
        for the first time.
        """

        self.get_db_data()
        self.update_station_status()

    def update_station_status(self):
        undeployed = []
        out = []
        high = []

        for stationID in self.stations.index.values:
            this_elapsed_time = self.get_elapsed_time(stationID)
            undeployed.append(this_elapsed_time is None)
            out.append(this_elapsed_time > OUTAGE_DURATION_THRESH_S)
            high.append(self.check_for_high_countrates(stationID))

        self.status = pd.DataFrame({
            'undeployed': undeployed,
            'out': out,
            'high': high,
            'ID': self.stations.index.values
        })
        self.status.set_index('ID', drop=True, inplace=True)

    def diff_status_and_report(self):
        """
        Check new station records and report any changes.
        """

        undeployed = []
        out = []
        high = []

        new_active_stations = []
        not_out = []
        not_high = []


        self.get_db_data()

        # 1. get the raw state from the database.
        for stationID in self.stations.index.values:
            if stationID not in self.status.index.values:
                new_active_stations.append(stationID)
            this_elapsed_time = self.get_elapsed_time(stationID)
            if this_elapsed_time is None:
                undeployed.append(stationID)
            if this_elapsed_time > OUTAGE_DURATION_THRESH_S:
                out.append(stationID)
            if self.check_for_high_countrates(stationID):
                high.append(stationID)

        # 2. compare with existing / previous Slacker status dataframe
        # 2a. previous outages
        prev_out = self.status[self.status['out']].index.values
        for stationID in prev_out:
            try:
                # we already know it's an outage, no need to report
                out.remove(stationID)
            except ValueError:
                # it used to be out, but no longer.
                not_out.append(stationID)
        # 2b. previous high countrate
        prev_high = self.status[self.status['high']].index.values
        for stationID in prev_high:
            try:
                high.remove(stationID)
            except ValueError:
                not_high.append(stationID)
        # 2c. previous undeployed
        prev_und = self.status[self.status['undeployed']].index.values
        for stationID in prev_und:
            try:
                undeployed.remove(stationID)
            except ValueError:
                new_active_stations.append(stationID)

        # 3. post report
        # 3a. all stations
        if len(out) == len(self.stations.index) - len(prev_out):
            assert (not not_out), 'Logic problem on all out!'
            self.post('Systemwide outage!!')
        # 3b. individual station outages
        else:
            self.post_each_station(out, 'down!')
        # 3c. individual stations back online
        self.post_each_station(not_out, 'back online.')
        # 3d. high countrate
        self.post_each_station(high, 'misbehaving with CPM > {}!'.format(
            HIGH_THRESH_CPM))
        # 3e. no more high countrate
        self.post_each_station(not_high, 'recovered from high CPM.')
        # 3f. new active stations
        self.post_each_station(
            new_active_stations, 'online for the first time!')

        # 4. update status
        self.update_station_status()

    def post_each_station(self, station_list, adj_text):
        """
        Post a generic message about each station in a list.
        """
        for stationID in station_list:
            msg = 'Station {} ({}) is {}}'.format(
                stationID,
                self.stations['Name'][stationID],
                adj_text)
            self.post(msg)

    def run(self):
        """Check SQL database, post messages. Blocks execution."""

        while True:
            time.sleep(self.interval_s)
            self.update_station_status()
            print('Posted at {}'.format(datetime.datetime.now()))

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

    def post_initial_report(self):
        """
        Compose a report based on the initial status as DoseNetSlacker starts.
        """

        n_out = np.sum(self.status['out'])
        n_high = np.sum(self.status['high'])
        n_undeployed = np.sum(self.status['undeployed'])

        header = ' '.join((
            '[DoseNet Slacker Startup Report]\n',
            'I see {} outages,'.format(n_out),
            '{} high countrates,'.format(n_high),
            'and {} undeployed stations.\n'.format(n_undeployed)
            ))

        outage_list = tuple(self.stations['Name'][self.status['out']])
        high_list = tuple(self.stations['Name'][self.status['high']])
        und_list = tuple(self.stations['Name'][self.status['undeployed']])
        outage_text = 'Outages: ' + ', '.join(outage_list) + '\n'
        high_text = 'High countrate: ' + ', '.join(high_list) + '\n'
        und_text = 'Undeployed: ' + ', '.join(und_list) + '\n'

        report_text = header + outage_text + high_text + und_text

        self.post(report_text)

    def post(self, msg_text, channel=SLACK_CHANNEL, icon_emoji=ICON):
        """
        Post a message on Slack. Defaults are filled in already
        """

        self.slack.api_call(
            'chat.postMessage',
            channel=channel,
            username=SLACK_USER,
            icon_emoji=icon_emoji,
            text=msg_text)

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
