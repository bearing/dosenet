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
import argparse
import os
import time
import datetime
import socket
import numpy as np
import pandas as pd
import subprocess
import traceback as tb
from slackclient import SlackClient
from mysql.mysql_tools import SQLObject
import MySQLdb as mdb

SLACK_USER = 'dosenet_server'
SLACK_CHANNEL = '#bugs_and_outages'
# SLACK_CHANNEL = '#dosenet-bot-testing'
TOKEN_PATH = os.path.expanduser('~/')
TOKEN_NAME = 'ucbdosenet_slack_token.txt'

ICONS = {
    'startup': ':radioactive_sign:',
    'out': ':confused:',
    'high': ':scream:',
    'new_active': ':sunglasses:',
    'not_out': ':joy:',
    'not_high': ':sweat_smile:',
    'all_out': ':weary:'
}

CHECK_INTERVAL_S = 2 * 60
TEST_CHECK_INTERVAL_S = 60
HIGH_THRESH_CPM = 20
HIGH_INTERVAL_STR = 'INTERVAL 1 WEEK'
HIGH_SQL = ' '.join((
    "SELECT * FROM dosimeter_network.dosnet",
    "WHERE (stationID = {}".format('{}'),
    "AND cpm > {}".format(HIGH_THRESH_CPM),
    "AND deviceTime > (NOW() - {}))".format(HIGH_INTERVAL_STR),
    "ORDER BY deviceTime DESC;"))
OUTAGE_DURATION_THRESH_S = 6 * 60 * 60
ALMOST_OUT_DURATION_THRESH_S = 6 * 60
TEST_OUTAGE_DURATION_THRESH_S = 300

MIN_STATION_ID = 1
MAX_STATION_ID = 9999

INJECTOR_CMD = ('bash', '/home/dosenet/git/dosenet/start-injector-in-tmux.sh')

MIN_STATION_ID = 1
MAX_STATION_ID = 9999

if socket.gethostname() != 'dosenet':
    raise RuntimeError('Unknown host {}, cannot connect to MySQL db'.format(
        socket.gethostname()))


class DoseNetSlacker(object):

    def __init__(self, tokenfile='~/ucbdosenet_slack_token.txt',
                 test=False, verbose=False, restart_injector=False):
        self.test = test
        self.v = verbose
        self.restart_injector = restart_injector
        self.get_slack(tokenfile)
        self.get_sql()
        if self.test:
            self.interval_s = TEST_CHECK_INTERVAL_S
            self.outage_interval_s = TEST_OUTAGE_DURATION_THRESH_S
            self.almost_out_interval_s = TEST_OUTAGE_DURATION_THRESH_S / 2
            print('Starting DoseNetSlacker in test mode: ' +
                  'check interval {}s, outage interval {}s'.format(
                      self.interval_s, self.outage_interval_s))
        else:
            self.interval_s = CHECK_INTERVAL_S
            self.outage_interval_s = OUTAGE_DURATION_THRESH_S
            self.almost_out_interval_s = ALMOST_OUT_DURATION_THRESH_S
            if self.v:
                print('Check interval: {}s'.format(self.interval_s))
                print('Outage interval: {}s'.format(self.outage_interval_s))
        self.initialize_station_status()
        self.post_initial_report()
        print('Posted initial report at {}'.format(datetime.datetime.now()))

    def get_slack(self, tokenfile):
        """Load slack token from file."""

        filename = os.path.join(TOKEN_PATH, TOKEN_NAME)
        if self.v:
            print('Loading Slack token from {}'.format(filename))
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

        self.get_current_station_list()
        self.update_station_status()

    def get_current_station_list(self):
        """
        Get station list from SQL.
        """
        self.sql.refresh()
        if self.test:
            self.stations = self.sql.getStations()
        else:
            self.stations = self.sql.getActiveStations()

    def get_status(self):
        """Check elapsed times and classify each station.

        Returns a status dataframe."""

        undeployed = []
        out = []
        almost_out = []
        high = []

        for stationID in self.stations.index.values:
            this_elapsed_time = self.get_elapsed_time(stationID)
            undeployed.append(this_elapsed_time is None)
            out.append(this_elapsed_time > self.outage_interval_s)
            almost_out.append(this_elapsed_time > self.almost_out_interval_s)
            high.append(self.check_for_high_countrates(stationID))

        status = pd.DataFrame({
            'undeployed': undeployed,
            'out': out,
            'almost': almost_out,
            'high': high,
            'ID': self.stations.index.values
        })
        status.set_index('ID', drop=True, inplace=True)

        if self.v:
            print('\nStatus Dataframe:')
            print(status)
            print('\n')

        return status

    def update_station_status(self):
        """Get status and save into self.stations"""

        self.status = self.get_status()

    def get_elapsed_time(self, stationID):
        """
        Check how long it's been since the device posted data.
        """

        self.sql.refresh()
        try:
            df = self.sql.getLatestStationData(stationID, verbose=False)
        except IndexError:
            # on SQLObject.getTimezoneFromID
            # posted in Slack 3/24/2017 at 1:46pm
            # try to replicate and debug
            q = "SELECT timezone FROM stations WHERE `ID` = {};".format(
                stationID)
            tz = self.rawSql(q)
            msg = '\n'.join(
                'IndexError in SQLObject.getTimezoneFromID',
                'q = {}'.format(q),
                'tz = self.rawSql(q) = {}'.format(tz))
            slacker.post('Exception: {}'.format(msg))

        try:
            elapsed_time = time.time() - df['deviceTime_unix']
        except KeyError:
            # no station data
            elapsed_time = None

        return elapsed_time

    def run(self):
        """Check SQL database, post messages. Blocks execution."""

        while True:
            time.sleep(self.interval_s)
            try:
                self.diff_status_and_report()
            except mdb.OperationalError:
                self.post('MySQL server has gone away... will try again',
                          icon_emoji=':disappointed:')
            print('Posted at {}'.format(datetime.datetime.now()))

    def diff_status_and_report(self):
        """
        Find differences between current status and previous status.
        """

        new = self.get_status()
        cur = new.copy()
        prev = self.status

        cur.rename(columns=lambda x: 'new_' + x, inplace=True)
        both = pd.concat([prev, cur], axis=1)

        enabled = pd.isnull(both['out']) & pd.notnull(both['new_out'])
        disabled = pd.notnull(both['out']) & pd.isnull(both['new_out'])

        new_out = ~both['out'].dropna() & both['new_out'].dropna()
        not_out = both['out'].dropna() & ~both['new_out'].dropna()
        new_high = ~both['high'].dropna() & both['new_high'].dropna()
        not_high = both['high'].dropna() & ~both['new_high'].dropna()

        enabled_inactive = enabled & both['new_undeployed'].dropna()
        enabled_active = enabled & ~both['new_undeployed'].dropna()
        new_active = (both['undeployed'].dropna() &
                      ~both['new_undeployed'].dropna())
        new_almost = ~both['out'].dropna() & both['new_almost'].dropna()

        # all stations out - server problem - but ignore at midnight
        now = datetime.datetime.now()
        not_midnight = not (now.hour == 0 and now.minute < 10)
        if np.all(both['new_almost'].dropna() |
                  both['new_undeployed'].dropna()) and not_midnight:
            if self.restart_injector:
                print('Restarting injector...')
                msg = '*_Systemwide outage!!_* Restarting injector...'
                subprocess.call(INJECTOR_CMD)
            else:
                msg = '*_Systemwide outage!!_*'
            self.post(msg, icon_emoji=ICONS['all_out'])

        self.report_one_condition(
            new_out, 'down!', icon_emoji=ICONS['out'])
        self.report_one_condition(
            not_out, 'back online.', icon_emoji=ICONS['not_out'])
        self.report_one_condition(
            new_high, 'misbehaving with CPM > {}!'.format(HIGH_THRESH_CPM),
            icon_emoji=ICONS['high'])
        self.report_one_condition(
            not_high, 'recovered from high CPM.', icon_emoji=ICONS['not_high'])
        self.report_one_condition(
            new_active, 'online for the first time!',
            icon_emoji=ICONS['new_active'])

        self.status = new

    def report_one_condition(self, b, message_text, icon_emoji=None):
        """
        Post a message to Slack about one condition for one or more stations.
        """

        if icon_emoji is None:
            icon_emoji = ''

        id_list = b[b].index.tolist()
        name_list = [self.stations['Name'][this_id] for this_id in id_list]

        n = np.sum(b)
        assert(n == len(id_list))

        if n == 1:
            message = '{} {} (#{}) is {}'.format(
                icon_emoji,
                name_list[0],
                id_list[0],
                message_text)
        elif n > 1:
            message = '{} {} stations are {}:'.format(
                icon_emoji,
                n,
                message_text)
            for i in xrange(n):
                message += '\n{} (#{})'.format(
                    name_list[i],
                    id_list[i])
        else:
            return None

        self.post(message, channel=SLACK_CHANNEL)

        return None

    def check_for_high_countrates(self, stationID):
        """
        Look for active stations with countrate > xxx.
        """
        self.sql.refresh()
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
            '*[Summary Report]*\n',
            'I see {} outages,'.format(n_out),
            '{} high countrates,'.format(n_high),
            'and {} undeployed stations.\n'.format(n_undeployed)
        ))

        outage_list = tuple(self.stations['Name'][self.status['out']])
        high_list = tuple(self.stations['Name'][self.status['high']])
        und_list = tuple(self.stations['Name'][self.status['undeployed']])
        outage_text = '*Outages:* ' + ', '.join(outage_list) + '\n'
        high_text = '*High countrate:* ' + ', '.join(high_list) + '\n'
        und_text = '*Undeployed:* ' + ', '.join(und_list) + '\n'

        report_text = header
        if outage_list:
            report_text += outage_text
        if high_list:
            report_text += high_text
        if und_list:
            report_text += und_text

        self.post(report_text, icon_emoji=ICONS['startup'])

    def post_each_station(self, station_list, adj_text, icon_emoji=None):
        """
        Post a generic message about each station in a list.
        """
        for stationID in station_list:
            msg = 'Station {} ({}) is {}'.format(
                stationID,
                self.stations['Name'][stationID],
                adj_text)
            self.post(msg, icon_emoji=icon_emoji)

    def post(self, msg_text, channel=SLACK_CHANNEL,
             icon_emoji=None):
        """
        Post a message on Slack. Defaults are filled in already
        """

        if icon_emoji is None:
            icon_emoji = ':radioactive_sign:'

        keep_trying = True
        n_tries = 0
        while keep_trying and n_tries < 5:
            try:
                self.slack.api_call(
                    'chat.postMessage',
                    channel=channel,
                    username=SLACK_USER,
                    icon_emoji=icon_emoji,
                    text=msg_text)
            except ValueError as e:
                if 'JSON' in e.__str__():
                    print('JSON error in slack module!')
                time.sleep(5)
                n_tries += 1
            else:
                keep_trying = False

    def get_db_data(self):
        """
        Read station data from SQL.
        """
        # self.stations = self.sql.getActiveStations()
        self.stations = self.sql.getStations()

    def get_elapsed_time(self, stationID):
        """
        Check how long it's been since the device posted data.
        """

        df = self.sql.getLatestStationData(stationID, verbose=False)
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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-t', '--test', action='store_true',
        help='Test mode: watch all stations, short intervals')
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='Verbose printing, for debug')
    parser.add_argument(
        '-i', '--injector', action='store_true',
        help='Auto restart of injector')
    args = parser.parse_args()

    slacker = DoseNetSlacker(
        './ucbdosenet_slack_token',
        test=args.test, verbose=args.verbose, restart_injector=args.injector)
    print('Running DoseNetSlacker...')
    try:
        slacker.run()
    except Exception as e:
        msg = tb.format_exc()
        slacker.post('Exception: {}: {}'.format(type(e), msg))
