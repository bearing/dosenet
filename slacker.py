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
from slackclient import SlackClient
from mysql.mysql_tools import SQLObject

INTERVAL = 300
SLACK_USER = 'dosenet_server'
ICON = ':radioactive_sign:'
SLACK_CHANNEL = '#random'
TOKEN_NAME = 'ucbdosenet_slack_token.txt'

if socket.gethostname().startswith('plimley'):
    TOKEN_PATH = './'
elif socket.gethostname() == 'dosenet':
    TOKEN_PATH = os.path.expanduser('~/')
else:
    raise RuntimeError('Unknown host {}, cannot load Slack token'.format(
        socket.gethostname()))


class DoseNetSlacker(object):

    def __init__(self, tokenfile='~/ucbdosenet_slack_token.txt'):
        self.get_slack(tokenfile)
        self.get_sql()
        self.interval_s = INTERVAL

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
            self.slack.api_call(
                'chat.postMessage',
                channel=SLACK_CHANNEL,
                username=SLACK_USER,
                icon_emoji=ICON,
                text='Testing')
            time.sleep(self.interval_s)


if __name__ == '__main__':
    slacker = DoseNetSlacker('./ucbdosenet_slack_token')
    print('Running DoseNetSlacker...')
    slacker.run()
