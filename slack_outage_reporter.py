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
from slackclient import SlackClient
from mysql.mysql_tools import SQLObject


sql = SQLObject()
print(sql.getLatestStationData('test'))


# api_token = open(os.path.expanduser('~/ucbdosenet_slack_token.txt')).read().strip('\n').strip()
# slack = SlackClient(api_token)
# # Send a message to #general channel
# slack.api_call("chat.postMessage", channel="#random", text="Testing with official client :tada:",
#                username='dosenet_server', icon_emoji=':radioactive_sign:')
