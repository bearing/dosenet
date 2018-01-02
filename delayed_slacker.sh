#!/usr/bin/env/bash
# runs slacker.py after waiting for SQL to be up

LOGTAG=Slack
SLACKER_PATH=/home/dosenet/git/dosenet_slack_outage
DELAY_TIME_S=10

sleep $DELAY_TIME_S

/home/dosenet/anaconda/bin/python $SLACKER_PATH/slacker.py -i &

if [[ $? -eq 0 ]]
then
  logger --stderr --id --tag=$LOGTAG "Successfully started slacker.py"
else
  logger --stderr --id --tag=$LOGTAG "Failed to start slacker.py"
fi
