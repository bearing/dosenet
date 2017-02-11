#!/usr/bin/env/bash
# runs slacker.py

LOGTAG=Slack
SLACKER_PATH=/home/dosenet/git/dosenet_slack_outage

logger --stderr --id --tag=$LOGTAG "Starting slacker.py..."

/home/dosenet/anaconda/bin/python $SLACKER_PATH/slacker.py &

if [[ $? -eq 0 ]]
then
  logger --stderr --id --tag=$LOGTAG "Successfully started slacker.py"
else
  logger --stderr --id --tag=$LOGTAG "Failed to start slacker.py"
fi
