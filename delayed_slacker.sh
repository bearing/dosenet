#!/usr/bin/env/bash
# runs slacker.py after waiting for SQL to be up

LOGTAG=Slack
HOMEPATH=/home/dosenet
PYTHONPATH=$HOMEPATH/anaconda/bin/
PATH=$PYTHONPATH:$PATH
SLACKER_PATH=/home/dosenet/git/dosenet
DELAY_TIME_S=10

sleep $DELAY_TIME_S

source activate aws-test-conda2

python $SLACKER_PATH/slacker.py -i &> /tmp/slacker.log

if [[ $? -eq 0 ]]
then
  logger --stderr --id --tag=$LOGTAG "Successfully started slacker.py"
else
  logger --stderr --id --tag=$LOGTAG "Failed to start slacker.py"
fi
