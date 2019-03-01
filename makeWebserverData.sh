#!/usr/bin/env bash

HOMEPATH=/home/dosenet
PYTHONPATH=$HOMEPATH/anaconda/bin/
PATH=$PYTHONPATH:$PATH
CODEPATH=$HOMEPATH/git/dosenet-ali-dev

. $HOMEPATH/.keychain/$HOSTNAME-sh

source activate aws-test-conda2

args=("$@")

while [ 1 ]; do
  if [ "${args[0]}" == "--geojson" ]
  then
      sleep 30
      python $CODEPATH/makeGeoJSON.py
  fi
  if [ "${args[0]}" == "--data" ]
  then
      sleep 150
      python $CODEPATH/makeCSV.py
  fi

  python $CODEPATH/sendDataToWebserver.py
  echo "Finished sending data to webserver"

  logger --stderr --id --tag $LOGTAG "Finished sending data to webserver"

  sleep 5m

done
