#!/usr/bin/env bash

HOMEPATH=/home/dosenet
PYTHONPATH=$HOMEPATH/anaconda/bin/
PATH=$PYTHONPATH:$PATH

. $HOMEPATH/.keychain/$HOSTNAME-sh

args=("$@")

while [ 1 ]; do
  if [ "${args[0]}" == "--geojson" ]
  then
      sleep 30
      python $HOMEPATH/git/dosenet/makeGeoJSON.py
  fi
  if [ "${args[0]}" == "--data" ]
  then
      sleep 150
      python $HOMEPATH/git/dosenet/makeCSV.py
  fi

  python $HOMEPATH/git/dosenet/sendDataToWebserver.py
  echo "Finished sending data to webserver"

  logger --stderr --id --tag $LOGTAG "Finished sending data to webserver"

  sleep 5m

done
