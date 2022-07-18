#!/usr/bin/env bash

HOMEPATH=/home/dosenet
PYTHONPATH=$HOMEPATH/anaconda3/envs/aws-test-conda2/bin
PATH=$PYTHONPATH:$PATH
CODEPATH=$HOMEPATH/git/dosenet-ali-dev

. $HOMEPATH/.keychain/$HOSTNAME-sh

args=("$@")

while [ 1 ]; do
  if [ "${args[0]}" == "--geojson" ]
  then
      sleep 30
      $PYTHONPATH/python $CODEPATH/makeGeoJSON.py
  fi
  if [ "${args[0]}" == "--data" ]
  then
      #sleep 150
      $PYTHONPATH/python $CODEPATH/makeCSV.py
  fi
  if [ "${args[0]}" == "--year" ]
  then
      #sleep 150
      $PYTHONPATH/python $CODEPATH/makeCSV.py -y
  fi

  $PYTHONPATH/python $CODEPATH/sendDataToWebserver.py
  echo "Finished sending data to webserver"

  logger --stderr --id --tag $LOGTAG "Finished sending data to webserver"

  sleep 5m

done
