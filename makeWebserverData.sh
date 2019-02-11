#!/usr/bin/env bash

HOMEPATH=/home/dosenet
PYTHONPATH=$HOMEPATH/anaconda/bin/
PATH=$PYTHONPATH:$PATH

. $HOMEPATH/.keychain/$HOSTNAME-sh

sleep 60

while [ 1 < 2 ]; do
  for arg in "$@"
  do
      if [ "$arg" == "--geojson" ]
      then
          python $HOMEPATH/git/dosenet/makeGeoJSON.py
      fi
      if [ "$arg" == "--data" ]
      then
          python $HOMEPATH/git/dosenet/makeCSV.py
      fi
  done

  python $HOMEPATH/git/dosenet/sendDataToWebserver.py
  echo "Finished sending data to webserver"

  logger --stderr --id --tag $LOGTAG "Finished sending data to webserver"

  sleep 5m

done
