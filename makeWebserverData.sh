#!/usr/bin/env bash

HOMEPATH=/home/dosenet
PYTHONPATH=$HOMEPATH/anaconda/bin/
PATH=$PYTHONPATH:$PATH

. $HOMEPATH/.keychain/$HOSTNAME-sh

# let devices post first
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
#python $HOMEPATH/git/dosenet/makeCSV.py
#python $HOMEPATH/git/dosenet/makeGeoJSON.py
python $HOMEPATH/git/dosenet/sendDataToWebserver.py

logger --stderr --id --tag $LOGTAG "Finished sending data to webserver"
