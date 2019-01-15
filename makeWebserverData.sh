#!/usr/bin/env bash

LOGTAG=makeWebserverData
HOMEPATH=/home/dosenet
PYTHONPATH=$HOMEPATH/anaconda/bin/
PATH=$PYTHONPATH:$PATH

. $HOMEPATH/.keychain/$HOSTNAME-sh

# let devices post first
sleep 60
logger --stderr --id --tag $LOGTAG "Making files for webserver..."
#python $HOMEPATH/git/dosenet/makeCSV.py
python $HOMEPATH/git/dosenet/makeGeoJSON.py
python $HOMEPATH/git/dosenet/sendDataToWebserver.py

logger --stderr --id --tag $LOGTAG "Finished sending data to webserver"
