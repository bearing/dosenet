#!/usr/bin/env bash

LOGTAG=makeWebserverData
HOMEPATH=/home/dosenet
PYTHONPATH=$HOMEPATH/anaconda/bin/
PATH=$PYTHONPATH:$PATH

. $HOMEPATH/.keychain/$HOSTNAME-sh

# let devices post first
logger --stderr --id --tag $LOGTAG "Making files for webserver..."
for arg in "$@"
do
    if [ "$arg" == "--geojson" ]
    then
        python $HOMEPATH/git/dosenet/makeGeoJSON.py
    fi
    if [ "$arg" == "--last-hour" ]
    then
        python $HOMEPATH/git/dosenet/makeCSV.py
    fi
    if [ "$arg" == "--last-day" ]
    then
        python $HOMEPATH/git/dosenet/makeCSV.py --last-day
    fi
    if [ "$arg" == "--last-week" ]
    then
        python $HOMEPATH/git/dosenet/makeCSV.py --last-week
    fi
    if [ "$arg" == "--last-month" ]
    then
        python $HOMEPATH/git/dosenet/makeCSV.py --last-month
    fi
    if [ "$arg" == "--last-year" ]
    then
        python $HOMEPATH/git/dosenet/makeCSV.py --last-year
    fi
done
#python $HOMEPATH/git/dosenet/makeCSV.py
#python $HOMEPATH/git/dosenet/makeGeoJSON.py
python $HOMEPATH/git/dosenet/sendDataToWebserver.py

logger --stderr --id --tag $LOGTAG "Finished sending data to webserver"
