#!/usr/bin/env bash

. ~/.keychain/$HOSTNAME-sh
~/anaconda/bin/python ~/git/dosenet/makeCSV.py
~/anaconda/bin/python ~/git/dosenet/makeGeoJSON.py
~/anaconda/bin/python ~/git/dosenet/sendDataToWebserver.py
