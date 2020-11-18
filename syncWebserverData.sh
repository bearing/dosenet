#!/bin/bash

# Script to backup dosenet data and sync with webserver

echo " Copying data to backups"
cp -rf /home/dosenet/backups/tmp2/* /home/dosenet/backups/tmp3
cp -rf /home/dosenet/backups/tmp1/* /home/dosenet/backups/tmp2
cp -rf /home/dosenet/backups/tmp/* /home/dosenet/backups/tmp1
cp -rf /home/dosenet/tmp/* /home/dosenet/backups/tmp

cd /home/dosenet/backups
source activate aws-test-conda2
export PATH=/home/dosenet/anaconda3/envs/aws-test-conda2/bin:$PATH
python /home/dosenet/git/dosenet/makeGeoJSON.py -p "/home/dosenet/backups/tmp/"
python /home/dosenet/git/dosenet/makeCSV.py -p "/home/dosenet/backups/tmp/"

echo " Syncing data with webserver"
lftp -e "mirror -Rnv /home/dosenet/backups/tmp /test/; quit;" -u coeradwatch-RADWATCH,r4dw4tch sftp://coeradwatch.sftp.wpengine.com:2222
