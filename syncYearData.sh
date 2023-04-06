#!/bin/bash

# Script to backup dosenet data and sync with webserver

echo " Copying data to backups"
cp -urf /home/dosenet/tmp/* /home/dosenet/backups/tmp

echo " Updating json and binned data"
cd /home/dosenet/backups
source activate aws-test-conda2
export PATH=/home/dosenet/anaconda3/envs/aws-test-conda2/bin:$PATH
python /home/dosenet/git/dosenet/makeCSV.py -y -p "/home/dosenet/backups/tmp/" -o "/home/dosenet/data/tmp/"

echo " Syncing data with webserver"
#lftp -e "mirror -Rnv /home/dosenet/backups/tmp /test/; quit;" -u coeradwatch-RADWATCH,r4dw4tch sftp://coeradwatch.sftp.wpengine.com:2222
#lftp -e "mirror -Rnv /home/dosenet/backups/tmp /test/; quit;" -u coeradwatch-RADWATCH,'KtL&zSE3' sftp://coeradwatch.sftp.wpengine.com:2222
#lftp -e "mirror -Rnv /home/dosenet/backups/tmp /test/; quit;" -u coeradwatch-RADWATCH1,'HBMbD9irRjYf' sftp://coeradwatch.sftp.wpengine.com:2222
lftp -e "mirror -Rnv /home/dosenet/data/tmp /test/; quit;" -u coeradwatch-RADWATCH,'x9DvsvP9gbVWT9F' sftp://coeradwatch.sftp.wpengine.com:2222
