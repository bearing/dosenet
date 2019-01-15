#!/usr/bin/env bash
#title:       :database_backup.sh
#description: :This shall backup subsets of the dosimeter_network database.
#author:      :Navrit Bal and Joseph Curtis
#date:        :20160623
#version:     :1.0
#usage:       :./database_backup.sh
#notes:       :Have fun Joey!
#bash_version :4.2.25(1)-release
#
# This should be run in crontab:
#
# crontab -e
# @daily . /home/dosenet/git/dosenet_backup/mysql/database_backup.sh all
#
#===============================================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[1;34m'
NC='\033[0m' # No Color
echo -e "Using shell: ${GREEN}`which bash`${NC}"
args=("$@")
CHOICE=${args[0]}
FILEDATE=$(date +%Y-%m-%dT%H-%M-%S)

if [[ "$CHOICE" == "all" ]]; then
    echo "Backing up the complete dosimeter_network database"
    FNAME=~/DBdumps/backup_$FILEDATE.sql
    CMD="mysqldump -u ne170group dosimeter_network > ${FNAME}"
elif [[ "$CHOICE" == "stations" ]]; then
    echo "Just backing up the stations table of the dosimeter_network database"
    FNAME=~/DBdumps/backup_stations_$FILEDATE.sql
    CMD="mysqldump -u ne170group dosimeter_network stations > ${FNAME}"
elif [[ "$CHOICE" == "data" ]]; then
    echo "Just backing up the dosnet (data) table of the dosimeter_network database"
    FNAME=~/DBdumps/backup_dosnet_$FILEDATE.sql
    CMD="mysqldump -u ne170group dosimeter_network dosnet > ${FNAME}"
else
    printf "\t%b\n" "${RED}INVALID INPUT.${NC}"
    printf "%b\n" "Valid options: ${GREEN}[all, stations, data]${NC} \n ${BLUE}Try again!?${NC}"
    exit 1
fi

# Run command and tell User
echo -e ${BLUE}${CMD}${NC}
eval $CMD

# Print back file info
stat $FNAME
ls -lah $FNAME | awk -F " " {'print $5'}
