#!/usr/bin/env bash
#title:       :database_backup.sh
#description: :This shall backup subsets of the dosimeter_network database.
#author:      :Navrit Bal
#date:        :20150728
#version:     :0.1
#usage:       :./database_backup.sh
#notes:       :Have fun Joey!
#bash_version :4.2.25(1)-release
#===============================================================================
/usr/bin/clear
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[1;34m'
NC='\033[0m' # No Color
printf "%b\n" "\e~ Using shell: ${GREEN}`which bash`${NC}"
args=("$@")
printf "\n%s\n" "Number of arguments passed: " $#
CHOICE=${args[0]}
printf "%b\n" ">> You will need the database password for these operations - look in the \
Google Drive /DoseNet/DatabaseAndNetworking/${GREEN}Networking Details.gdoc${NC}"

if [[ "$CHOICE" == "all" ]]; then
   printf "%s\n" "~ Backing up the whole dosimeter_network database"
   printf "%b\n" "\e~~ Using: ${BLUE}mysqldump -u ne170group -p dosimeter_network > ~/backup_all_dosenet.sql${NC}"
   mysqldump -u ne170group -p dosimeter_network > ~/backup_all_dosenet.sql
elif [[ "$CHOICE" == "stations" ]]; then
   printf "%s\n" "~ Just backing up the stations table of the dosimeter_network database"
   printf "%b\n" "\e~~ Using: ${BLUE}mysqldump -u ne170group -p dosimeter_network stations > ~/backup_stations_dosenet.sql${NC}"
   mysqldump -u ne170group -p dosimeter_network stations > ~/backup_stations_dosenet.sql
elif [[ "$CHOICE" == "data" ]]; then
   printf "%s\n" "~ Just backing up the dosnet (data) table of the dosimeter_network database"
   printf "%b\n" "\e~~ Using: ${BLUE}mysqldump -u ne170group -p dosimeter_network dosnet > ~/backup_dosnet_dosenet.sql${NC}"
   mysqldump -u ne170group -p dosimeter_network dosnet > ~/backup_dosnet_dosenet.sql
else
   printf "\t%b\n" "${RED}INVALID INPUT.${NC}"
   printf "%b\n" "Valid options: ${GREEN}all, stations or data${NC} \n ${BLUE}Try again!?${NC}"
fi

ls ~/*.sql
