#!/bin/bash
# script to do everything needed to add a new station to the system:
#    - run addDosimeterToDB.py
#    - restart udp_injector.py to pick up new location

numargs=$#

NAME=$1
NICKNAME=$2
LAT=$3
LONG=$4
CONV=$5
DISPLAY=$6
ID=$7

echo "NAME = ${NAME}"
echo "NICKNAME = ${NICKNAME}"
echo "LAT = ${LAT}"
echo "LONG = ${LONG}"
echo "CONV = ${CONV}"
echo "DISPLAY = ${DISPLAY}"
echo "ID = ${ID}"

if [[ $numargs < 6 ]]
then
  echo "Not enough arguments!"
  python ~/git/dosenet/addDosimeterToDB.py -h
  echo "Add option ID argument last"
  exit -1
fi

echo "Adding new station to database"
if [[ $ID ]]
then
  python ~/git/dosenet/addDosimeterToDB.py --ID $ID "${NAME}" "${NICKNAME}" $LAT $LONG $CONV $DISPLAY
else
  python ~/git/dosenet/addDosimeterToDB.py "${NAME}" "${NICKNAME}" $LAT $LONG $CONV $DISPLAY  
fi

status=$?
if [[ $status ]]
then
  echo "ERROR adding station to database! Exiting now."
  exit 1
fi

echo "Stopping udp_injector"
killall python

echo "Restarting udp_injector ... start new session if session doesn't exist"
tmux new-session -d -s UDP_INJECTOR 
tmux send-keys -t UDP_INJECTOR "injector" C-m

ps aux | grep python | grep -v grep

echo "New station added!"
exit 0