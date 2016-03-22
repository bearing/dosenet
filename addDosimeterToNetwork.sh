#!/bin/bash
# script to do everything needed to add a new station to the system:
#    - run addDosimeterToDB.py
#    - restart udp_injector.py to pick up new location

numargs=$#

if [[ $numargs < 6 ]]
then
  echo "Not enough arguments!"
  python ~/git/dosenet_dev/addDosimeterToDB.py -h
  exit -1
fi

NAME=$1
NICKNAME=$2
LAT=$3
LONG=$4
CONV=$5
DISPLAY=$6

echo "NAME = ${NAME}"
echo "NICKNAME = ${NICKNAME}"
echo "LAT = ${LAT}"
echo "LONG = ${LONG}"
echo "CONV = ${CONV}"
echo "DISPLAY = ${DISPLAY}"

echo "Adding new station to database"
python ~/git/dosenet_dev/addDosimeterToDB.py "${NAME}" "${NICKNAME}" $LAT $LONG $CONV $DISPLAY

echo "Stopping udp_injector"
killall python

echo "Restarting udp_injector ... start new session if session doesn't exist"
tmux new-session -d -s UDP_INJECTOR 
tmux send-keys -t UDP_INJECTOR "injector" C-m

echo "New station added!"