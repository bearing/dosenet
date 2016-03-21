#!/bin/bash
# script to do everything needed to add a new station to the system:
#    - run addDosimeterToDB.py
#    - restart udp_injector.py to pick up new location

. ~/.keychain/$HOSTNAME-sh

numargs=$#



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
sudo killall python &

echo "Restarting udp_injector"
#screen -dm python udp_injector.py
tmux new-session -d -s UDP_INJECTOR '~/anaconda/bin/python udp_injector.py'

#echo "killing all old tmux sessions..."
#tmux kill-session -a

echo "New station added!"