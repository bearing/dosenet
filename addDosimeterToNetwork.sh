#!/bin/bash
# script to do everything needed to add a new station to the system:
#    - run addDosimeterToDB.py
#    - restart udp_injector.py to pick up new location

. ~/.keychain/$HOSTNAME-sh

while [[ $# > 1 ]]
do
key="$1"

case $key in
    --name)
    NAME="$2"
    NICKNAME="$3"
    shift # past argument
    ;;
    --latlong)
    LAT="$2"
    LONG="$3"
    shift # past argument
    ;;
    --conv)
    CONV="$2"
    shift # past argument
    ;;
    --display)
    DISPLAY="$2"
    ;;
    *)
       # unknown option
    ;;
esac
shift # past argument or value
done

echo "NAME = ${NAME}"
echo "NICKNAME = ${NICKNAME}"
echo "LAT = ${LAT}"
echo "LONG = ${LONG}"
echo "CONV = ${CONV}"
echo "DISPLAY = ${DISPLAY}"

echo "Adding new station to database"
python ~/git/dosenet_dev/addDosimeterToDB.py --name '$NAME' '$NICKNAME' --latlong $LAT $LONG --conv $CONV --display $DISPLAY

echo "Stopping udp_injector"
sudo killall python &

echo "Restarting udp_injector"
tmux
injector
tmux detach

echo "checking that udp_injector restarted"
grepp

echo "New station added!"