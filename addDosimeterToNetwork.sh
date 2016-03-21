#! /bin/sh
# script to do everything needed to add a new station to the system:
#    - run addDosimeterToDB.py
#    - restart udp_injector.py to pick up new location


NAME = $1
NICKNAME = $2
LAT = $3
LONG = $4
CONV = $5
DISPLAY = $6

if [ ! -f $DISPLAY ]
then
  # not all arguments provided
  python ~/git/dosenet/addDosimeterToDB.py -h
  exit -1
fi

echo "Adding new station to database"
python ~/git/dosenet/addDosimeterToDB.py --name '$NAME' '$NICKNAME' --latlong $LAT $LONG --conv $CONV --display $DISPLAY

echo "Stopping udp_injector"
sudo killall python &

echo "Restarting udp_injector"
tmux
injector
tmux detach

echo "checking that udp_injector restarted"
grepp

echo "New station added!"