#!/bin/bash
# script to do everything needed to add a new station to the system:
#    - run addDosimeterToDB.py
#    - restart udp_injector.py to pick up new location

numargs=0

read -p "Full location name: " NAME
if [[ $NAME ]]
	then let "numargs++"
fi

read -p "location nickname (lowercase, no spaces): " NICKNAME
if [[ $NICKNAME ]] 
	then let "numargs++"
fi

read -p "Lattitude: " LAT
if [[ $LAT ]]
	then let "numargs++"
fi

read -p "Longitude: " LONG
if [[ $LONG ]]
	then let "numargs++"
fi

read -p "Display on (1) or off (0)? " DISPLAY
if [[ $DISPLAY ]]
	then let "numargs++"
fi

read -p "ID (optional): " ID

CONV=0.0036

if [[ $numargs < 5 ]]
then
  echo "Only the ID is optional. All other requested inputs are required"!
  ehco "As a reminder, this runs addDosimeter.py: "
  python ~/git/dosenet/addDosimeterToDB.py -h
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
echo "python script returned ${status}"

if [[ $status -ne 0 ]]
then
  echo "ERROR adding station to database! Exiting now."
  exit 1
fi

tmux new-session -d -s INJECTOR 

echo "Stopping injector"
tmux send-keys -t INJECTOR C-c
tmux send-keys -t INJECTOR C-c

echo "Restarting injector"
tmux send-keys -t INJECTOR "injector" C-m

ps aux | grep python | grep -v grep

echo "New station added!"
exit 0
