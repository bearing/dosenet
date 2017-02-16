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
  echo "As a reminder, this runs addDosimeterToDB.py: "
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

echo "Restarting injector in tmux"
bash -c ~/git/dosenet/start-injector-in-tmux.sh &> /dev/null

# tmux new-session -d -s INJECTOR 

# echo "Stopping injector"
# tmux send-keys -t INJECTOR C-c
# tmux send-keys -t INJECTOR C-c

# echo "Restarting injector"
# tmux send-keys -t INJECTOR "injector" C-m

ps aux | grep python | grep -v grep

echo
echo "New station added!"
echo
echo "  Please confirm working injector via:"
echo "  $ tmux attach-session -t INJECTOR"
echo "  and then detach by:"
echo "    [Ctrl-b] d"
exit 0
