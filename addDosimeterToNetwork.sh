#! /bin/sh
# script to do everything needed to add a new station to the system:
#    - run addDosimeterToDB.py
#    - restart udp_injector.py to pick up new location

while [[ $# > 1 ]]
do
key="$1"

case $key in
    --name)
    NAME="$2"
    shift # past argument
    ;;
    --nickname)
    NICKNAME="$2"
    shift # past argument
    ;;
    --lat)
    LAT="$2"
    shift # past argument
    ;;
    --long)
    LONG="$2"
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