#! /bin/sh
# /etc/init.d/dosenet.sh
### BEGIN INIT INFO
# Provides: dosenet
# Required-Start: $all
# Required-Stop: $all
# Default-Start: 2 3 4 5
# Default-Stop: 0 1 6
# X-Interactive: false
# Short-Description: DoseNet - sends UDP packets to the GRIM for the DoseNet project
### END INIT INFO
CONFIGFILE=config.csv
HOME=/home/pi
DOSENET=$HOME/dosenet
CONDA=$HOME/miniconda/bin
LOG=/home/pi/sender.log
echo "DoseNet script called @ " > $LOG
date >> $LOG
case "$1" in
  start)
    echo "Starting DoseNet script" >> $LOG
    echo "Starting DoseNet script"
    sudo screen $CONDA/python $DOSENET/udp_sender.py -f $DOSENET/config-files/$CONFIGFILE
    date >> $LOG
    ;;
  stop)
    echo "Stopping DoseNet script" >> $LOG
    echo "Stopping DoseNet script"
    date >> $LOG
    sudo killall python &
    ;;
  test)
    echo "Testing DoseNet Script" >> $LOG
    echo "Testing DoseNet Script"
    sudo $CONDA/python $DOSENET/udp_sender.py -f $DOSENET/config-files/$CONFIGFILE --test
    date >> $LOG
    ;;
  *)
    echo "Usage: /etc/init.d/dosenet {start|test|stop}"
    exit 1
    ;;
esac

exit 0
