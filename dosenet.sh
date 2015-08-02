#! /bin/sh
# /etc/init.d/dosenet
### BEGIN INIT INFO
# Provides:          dosenet
# Required-Start:    networking
# Required-Stop:     networking
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# X-Interactive:     false
# Short-Description: DoseNet - sends UDP packets to the GRIM for the DoseNet project
### END INIT INFO
CONFIGFILE=etchhall.csv
HOME=/home/pi
DOSENET=$HOME/dosenet
CONDA=$HOME/miniconda/bin
LOG=/home/pi/sender.log
echo "DoseNet script called @ " > $LOG
date >> $LOG
case "$1" in
  start)
    echo "Starting DoseNet script" >> $LOG
    sudo $CONDA/python $DOSENET/udp_sender.py -f $DOSENET/config-files/$CONFIGFILE >> $LOG &
    date >> $LOG
    ;;
  stop)
    echo "Stopping DoseNet script"  >> $LOG
    sudo killall python
    ;;
  *)
    echo "Usage: /etc/init.d/dosenet {start|stop}"
    exit 1
    ;;
esac

exit 0
