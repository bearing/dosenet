#! /bin/sh
# /etc/init.d/dosenet_listener
### BEGIN INIT INFO
# Provides:          dosenet UDP listener
# Required-Start:    networking
# Required-Stop:     networking
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# X-Interactive:     false
# Short-Description: DoseNet - Recives & injects UDP packets on GRIM for the DoseNet project
### END INIT INFO
HOME=/home/dosenet
DOSENET=$HOME/dosenet
LOG=/$HOME/listener.log
echo "DoseNet listener script called @ " > $LOG
date >> $LOG
case "$1" in
  start)
    echo "Starting DoseNet listener script" >> $LOG
    #python $DOSENET/udp_injector.py >> $LOG &
    date >> $LOG
    ;;
  stop)
    echo "Stopping DoseNet script"  >> $LOG
    sudo killall python
    ;;
  *)
    echo "Usage: /etc/init.d/dosenet_listener {start|stop}"
    exit 1
    ;;
esac

exit 0
