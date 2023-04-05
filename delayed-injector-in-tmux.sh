#!/bin/bash

# Upon boot, SQL db is not ready immediately.
# Therefore, wait a bit before starting the injector.

#UPDATE: not using SQL anymore, but slight delay for system still not a terrible idea
DELAY_TIME_S=5

sleep $DELAY_TIME_S

bash /home/dosenet/git/dosenet/start-injector-in-tmux.sh

