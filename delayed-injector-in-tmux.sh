#!/bin/bash

# Upon boot, SQL db is not ready immediately.
# Therefore, wait a bit before starting the injector.

DELAY_TIME_S=10

sleep $DELAY_TIME_S

/home/dosenet/git/dosenet/start-injector-in-tmux.sh

