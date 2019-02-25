#!/bin/bash

# (Re)Start the DoseNet injector in a tmux session.

tmux has-session -t INJECTOR &> /dev/null
STATUS=$?

if [[ $STATUS -eq 0 ]]
then
    echo "Found existing INJECTOR tmux."
    echo "  Stopping injector..."
    tmux send-keys -t INJECTOR C-c
    tmux send-keys -t INJECTOR C-c
    echo "  Clearing old tmux session..."
    tmux kill-session -t INJECTOR
else
    echo "INJECTOR tmux not found"
fi

echo "Creating tmux session INJECTOR"
tmux new-session -d -s INJECTOR
echo "  Clearing possible tmux errors..."
tmux send-keys -t INJECTOR C-c
tmux send-keys -t INJECTOR C-c

echo "  Sourcing .bashrc to get alias..."
tmux send-keys -t INJECTOR ". ~/.bashrc" C-m
tmux send-keys -t INJECTOR "source activate aws-test-conda2"
echo "  Starting new injector..."
tmux send-keys -t INJECTOR "injector" C-m
# the alias is not loaded in this bash script.

echo "Done!"
echo "  Please confirm via:"
echo "  $ tmux attach-session -t INJECTOR"
echo "  and then detach by:"
echo "    [Ctrl-b] d"
