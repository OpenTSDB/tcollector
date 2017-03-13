#!/bin/bash

log_folder=/tmp/log

if [[ ! -d $log_folder ]]; then
    mkdir "$log_folder"
fi

function kill_child_process {
    CHILD_PID=$(pgrep -P $$)
    kill $CHILD_PID
}

trap kill_child_process EXIT

./daemon.py --logfile "$log_folder/uagent.log" "$@"
