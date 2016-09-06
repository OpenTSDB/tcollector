#!/bin/bash

log_folder=/tmp/log

if [[ ! -d $log_folder ]]; then
    mkdir "$log_folder"
fi

./daemon.py --logfile "$log_folder/uagent.log" "$@"
