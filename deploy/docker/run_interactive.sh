#!/bin/bash

if [ $# -lt 1 ]; then
    echo "Usage: $0 <image>"
    exit 1
fi

docker run -i -t $1

exit $?
