#!/bin/bash

#docker rm $(docker ps --filter=status=exited --filter=status=created -q)
containers=$(docker ps --filter=status=exited -q)

if [ "_$containers" != "_" ]; then
    docker rm $containers
else
    echo "Nothing to remove"
fi
