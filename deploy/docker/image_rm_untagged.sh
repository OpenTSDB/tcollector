#!/bin/bash

images=$(docker images | grep "^<none>" | awk "{print $3}")

if [ "_$images" != "_" ]; then
    docker rmi $images
else
    echo "Nothing to remove"
fi
