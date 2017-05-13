#!/bin/bash

images=$(docker images -a --filter=dangling=true -q)

if [ "_$images" != "_" ]; then
    docker rmi $images
else
    echo "Nothing to remove"
fi
