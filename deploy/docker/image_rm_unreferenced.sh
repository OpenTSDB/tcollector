#!/bin/bash

# This will cleanup the docker build cache
# Note that images currently in use by a container will not be removed.
images=$(docker images -a -q)

if [ "_$images" != "_" ]; then
    docker rmi $images
else
    echo "Nothing to remove"
fi
