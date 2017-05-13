#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Usage: $0 <os> <version>"
    exit 1
fi

OS=${1,,}
VER=${2,,}

if [ ! -f "$OS/$VER/Dockerfile" ]; then
    echo "File $OS/$VER/Dockerfile does not exist!"
    exit 2
fi

echo "Building $OS/$VER"

docker build --tag cloudwiz.cn/$OS/$VER:latest $OS/$VER

echo "Done"

exit 0
