#!/bin/bash

# Collects the OpenTSDB stats published over HTTP.

TSD_HOST="localhost"
TSD_PORT="4242"
COLLECTION_INTERVAL="15s"

while :; do
  echo stats || exit
  sleep ${COLLECTION_INTERVAL}
done | nc ${TSD_HOST} ${TSD_PORT}
