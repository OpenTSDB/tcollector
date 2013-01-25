#!/bin/bash

# Collects the OpenTSDB stats published over HTTP.

URL="http://localhost:4242/stats"
CURL_BIN="/usr/bin/curl"
COLLECTION_INTERVAL="1m"

while true
do
    ${CURL_BIN} -s ${URL}
    sleep ${COLLECTION_INTERVAL}
done
