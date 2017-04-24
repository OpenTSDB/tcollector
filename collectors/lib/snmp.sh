#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 <device>"
    exit 1
fi

DEVICE=$1
TSTAMP=$(date +%s)

OUT=$(snmpget $DEVICE 1.3.6.1.2.1.11.1.0)
read -r -a array <<< "$OUT"
if [ ${#array[@]} -lt 4 ]; then
    exit 2
fi
echo "snmp.${DEVICE}.inpkts $TSTAMP ${array[3]} host=$HOSTNAME"

OUT=$(snmpget $DEVICE 1.3.6.1.2.1.11.2.0)
read -r -a array <<< "$OUT"
if [ ${#array[@]} -lt 4 ]; then
    exit 3
fi
echo "snmp.${DEVICE}.outpkts $TSTAMP ${array[3]} host=$HOSTNAME"
