#!/bin/bash

URL=http://10.9.126.97:9200

declare -A STATUS=( ["unknown"]="-1" ["green"]="0" ["yellow"]="1" ["red"]="2" )

OUT=$(curl ${URL}/_cat/health 2> /dev/null)
read -r -a array <<< "$OUT"

if [ ${#array[@]} -le 13 ]; then
    exit 1
fi

stat=${array[3]}
[ ${STATUS[$stat]+abc} ] || stat="unknown"

echo "elasticsearch.${array[2],,}.status ${array[0]} ${STATUS[$stat]} host=$HOSTNAME"
echo "elasticsearch.${array[2],,}.node.total ${array[0]} ${array[4]} host=$HOSTNAME"
echo "elasticsearch.${array[2],,}.node.data ${array[0]} ${array[5]} host=$HOSTNAME"
echo "elasticsearch.${array[2],,}.shards.total ${array[0]} ${array[6]} host=$HOSTNAME"
echo "elasticsearch.${array[2],,}.shards.primary ${array[0]} ${array[7]} host=$HOSTNAME"
echo "elasticsearch.${array[2],,}.shards.relocating ${array[0]} ${array[8]} host=$HOSTNAME"
echo "elasticsearch.${array[2],,}.shards.initializing ${array[0]} ${array[9]} host=$HOSTNAME"
echo "elasticsearch.${array[2],,}.shards.unassigned ${array[0]} ${array[10]} host=$HOSTNAME"
echo "elasticsearch.${array[2],,}.shards.active ${array[0]} ${array[13]%\%} host=$HOSTNAME"

exit 0
