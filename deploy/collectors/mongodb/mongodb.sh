#!/bin/bash

TSTAMP=$(/bin/date +%s)
RAW=$(./mongostat -h 172.16.16.2 -n 1 --noheaders)
read -r -a array <<< "$RAW"

if [ ${#array[@]} -lt 19 ]; then
    exit 1
fi

function convert()
{
    local src=${1,,}
    local __dstvar=$2
    local dst=''
    local factor=1

    if [[ "$src" == *b ]]; then
        dst=${src%'b'}
    elif [[ "$src" == *k ]]; then
        dst=${src%'k'}
        factor=1024
    elif [[ "$src" == *m ]]; then
        dst=${src%'m'}
        factor=1024000
    elif [[ "$src" == *g ]]; then
        dst=${src%'g'}
        factor=1024000000
    fi

    dst=$(echo "$dst*$factor" | /usr/bin/bc)
    eval $__dstvar="'$dst'"

    return 0
}

echo "mongodb.insert_count $TSTAMP ${array[0]#"*"} host=$HOSTNAME"
echo "mongodb.query_count $TSTAMP ${array[1]#"*"} host=$HOSTNAME"
echo "mongodb.update_count $TSTAMP ${array[2]#"*"} host=$HOSTNAME"
echo "mongodb.delete_count $TSTAMP ${array[3]#"*"} host=$HOSTNAME"
echo "mongodb.get_more $TSTAMP ${array[4]#"*"} host=$HOSTNAME"
echo "mongodb.command.local $TSTAMP ${array[5]%|*} host=$HOSTNAME"
echo "mongodb.command.replicated $TSTAMP ${array[5]#*|} host=$HOSTNAME"
echo "mongodb.dirty $TSTAMP ${array[6]%'%'} host=$HOSTNAME"
echo "mongodb.used $TSTAMP ${array[7]%'%'} host=$HOSTNAME"
echo "mongodb.flushes $TSTAMP ${array[8]} host=$HOSTNAME"
convert ${array[9]} vsize
echo "mongodb.vsize $TSTAMP $vsize host=$HOSTNAME"
convert ${array[10]} res
echo "mongodb.res $TSTAMP $res host=$HOSTNAME"
echo "mongodb.queue_read $TSTAMP ${array[11]%|*} host=$HOSTNAME"
echo "mongodb.queue_write $TSTAMP ${array[11]#*|} host=$HOSTNAME"
echo "mongodb.active_read $TSTAMP ${array[12]%|*} host=$HOSTNAME"
echo "mongodb.active_write $TSTAMP ${array[12]#*|} host=$HOSTNAME"
convert ${array[13]} netin
echo "mongodb.net_in $TSTAMP $netin host=$HOSTNAME"
convert ${array[14]} netout
echo "mongodb.net_out $TSTAMP $netout host=$HOSTNAME"
echo "mongodb.conn $TSTAMP ${array[15]} host=$HOSTNAME"

exit 0
