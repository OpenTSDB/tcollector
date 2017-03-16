#!/bin/bash
#
# filebeat          filebeat shipper
#
# chkconfig: 2345 98 02
#

### BEGIN INIT INFO
# Provides:          filebeat
# Required-Start:    $local_fs $network $syslog
# Required-Stop:     $local_fs $network $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Sends log files to Logstash or directly to Elasticsearch.
# Description:       filebeat is a shipper part of the Elastic Beats 
#					 family. Please see: https://www.elastic.co/products/beats
### END INIT INFO



PATH=/usr/bin:/sbin:/bin:/usr/sbin
export PATH

name="filebeat"

[ -r /etc/default/$name ] && . /etc/default/$name

agent="$FB_HOME/filebeat"
args="-c $FB_HOME/filebeat.yml"
test_args="-e -configtest"

# Source function library.
[ -r /etc/rc.d/init.d/functions ] && . /etc/rc.d/init.d/functions

function kill_child_process {
    CHILD_PID=$(pgrep -P $$)
    kill $CHILD_PID
}

trap kill_child_process EXIT

echo -n $"Starting filebeat..."

$agent $args $test_args
if [ $? -ne 0 ]; then
    echo
    exit 1
fi

$agent $args
exit $?
