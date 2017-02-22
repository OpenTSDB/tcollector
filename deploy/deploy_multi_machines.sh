#!/bin/bash

TCOLLECTOR_DIR=/tmp/publish/RedHat
HOST=( "172.1.1.1" "172.1.1.2" "172.1.1.3" )
USERNAME=root
TOKEN=xxxx
CLIENT_ID=xxx
METRIC_SERVER_HOST=localhost
ALERTD_SERVER=localhost

function usage() {
  printf "usage: deploy_multi_machines.sh [-h][-update][-deploy]\n"
  echo "Arguments:"
  echo "    -h            help manual"
  echo "    -update       update collector which have already deploy"
  echo "    -deploy       deploy a new collector"
}


if [ "$#" -gt 0 ]; then
    if [ "$1" == "-h" ]; then
        usage
        exit 0
    elif [ "$1" == "-update" ]; then
        for host in "${HOST[@]}"
        do
            echo "prepare tcollector deploy"
            scp -r  ${TCOLLECTOR_DIR} ${USERNAME}@${host}:${TCOLLECTOR_DIR}
            echo "deploying tcollector for ${USERNAME}@${host}"
            ssh -t  ${USERNAME}@${host} "sudo ORG_TOKEN=${TOKEN} CLIENT_ID=${CLIENT_ID} METRIC_SERVER_HOST=${METRIC_SERVER_HOST} ALERTD_SERVER=${ALERTD_SERVER} ${TCOLLECTOR_DIR}/deploy_agent.sh -update > /tmp/tcollector_udpate.log"
            ssh -t  ${USERNAME}@${host} "sudo /etc/init.d/cloudwiz-agent start > /tmp/tcollector_udpate.log"
        done
        exit 0
    elif [ "$1" == "-deploy" ]; then
        for host in "${HOST[@]}"
        do
            echo "prepare tcollector deploy"
            scp -r  ${TCOLLECTOR_DIR} ${USERNAME}@${host}:${TCOLLECTOR_DIR}
            echo "deploying tcollector for ${USERNAME}@${host}"
            ssh -t  ${USERNAME}@${host} "sudo ORG_TOKEN=${TOKEN} CLIENT_ID=${CLIENT_ID} METRIC_SERVER_HOST=${METRIC_SERVER_HOST} ALERTD_SERVER=${ALERTD_SERVER} ${TCOLLECTOR_DIR}/deploy_agent.sh > /tmp/tcollector_udpate.log"
        done
        exit 0
    fi
else
    usage
    exit 1
fi


