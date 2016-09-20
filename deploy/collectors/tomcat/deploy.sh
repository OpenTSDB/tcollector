#!/bin/bash

if [[ "$#" -lt 1 ]]; then
    echo "missing tomcat webapp path"
    exit 1
fi

tomcat_webapp_path=$1
currdir=$(cd $(dirname $0); pwd)
echo "cp $currdir/jolokia.war $tomcat_webapp_path"
yes | cp -f $currdir/jolokia.war $tomcat_webapp_path

