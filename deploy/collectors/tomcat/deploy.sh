#!/bin/bash

conf_file="/opt/cloudwiz-agent/agent/collectors/conf/tomcat.conf"

function display_usage() {
    printf "usage:\n$0 tomcat-webapp1-root:port1 tomcat-webapp2-root:port2 ...\n"
}

if [[ "$#" -lt 1 ]]  || [[ $1 = '-h' ]]; then
    display_usage
    exit 1
fi

currdir=$(cd $(dirname $0); pwd)
ports=''
for webapp_path_and_root in "$@"; do
    IFS=':'
    path_port=(${webapp_path_and_root})
    if [ -z "${path_port[0]}" ]; then
        printf "${color_red}empty webapp root path. abort!${color_normal}\n"
        exit 1
    fi
    if [ -z "${path_port[1]}" ]; then
        printf "${color_red}empty port number. abort!${color_normal}\n"
        exit 1
    fi
    echo "cp -f $currdir/jolokia.war ${path_port[0]}"
    yes | cp -f $currdir/jolokia.war ${path_port[0]} || exit 1
    ports="${ports},${path_port[1]}"
done

ports=${ports:1}
echo "set ports: $ports in $conf_file"
if [[ -f $conf_file ]]; then
    sed -i "/^ports *= */c\ports = $ports" "$conf_file"
fi