#!/bin/bash

color_red=$(tput setaf 1)
color_normal=$(tput sgr0)
color_green=$(tput setaf 2)

agent_install_folder="/opt/cloudwiz-agent"
mysql_conf_file=${agent_install_folder}/agent/collectors/conf/mysql.conf
mysql_stats_user="cloudwiz_user"
mysql_stats_pass="cloudwiz_pass"
mysql_priv_user=${1:-"root"}

function check_root() {
  if [[ "$USER" != "root" ]]; then
    echo "please run as: sudo $0"
    exit 1
  fi
}

function abort_if_failed() {
  if [ $? -ne 0 ]; then
    printf "${color_red}$1. abort!${color_normal}\n"
    exit 1
  fi
}

function log_info() {
  printf "${color_green}$1${color_normal}\n"
}

read -p "the mysql collector will create a mysql user called ${mysql_stats_user}. If you have a user in use with the same name, abort now and contact cloudwiz support person. Otherwise, press any key to continue."

check_root

if [ "$#" -lt 1 ]; then
    log_info "no pass-in privilege user, default to ${mysql_priv_user}"
fi 

if [ ! -d "$agent_install_folder" ]; then
    printf "${color_red}cloudwiz-agent installation folder ($agent_install_folder) does not exist. abort!${color_normal}\n"
    exit 1
fi

if [ ! -f "$mysql_conf_file" ]; then
    printf "${color_red}mysql collector conf file $mysql_conf_file does not exist. abort!${color_normal}\n"
    exit 1
fi

command -v mysql > /dev/null 2>&1 || { echo >&2 "mysql command does not exist.  Aborting."; exit 1; }
command -v mysql_config > /dev/null 2>&1 || { echo >&2 "mysql_config command does not exist. Make sure it is in the path. Usually it is under <mysql_root>/bin. Easy fix is to create a symlink like ln -s <mysql_install_root>/bin/mysql_config /usr/bin/mysql_config. Aborting."; exit 1; }
log_info "CREATE USER '${mysql_stats_user}'@'localhost' IDENTIFIED BY '${mysql_stats_pass}'"
log_info "type in password for mysql user $mysql_priv_user"
mysql -u "$mysql_priv_user" -p -e "GRANT USAGE ON *.* TO '${mysql_stats_user}'@'localhost'; DROP USER '${mysql_stats_user}'@'localhost'; GRANT PROCESS, REPLICATION CLIENT ON *.* TO '${mysql_stats_user}'@'localhost' IDENTIFIED BY '${mysql_stats_pass}'"
abort_if_failed "failed to create stats user '${mysql_stats_user}'@'localhost'"

log_info "config mysql stats user/pass in ${mysql_conf_file} s/user:.*/user: ${mysql_stats_user}/g"
sed -i.bak -e "s/enabled:.*/enabled: True/g" -e "s/user:.*/user: ${mysql_stats_user}/g" -e "s/pass:.*/pass: ${mysql_stats_pass}/g" ${mysql_conf_file}
abort_if_failed "failed to config mysql collector conf file"

if ! $("$agent_install_folder/altenv/bin/python" -c 'import MySQLdb' &> /dev/null); then
    log_info "install MySQLdb python module to talk to mysql to collect data"
    pushd "$agent_install_folder"
    mkdir -p workspace
    wget --directory-prefix=./workspace/ https://pypi.python.org/packages/a5/e9/51b544da85a36a68debe7a7091f068d802fc515a3a202652828c73453cad/MySQL-python-1.2.5.zip#md5=654f75b302db6ed8dc5a898c625e030c
    abort_if_failed 'failed to download https://pypi.python.org/packages/a5/e9/51b544da85a36a68debe7a7091f068d802fc515a3a202652828c73453cad/MySQL-python-1.2.5.zip#md5=654f75b302db6ed8dc5a898c625e030c'
    unzip workspace/MySQL-python-1.2.5.zip -d workspace/
    abort_if_failed 'failed to unzip workspace/MySQL-python-1.2.5.zip'
    cd workspace/MySQL-python-1.2.5
    "$agent_install_folder/altenv/bin/python" setup.py install --prefix="$agent_install_folder/altenv" --record ../mysql-python-filelist.txt
    abort_if_failed 'failed to install MySQL-python-1.2.5'
    popd
fi
log_info "Complete!"
