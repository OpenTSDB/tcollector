#!/bin/bash

source common.sh

color_red=$(tput setaf 1)
color_normal=$(tput sgr0)
color_blue=$(tput setaf 4)

agent_user="cwiz-user"
agent_pass="cwiz-pass"
agent_startup_scripts="cloudwiz-agent"
download_source=${AGENT_URL:-"file:///tmp/publish"}
working_folder="/tmp"
agent_install_folder="/opt/cloudwiz-agent"
workspace_folder="${agent_install_folder}/workspace"
altenv_folder="${agent_install_folder}/altenv"
altenv_etc_folder="${altenv_folder}/etc"
altenv_var_folder="${altenv_folder}/var"
altenv_cache_folder="${altenv_var_folder}/cache"

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
  printf "${color_blue}$1${color_normal}\n"
}

function usage() {
  printf "sudo ORG_TOKEN=<token> CLIENT_ID=<id> [METRIC_SERVER_HOST=<server>] deploy_agent.sh [-h | --log-collector <log_server_host:port>]\n"
}

if [ "$#" -gt 0 ]; then
  if [ "$1" == "-h" ]; then
    usage
    exit 0
  elif [ "$1" == "--log-collector" ]; then
    enable_log_collection=true
    if [ "$#" -ge 2 ]; then
      log_server_hostport=$2
    else
      printf "invalid argument\n"
      usage
      exit 1
    fi
  else
    printf "unrecognized option\n"
    usage
    exit 1
  fi
fi

OS=$(get_os)

check_root

if [ -z "${ORG_TOKEN// }" ]; then
  echo "ORG_TOKEN env variable is not set or empty"
  usage
  exit 1
fi

if [ -z "${CLIENT_ID// }" ]; then
  echo "CLIENT_ID env variable is not set or empty"
  usage
  exit 1
fi

log_info "recreate ${agent_install_folder}"
rm -rf ${agent_install_folder}
abort_if_failed "failed to delete ${agent_install_folder}"

log_info "create agent user $agent_user"
userdel $agent_user
exitcode=$?
if [ $exitcode -ne 0 ] && [ $exitcode -ne 6 ]; then   # 6 means user does not exist
  printf "failed to remove existing user $agent_user. try to kill the process running on this user and retry\n"
  exit 1
fi
useradd -d "${agent_install_folder}" -p "$agent_pass" "$agent_user" 
abort_if_failed "failed to create user $agent_user"

log_info "downloading agent tarball ${download_source}/${OS}/agent.tar.gz ${working_folder} and extract it"
curl -Lo ${working_folder}/agent.tar.gz "${download_source}/${OS}/agent.tar.gz"
abort_if_failed "failed to download tarball"
tar -xzf "${working_folder}/agent.tar.gz" -C /
abort_if_failed "failed to extract agent tarball"

sed -i "s/<token>/$ORG_TOKEN/" ${agent_install_folder}/agent/runner.conf
abort_if_failed "failed to set ORG_TOKEN value in runner.conf file"

sed -i "/^client_id *= */c\client_id = $CLIENT_ID" ${agent_install_folder}/uagent/uagent.conf
abort_if_failed "failed to set client_id value in ${agent_install_folder}/uagent/uagent.conf"

if [ "$enable_log_collection" = true ]; then
  log_info "config filebeat in ${altenv_etc_folder}/supervisord.conf"
  sed -i "/\[group:cloudwiz-agent\]/i\\[program:filebeat\]\ncommand=${agent_install_folder}/filebeat-1.3.1/filebeat -c ${agent_install_folder}/filebeat-1.3.1/filebeat.yml\nstartsecs=5\nstartretries=3\nstopasgroup=true\n" ${altenv_etc_folder}/supervisord.conf
  sed -i '/^programs=/ s/$/,filebeat/' ${altenv_etc_folder}/supervisord.conf
  log_info "set log-server-host-port to ${log_server_hostport}"
  sed -i "s/<log-server-host-port>/\"${log_server_hostport}\"/" ${agent_install_folder}/filebeat-1.3.1/filebeat.yml
fi

if [ -z "${METRIC_SERVER_HOST// }" ]; then
  echo "METRIC_SERVER_HOST env variable is not set or empty, default to localhost"
else
  echo "set metric server host s/-H .* /-H $METRIC_SERVER_HOST /g"
  sed -i "s/-H .* /-H $METRIC_SERVER_HOST /g" ${agent_install_folder}/agent/run
fi

mkdir -p "${altenv_cache_folder}"

log_info "creating download folder"
mkdir -p ${agent_install_folder}/download/unpack
abort_if_failed "failed to create ${agent_install_folder}/download/unpack"
abort_if_failed "failed to change ownership of ${agent_install_folder}/download to $agent_user"
log_info "finish creating download folder"

log_info "copy cloudwiz scripts to init.d"
mv -f "${agent_install_folder}/startup_scripts/${OS}/${agent_startup_scripts}" /etc/init.d/
abort_if_failed "failed to mv ${agent_install_folder}/startup_scripts/${OS}/${agent_startup_scripts} to /etc/init.d"
log_info "install the scripts..."
if [ $OS = "Debian" ]; then
  update-rc.d -f ${agent_startup_scripts} remove
  abort_if_failed "failed to unlink the startup scripts"
  update-rc.d ${agent_startup_scripts} defaults
  abort_if_failed "failed to link the startup scripts"
elif [ $OS = "RedHat" ]; then
  chkconfig --del "${agent_startup_scripts}"
  abort_if_failed "failed to unlink the startup scripts"
  chkconfig --add "${agent_startup_scripts}"
  abort_if_failed "failed to link the startup scripts"
else
  printf "${color_red}unrecognized OS $OS. abort!${color_normal}\n"
fi

chown -hR "$agent_user" "${agent_install_folder}"

log_info 'Done!'
log_info 'run "sudo /etc/init.d/cloudwiz-agent start" to start'
