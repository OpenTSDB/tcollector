#!/bin/bash

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

function _md5() {
  if which md5sum >/dev/null 2>&1; then
    md5sum "$1" | awk '{ print $1 }'
    echo >&2 "switch md5sum to publish md5 key"
  else
    md5 -q "$1"
    echo >&2 "switch md5 to publish md5 key"
  fi
}

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

function get_os() {
	# OS/Distro Detection
	# Try lsb_release, fallback with /etc/issue then uname command
	known_distribution="(Debian|Ubuntu|RedHat|CentOS|openSUSE|Amazon)"
	distribution=$(lsb_release -d 2>/dev/null | grep -Eo $known_distribution  || grep -Eo $known_distribution /etc/issue 2>/dev/null || uname -s)
	if [ $distribution = "Darwin" ]; then
			OS="Darwin"
	elif [ -f /etc/debian_version -o "$distribution" == "Debian" -o "$distribution" == "Ubuntu" ]; then
			OS="Debian"
	elif [ -f /etc/redhat-release -o "$distribution" == "RedHat" -o "$distribution" == "CentOS" -o "$distribution" == "openSUSE" -o "$distribution" == "Amazon" ]; then
			OS="RedHat"
	# Some newer distros like Amazon may not have a redhat-release file
	elif [ -f /etc/system-release -o "$distribution" == "Amazon" ]; then
			OS="RedHat"
	fi

	echo $OS
}

function usage() {
  printf "sudo ORG_TOKEN=<token> CLIENT_ID=<id> [AGENT_URL=<agent-tarball_url> METRIC_SERVER_HOST=<server> LOG_COLLECTOR=<log_server_host:port> ALERTD_SERVER=<alert_server:port>] deploy_agent.sh [-h][-update]\n"
}

if [ "$#" -gt 0 ]; then
  if [ "$1" == "-h" ]; then
    usage
    exit 0
  elif [ "$1" == "-update" ]; then
    #countinue
    printf "redeploy tcollector"
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

# stop all
if  which /etc/init.d/cloudwiz-agent >/dev/null 2>&1; then
    log_info "stop the tcollector"
    /etc/init.d/cloudwiz-agent stop
    abort_if_failed "failed stop the collector"
fi

if [ "$1" == "-update" ]; then
    current_time=$(date "+%Y.%m.%d-%H.%M.%S")
    log_info "copy ${agent_install_folder}/agent/collectors/conf into /tmp"
    yes | cp -fr ${agent_install_folder}/agent/collectors/conf ${working_folder}
    log_info "backup old tcollector"
    yes | cp -fr ${agent_install_folder} "${working_folder}/cloudwiz-agent-bk-${current_time}"
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
abort_if_failed "failed to download tarball or source tarball isn't exit "

## should check sum by md5
log_info "downloading agent md5 hash file"
curl -Lo ${working_folder}/agent.tar.gz.md5 "${download_source}/${OS}/agent.tar.gz.md5"
abort_if_failed "failed to download md5 file or source file isn't exit"

MD5=$(_md5 "${working_folder}/agent.tar.gz")
MD5_check=$(cat "${working_folder}/agent.tar.gz.md5")

if [ $MD5 != $MD5_check ] ; then
   printf "${color_red} MD5 value isn't same as tarball. abort!${color_normal}\n"
   exit 1
fi

tar -xzf "${working_folder}/agent.tar.gz" -C /
abort_if_failed "failed to extract agent tarball"

sed -i "/^token *= */c\token=$ORG_TOKEN" ${agent_install_folder}/agent/runner.conf
abort_if_failed "failed to set ORG_TOKEN value in runner.conf file"

sed -i "/^client_id *= */c\client_id = $CLIENT_ID" ${agent_install_folder}/uagent/uagent.conf
abort_if_failed "failed to set client_id value in ${agent_install_folder}/uagent/uagent.conf"

if [ ! -z "${LOG_COLLECTOR// }" ]; then
  log_info "config filebeat in ${altenv_etc_folder}/supervisord.conf"
  sed -i "/\[group:cloudwiz-agent\]/i\\[program:filebeat\]\ncommand=${agent_install_folder}/filebeat-1.3.1/filebeat -c ${agent_install_folder}/filebeat-1.3.1/filebeat.yml\nstartsecs=5\nstartretries=3\nstopasgroup=true\n" ${altenv_etc_folder}/supervisord.conf
  sed -i '/^programs=/ s/$/,filebeat/' ${altenv_etc_folder}/supervisord.conf
  log_info "set log-server-host-port to ${LOG_COLLECTOR}"
  sed -i "s/<log-server-host-port>/\"${LOG_COLLECTOR}\"/" ${agent_install_folder}/filebeat-1.3.1/filebeat.yml
  sed -i "s/<token>/\"${ORG_TOKEN}\"/" ${agent_install_folder}/filebeat-1.3.1/filebeat.yml
fi

if [ -z "${METRIC_SERVER_HOST// }" ]; then
  echo "METRIC_SERVER_HOST env variable is not set or empty, default to localhost"
else
  echo "set metric server host s/-H .* /-H $METRIC_SERVER_HOST /g"
  sed -i "s/-H .* /-H $METRIC_SERVER_HOST /g" ${agent_install_folder}/agent/run
  ## TODO probably should need the port or add the new parameter
  echo -e "host=$METRIC_SERVER_HOST" >> ${agent_install_folder}/agent/runner.conf
fi

if [ -z "${ALERTD_SERVER// }" ]; then
  echo "ALERTD_SERVER env variable is not set or empty, default to localhost:5001"
else
  echo "set alertd server host s/-H .* /-H $ALERTD_SERVER /g"
  echo -e "alertd_server_and_port=$ALERTD_SERVER" >> ${agent_install_folder}/agent/runner.conf
fi

mkdir -p "${altenv_cache_folder}"

log_info "creating download folder"
mkdir -p ${agent_install_folder}/download/unpack
abort_if_failed "failed to create ${agent_install_folder}/download/unpack"
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

if [ "$1" == "-update" ]; then
    log_info "override ${agent_install_folder}/agent/collectors/conf use by before"
    yes | cp -rf ${working_folder}/conf ${agent_install_folder}/agent/collectors
    yes | rm -rf ${working_folder}/conf
    yes | cp -f  ${working_folder}/cloudwiz-agent-bk-${current_time}/filebeat-1.3.1/filebeat.yml ${agent_install_folder}/filebeat-1.3.1
    yes | cp -f  ${working_folder}/cloudwiz-agent-bk-${current_time}/filebeat-1.3.1/filebeat.startup.sh ${agent_install_folder}/filebeat-1.3.1
    yes | cp -f  ${working_folder}/cloudwiz-agent-bk-${current_time}/altenv/etc/supervisord.conf ${agent_install_folder}/altenv/etc/
    echo "FB_HOME=${agent_install_folder}/filebeat-1.3.1" > /etc/default/filebeat
fi
# chown -hR "$agent_user" "${agent_install_folder}"
# abort_if_failed "failed to change ownership of ${agent_install_folder}/download to $agent_user"

log_info 'Done!'
log_info 'run "sudo /etc/init.d/cloudwiz-agent start" to start'
