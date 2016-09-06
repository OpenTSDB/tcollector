#!/bin/bash
source common.sh

basedir=$(cd $(dirname $0); pwd)
agent_folder_name="agent"
workspace_folder="${basedir}/workspace"
agent_install_folder="/opt/cloudwiz-agent"
agent_install_folder_escaped="\/opt\/cloudwiz-agent"
agent_install_python_escaped="\/opt\/cloudwiz-agent\/altenv\/bin\/python"
altenv_folder="${agent_install_folder}/altenv"
altenv_bin_folder="${altenv_folder}/bin"
altenv_etc_folder="${altenv_folder}/etc"
altenv_var_folder="${altenv_folder}/var"
altenv_run_folder="${altenv_var_folder}/run"
altenv_log_folder="${altenv_var_folder}/log"
agent_collector_folder="${agent_install_folder}/$agent_folder_name"
publish_location="./releases"

function display_usage() {
 log_info "$0 [-c] [-s] <path/to/tcollector/root>"
}

function fix_python_recursively() {
  for i in "$1"/*; do
    if [ -d "$i" ];then
      fix_python_recursively "$i"
    elif [[ ${i: -3} == ".py" ]]; then
      sed -i "s/\#\!.*/\#\!${agent_install_python_escaped}/g" "$i"
    fi  
  done
}

os_type=$(get_os)

check_root

#parse cmdline
while getopts "cs" flag; do
  case "$flag" in
    c) clean=true;;
    s) skip=true;;
    *) exit 1
  esac
done
if [ $(( $# - $OPTIND )) -lt 0 ]; then
  log_info 'wrong number of parameters'
  display_usage
  exit 1
fi
collector_source_path=${@:$OPTIND:1}

if [[ -z $collector_source_path ]]; then
  log_info "collector source path root is not set"
  display_usage
  exit 1
fi

if [[ ! -d $collector_source_path ]]; then
  log_info "directory $collector_source_path does not exist"
  exit 1
fi

if [[ "$clean" = true ]]; then
  log_info "clean, remove ${workspace_folder}"
  rm -rf ${workspace_folder}
fi

if [[ ! "$skip" = true ]]; then
  log_info "recreate ${agent_install_folder}"
  rm -rf ${agent_install_folder}
  abort_if_failed "failed to delete ${agent_install_folder}"
  mkdir -p "${agent_collector_folder}"
  mkdir -p "${workspace_folder}"
  mkdir -p "${altenv_etc_folder}"
  mkdir -p "${altenv_run_folder}"
  mkdir -p "${altenv_log_folder}"

  log_info 'setup python environment'
  if [[ ! -f ${workspace_folder}/Python-2.7.11.tgz ]]; then
    log_info 'download python-2.7.11 package'
    wget --directory-prefix="${workspace_folder}" https://www.python.org/ftp/python/2.7.11/Python-2.7.11.tgz
    abort_if_failed 'failed to download python-2.7.11 package'
  fi
  tar -xzf "${workspace_folder}"/Python-2.7.11.tgz -C "${workspace_folder}"
  abort_if_failed 'failed to extract python-2.7.11 tarball'

  pushd "${workspace_folder}"/Python-2.7.11
  ./configure --prefix="${altenv_folder}"
  abort_if_failed 'python build: failed to run configure'
  make install
  abort_if_failed 'python build: failed to run make'
  popd
  log_info 'finish building python-2.7.11'

  log_info 'setup supervisord and its dependencies ...'
  log_info 'set up setuptools ...'
  if [[ ! -f ${workspace_folder}/setuptools-20.2.2.tar.gz ]]; then
    log_info 'download setuptools-20.2.2 tarball'
    wget --directory-prefix="${workspace_folder}" https://pypi.python.org/packages/source/s/setuptools/setuptools-20.2.2.tar.gz#md5=bf37191cb4c1472fb61e6f933d2006b1
    abort_if_failed 'failed to download setuptools-20.2.2 tarball' 
  fi
  tar -xzf "${workspace_folder}"/setuptools-20.2.2.tar.gz -C "${workspace_folder}"
  abort_if_failed 'failed to extract setuptools tarball'

  pushd "${workspace_folder}"/setuptools-20.2.2
  "${altenv_bin_folder}"/python setup.py install --prefix="${altenv_folder}"
  abort_if_failed 'failed to install setuptools'
  popd
  log_info 'finish setting up setuptools'

  log_info 'set up meld3 ...'
  if [[ ! -f ${workspace_folder}/meld3-0.6.5.tar.gz ]]; then
    log_info 'download meld3-0.6.5 tarball'
    wget --directory-prefix="${workspace_folder}" http://www.plope.com/software/meld3/meld3-0.6.5.tar.gz
    abort_if_failed 'failed to download meld3-0.6.5 tarball' 
  fi
  tar -xzf "${workspace_folder}"/meld3-0.6.5.tar.gz -C "${workspace_folder}"
  abort_if_failed 'failed to extract meld3-0.6.5 tarball'

  pushd "${workspace_folder}"/meld3-0.6.5
  "${altenv_bin_folder}"/python setup.py install --prefix="${altenv_folder}"
  abort_if_failed 'failed to install meld3'
  popd
  log_info 'finish setting up meld3'

  log_info 'set up elementtree ...'
  if [[ ! -f ${workspace_folder}/elementtree-1.2.6-20050316.tar.gz ]]; then
    log_info 'download elementtree-1.2.6-20050316 tarball'
    wget --directory-prefix="${workspace_folder}" http://effbot.org/media/downloads/elementtree-1.2.6-20050316.tar.gz
    abort_if_failed 'failed to download elementtree-1.2.6-20050316  tarball' 
  fi
  tar -xzf "${workspace_folder}"/elementtree-1.2.6-20050316.tar.gz -C "${workspace_folder}"
  abort_if_failed 'failed to extract elementtree-1.2.6-20050316.tar.gz tarball'

  pushd "${workspace_folder}"/elementtree-1.2.6-20050316
  "${altenv_bin_folder}"/python setup.py install --prefix="${altenv_folder}"
  abort_if_failed 'failed to install elementtree'
  popd
  log_info 'finish setting up elementtree'

  log_info 'set up supervisord ...'
  if [[ ! -f ${workspace_folder}/supervisor-3.2.2.tar.gz ]]; then
    log_info 'download supervisor-3.2.2 tarball'
    wget --directory-prefix="${workspace_folder}" https://pypi.python.org/packages/source/s/supervisor/supervisor-3.2.2.tar.gz#md5=bf1c8877f2ace04d62665a7c6e351219
    abort_if_failed 'failed to download supervisor-3.2.2 tarball' 
  fi
  tar -xzf "${workspace_folder}"/supervisor-3.2.2.tar.gz -C "${workspace_folder}"
  abort_if_failed 'failed to extract supervisor-3.2.2 tarball'

  pushd "${workspace_folder}"/supervisor-3.2.2
  "${altenv_bin_folder}"/python setup.py install --prefix="${altenv_folder}"
  abort_if_failed 'failed to install supervisor-3.2.2'
  popd

  yes | cp -f "${basedir}/supervisord.conf" "${altenv_etc_folder}/supervisord.conf"
  abort_if_failed "failed to copy supervisord conf file"
  sed -i "s/<basedir>/${agent_install_folder_escaped}/g" "${altenv_etc_folder}/supervisord.conf"
  abort_if_failed "failed to config supervisord.conf"
  log_info 'finish setting up supervisord and its dependencies'

  log_info 'set up psutil ...'
  if [[ ! -f ${workspace_folder}/psutil-2.1.3.tar.gz ]]; then
    log_info 'download psutil-2.1.3 tarball'
    wget --directory-prefix="${workspace_folder}" https://pypi.python.org/packages/source/p/psutil/psutil-2.1.3.tar.gz
  fi
  tar -xzf "${workspace_folder}"/psutil-2.1.3.tar.gz -C "${workspace_folder}"
  abort_if_failed 'failed to extact psutil'
  pushd "${workspace_folder}"/psutil-2.1.3
  "${altenv_bin_folder}"/python setup.py install --prefix="${altenv_folder}"
  abort_if_failed 'failed to install psutil'
  popd
  log_info 'finish setting up psutil'

  log_info 'set up python-gnupg ...'
  if [[ ! -f ${workspace_folder}/gnupg-2.0.2.tar.gz ]]; then
    log_info 'download gnupg-2.0.2 tarball'
    wget --directory-prefix="${workspace_folder}" https://pypi.python.org/packages/3d/91/0d1181069007854eb46eaa595be1d8c195e6213cff4750cbc1b79cf8c071/gnupg-2.0.2.tar.gz#md5=7ca1d438430428aac7bea1491b1c237e
  fi
  tar -xzf "${workspace_folder}"/gnupg-2.0.2.tar.gz -C "${workspace_folder}"
  abort_if_failed 'failed to extact gnupg'
  pushd "${workspace_folder}"/gnupg-2.0.2
  "${altenv_bin_folder}"/python setup.py install --prefix="${altenv_folder}"
  abort_if_failed 'failed to install gnupg'
  popd
  log_info 'finish setting up gnupg'

  log_info 'set up requests ...'
  if [[ ! -f ${workspace_folder}/python-requests.tar.gz ]]; then
    log_info 'download python-requests tarball'
    wget -O "${workspace_folder}/python-requests.tar.gz" https://github.com/kennethreitz/requests/tarball/master
  fi
  mkdir -p ${workspace_folder}/python-requests
  tar -xzf "${workspace_folder}"/python-requests.tar.gz -C "${workspace_folder}/python-requests" --strip-components=1
  abort_if_failed 'failed to extact python requests'
  pushd "${workspace_folder}"/python-requests
  "${altenv_bin_folder}"/python setup.py install --prefix="${altenv_folder}"
  abort_if_failed 'failed to install python requests'
  popd
  log_info 'finish setting up requests'

fi

log_info "setup agent/runner ${collector_source_path} => ${agent_collector_folder}"
rm -rf ${agent_collector_folder}/*
mkdir -p ${agent_collector_folder}/collectors
cp ${collector_source_path}/runner.py ${agent_collector_folder}/runner.py
abort_if_failed 'failed to copy runner.py'
cp ${collector_source_path}/runner.conf ${agent_collector_folder}/runner.conf
abort_if_failed 'failed to copy runner.conf'
cp ${collector_source_path}/common_utils.py ${agent_collector_folder}/common_utils.py
abort_if_failed 'failed to copy common_utils.py'
cp ${collector_source_path}/run ${agent_collector_folder}/run
abort_if_failed 'failed to copy run'
sed -i "s/^\.\/runner/${agent_install_folder_escaped}\/agent\/runner/g" ${agent_collector_folder}/run
sed -i "/^log_folder=/c\log_folder=${agent_install_folder}/altenv/var/log" ${agent_collector_folder}/run
sed -i "/^run_folder=/c\run_folder=${agent_install_folder}/altenv/var/run" ${agent_collector_folder}/run
cp ${collector_source_path}/collectors/__init__.py ${agent_collector_folder}/collectors/__init__.py
abort_if_failed 'failed to copy collectors/__init__.py'
cp -al ${collector_source_path}/collectors/builtin ${agent_collector_folder}/collectors/builtin
abort_if_failed 'failed to copy-archive collectors/builtin'
cp -al ${collector_source_path}/collectors/conf ${agent_collector_folder}/collectors/conf
abort_if_failed 'failed to copy-archive collectors/conf'
cp -al ${collector_source_path}/collectors/lib ${agent_collector_folder}/collectors/lib
abort_if_failed 'failed to copy-archive collectors/lib'
log_info 'modify python file scripts path'
fix_python_recursively ${agent_collector_folder}
log_info 'finish setting up agent/tcollector'

log_info "set up uagent"
yes | cp -f -r "${collector_source_path}/uagent" "${agent_install_folder}/"
abort_if_failed "failed to copy ${basedir}/uagent to ${agent_install_folder}/"
sed -i "/^server_base *= */c\server_base = https://github.com/wangy1931/tcollector/tree/uagent-deploy/deploy/releases" "${agent_install_folder}/uagent/uagent.conf"
sed -i "/^install_root *= */c\install_root = ${agent_install_folder}" "${agent_install_folder}/uagent/uagent.conf"
sed -i "/^platform *= */c\platform = ${os_type}" "${agent_install_folder}/uagent/uagent.conf"
abort_if_failed "failed to config uagent.conf"
sed -i "/^log_folder=/c\log_folder=${agent_install_folder}/altenv/var/log" "${agent_install_folder}/uagent/run-uagent.sh"
sed -i "s/^\.\/daemon/${agent_install_folder_escaped}\/uagent\/daemon/g" "${agent_install_folder}/uagent/run-uagent.sh"
abort_if_failed "failed to config run-uagent.sh"
fix_python_recursively "${agent_install_folder}/uagent"
abort_if_failed "failed to fix python env ${agent_install_folder}/uagent"
cp -f "${collector_source_path}"/version.json "${agent_install_folder}/"
abort_if_failed "failed to copy ${collector_source_path}/version.json ${agent_install_folder}/"
mkdir -p ${agent_install_folder}/.gnupg
abort_if_failed "failed to create ${agent_install_folder}/.gnupg"
yes | cp -f ~/.gnupg/pubring.gpg ${agent_install_folder}/.gnupg
yes | cp -f ~/.gnupg/trustdb.gpg ${agent_install_folder}/.gnupg
abort_if_failed "failed to copy gpg files"
log_info "finish setting up uagent"

log_info "copy common_utils.py"
yes | cp -f "${collector_source_path}/common_utils.py"  "${agent_install_folder}"
abort_if_failed "failed to cp ${collector_source_path}/common_utils.py ${agent_install_folder}"
log_info "finish copying common_utils.py"

log_info "copy cloudwiz-agent startup scripts ${basedir}/startup_scripts/"
yes | cp -f -r "${basedir}/startup_scripts" "${agent_install_folder}/" 
abort_if_failed 'failed to copy startup scripts'

tar -zcf ${basedir}/agent.tar.gz "$agent_install_folder"
abort_if_failed 'failed to add agent to tar file'

log_info "publish agent tarball to $publish_location/$os_type"
mkdir -p "$publish_location/$os_type"
scp "${basedir}/agent.tar.gz" "${publish_location}/$os_type/"
mkdir -p /tmp/publish/$os_type
scp "${basedir}/agent.tar.gz" /tmp/publish/$os_type

log_info "Done!"
