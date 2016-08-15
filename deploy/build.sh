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
 log_info "$0 [-c] <path/to/tcollector/root>"
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
while getopts "c" flag; do
  case "$flag" in
    c) clean=true;;
    *) exit 1
  esac
done
if [ $(( $# - $OPTIND )) -lt 0 ]; then
  log_info 'wrong number of parameters'
  display_usage
  exit 1
fi
collector_source_path=${@:$OPTIND:1}
package_type=${@:$OPTIND+1:1}

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

log_info "setup agent/runner ${collector_source_path} => ${agent_collector_folder}"
rm -rf ${agent_collector_folder}/*
mkdir -p ${agent_collector_folder}/collectors
cp ${collector_source_path}/runner.py ${agent_collector_folder}/runner.py
abort_if_failed 'failed to copy runner.py'
cp ${collector_source_path}/runner.conf ${agent_collector_folder}/runner.conf
abort_if_failed 'failed to copy runner.conf'
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

log_info "copy cloudwiz-agent startup scripts ${basedir}/startup_scripts/"
yes | cp -f -r "${basedir}/startup_scripts" "${agent_install_folder}/" 
abort_if_failed 'failed to copy startup scripts'

tar -zcf ${basedir}/agent.tar.gz "$agent_install_folder"
abort_if_failed 'failed to add agent to tar file'

log_info "publish agent tarball to $publish_location/$os_type"
mkdir -p "$publish_location/$os_type"
scp "${basedir}/agent.tar.gz" "${publish_location}/$os_type/"

log_info "Done!"
