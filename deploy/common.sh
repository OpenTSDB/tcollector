 #!/bin/bash

color_red=$(tput setaf 1)
color_normal=$(tput sgr0)
color_blue=$(tput setaf 4)

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
  printf "${color_blue}$1.${color_normal}\n"
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

