#!/bin/bash

alias cwlog="vim /opt/cloudwiz-agent/altenv/var/log/collector.log"
alias cwtail="tail -f /opt/cloudwiz-agent/altenv/var/log/collector.log"
alias cwpy="/opt/cloudwiz-agent/altenv/bin/python"
alias cwlist="/opt/cloudwiz-agent/altenv/agent/collector_mgr.py list"

function cwenable() {
  /opt/cloudwiz-agent/altenv/agent/collector_mgr.py enable $1
}