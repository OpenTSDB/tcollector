#!/bin/bash
# Copyright (C) 2013  The tcollector Authors.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.

agents='
Acl
Arp
Bfd
CliSessionMgr
ConfigAgent
Ebra
Fru
Ira
LacpTxAgent
Lag
Lldp
Rib
SandCounters
SandFabric
SandFap
SandMact
Sflow
Snmp
Stp
StpTopology
Strata
StrataL2
StrataL3
SuperServer
Sysdb
TopoAgent
XcvrAgent
ribd
'

while :; do
  for task in $agents; do
    usercpu=0
    systcpu=0
    for pid in `pidof $task`; do
      ts=`date +%s`
      eval `awk '{print "ppid=" $4 ";usercpu=$((usercpu+" $14 ")); systcpu=$((systcpu+" $15 "));"}' /proc/$pid/stat`
      if fgrep -q $task /proc/$ppid/stat; then
        continue  # We are a fork of the agent.
      fi
    done
    echo "proc.stat.cpu.task $ts $usercpu type=user task=$task"
    echo "proc.stat.cpu.task $ts $systcpu type=system task=$task"
  done
  sleep 5
done
