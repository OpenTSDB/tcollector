#!/bin/bash
# Copyright (C) 2015  The tcollector Authors.
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

while :; do
  for task in Sysdb Arp Ebra Ira Stp StpTopology TopoAgent ribd Strata StrataL2 StrataL3 Snmp CliSessionMgr; do
    for pid in `pidof $task`; do
      ts=`date +%s`
      eval `awk '{print "ppid=" $4 ";rss=" ($24*4096) "; vsize=" $23 ";"}' /proc/$pid/stat`
      if fgrep -q $task /proc/$ppid/stat; then
        continue  # We are a fork of the agent.
      fi
      echo "proc.stat.mem.rss $ts $rss task=$task"
      echo "proc.stat.mem.vsize $ts $vsize task=$task"
    done
  done
  sleep 5
done
