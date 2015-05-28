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

while :; do
  for task in Sysdb Arp Ebra Ira Stp StpTopology TopoAgent ribd; do
    for pid in `pidof $task`; do
      ts=`date +%s`
      eval `awk '{print "ppid=" $4 ";usercpu=" $14 "; systcpu=" $15 ";"}' /proc/$pid/stat`
      if fgrep -q $task /proc/$ppid/stat; then
        continue  # We are a fork of the agent.
      fi
      echo "proc.stat.cpu.task $ts $usercpu type=user task=$task"
      echo "proc.stat.cpu.task $ts $systcpu type=system task=$task"
    done
  done
  sleep 5
done
