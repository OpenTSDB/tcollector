#!/bin/bash
# This file is part of tcollector.
# Copyright (C) 2010  StumbleUpon, Inc.
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
#
# demonstrates using a bash script that doesn't loop, this should really not
# be done this way.  spawns a process to spit out uptime which is useless.
#

UPTIME=$(</proc/uptime)
UP=${UPTIME% *}
IDLE=${UPTIME#* }
NOW=$(date +%s)

echo proc.uptime.total $NOW $UP
echo proc.uptime.idle $NOW $IDLE
