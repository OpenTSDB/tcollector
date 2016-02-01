#!/bin/bash
# This file is part of tcollector.
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

# Collects OpenTSDB's own stats.

TSD_HOST=localhost
TSD_PORT=4242
COLLECTION_INTERVAL=15

nc -z $TSD_HOST $TSD_PORT >/dev/null || exit 13

while :; do
  echo stats || exit
  sleep $COLLECTION_INTERVAL
done | nc $TSD_HOST $TSD_PORT
