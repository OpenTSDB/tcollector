#!/usr/bin/env python
#
# This file is part of tcollector.
# Copyright (C) 2016  The tcollector Authors.
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

def get_settings():
  return {
    "db": "db1,db2",
    "config": "192.168.0.10:27017,192.168.0.11:27017",
    "mongos": "192.168.0.13:27017",
    "replica": "192.168.0.14:27017",
    "username": "",
    "password": "",
    "interval": 15
  }

