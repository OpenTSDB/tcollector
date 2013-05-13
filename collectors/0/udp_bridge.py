#!/usr/bin/python
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
"""Listens on a local UDP socket for incoming Metrics """

import socket
import sys

HOST = '127.0.0.1'
PORT = 8953
SIZE = 8192
TIMEOUT = 1
s = None

try:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((HOST, PORT))
except socket.error as msg:
    s = None

if s is None:
    sys.stderr.write('could not open socket')
    sys.exit(1)

try:
    while 1:
        data, address = s.recvfrom(SIZE)
        if not data:
            sys.stderr.write("invalid data\n")
            break
        print data
except KeyboardInterrupt:
    sys.stderr.write("keyboard interrupt, exiting\n")
finally:
    s.close()

sys.exit(0)
