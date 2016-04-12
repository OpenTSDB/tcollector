#!/usr/bin/env python
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
import time
from collectors.lib import utils

try:
  from collectors.etc import udp_bridge_conf
except ImportError:
  udp_bridge_conf = None

HOST = '127.0.0.1'
PORT = 8953
SIZE = 8192

def main():
    if not (udp_bridge_conf and udp_bridge_conf.enabled()):
      sys.exit(13)
    utils.drop_privileges()

    def removePut(line):
        if line.startswith('put '):
            return line[4:]
        else:
            return line

    try:
        if (udp_bridge_conf and udp_bridge_conf.usetcp()):
          sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
          sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((HOST, PORT))
    except socket.error, msg:
        utils.err('could not open socket: %s' % msg)
        sys.exit(1)

    try:
        flush_delay = udp_bridge_conf.flush_delay()
    except AttributeError:
        flush_delay = 60

    flush_timeout = int(time.time())
    try:
        try:
            while 1:
                data, address = sock.recvfrom(SIZE)
                if data:
                    lines = data.splitlines()
                    data = '\n'.join(map(removePut, lines))
                if not data:
                    utils.err("invalid data")
                    break
                print data
                now = int(time.time())
                if now > flush_timeout:
                    sys.stdout.flush()
                    flush_timeout = now + flush_delay

        except KeyboardInterrupt:
            utils.err("keyboard interrupt, exiting")
    finally:
        sock.close()

if __name__ == "__main__":
    main()

sys.exit(0)
