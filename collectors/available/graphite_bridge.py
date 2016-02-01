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
"""Listens on a local TCP socket for incoming metrics in the graphite protocol."""

from __future__ import print_function

import sys
from collectors.lib import utils
import SocketServer
import threading

try:
  from collectors.etc import graphite_bridge_conf
except ImportError:
  graphite_bridge_conf = None

HOST = '127.0.0.1'
PORT = 2003
SIZE = 8192

class GraphiteServer(SocketServer.ThreadingTCPServer):
    allow_reuse_address = True

    print_lock = threading.Lock()

class GraphiteHandler(SocketServer.BaseRequestHandler):

    def handle_line(self, line):
        line_parts = line.split()
        with self.server.print_lock:
            if len(line_parts) != 3:
                print("Bad data:", line, file=sys.stderr)
            else:
                print(line_parts[0], line_parts[2], line_parts[1])


    def handle(self):
        data = ''
        while True:
            new_data = self.request.recv(SIZE)
            if not new_data:
                break
            data += new_data

            if "\n" in data:
                line_data, data = data.rsplit("\n", 1)
                lines = line_data.splitlines()

                for line in lines:
                    self.handle_line(line)

        self.request.close()


def main():
    if not (graphite_bridge_conf and graphite_bridge_conf.enabled()):
      sys.exit(13)
    utils.drop_privileges()

    server = GraphiteServer((HOST, PORT), GraphiteHandler)
    server.daemon_threads = True
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
        server.server_close()

if __name__ == "__main__":
    main()

sys.exit(0)
