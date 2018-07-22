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
"""Listens on a local TCP socket for incoming Metrics """

from __future__ import print_function

import socket
import os
import sys
import time
from collectors.lib import utils

try:
    from _thread import *
except ImportError:
    from thread import *

try:
    from collectors.etc import tcp_bridge_conf
except ImportError:
    print('unable to import tcp_bridge_conf', file=sys.stderr)
    tcp_bridge_conf = None

HOST = '127.0.0.1'
PORT = 4243

# metrics
m_namespace = 'tcollector.tcp_bridge.'
m_lines = 0
m_connections = 0
m_delay = 15
m_last = 0
m_ptime = 0

# buffered stdout seems to break metrics
out = os.fdopen(sys.stdout.fileno(), 'w', 0)

def main():
    if not (tcp_bridge_conf and tcp_bridge_conf.enabled()):
        print('not enabled, or tcp_bridge_conf unavilable', file=sys.stderr)
        sys.exit(13)
    utils.drop_privileges()

    def printm(string, time, value):
        out.write(m_namespace+string+' '+str(time)+' '+str(value)+'\n')

    def printmetrics():
        global m_delay
        global m_last

        ts = int(time.time())
        if ts > m_last+m_delay:
            printm('lines_read', ts, m_lines)
            printm('connections_processed', ts, m_connections)
            printm('processing_time', ts, m_ptime)
            printm('active', ts, 1)
            m_last = ts

    def clientthread(connection):
        global m_lines
        global m_connections
        global m_ptime

        start = time.time()
        f = connection.makefile()
        while True:
            data = f.readline()

            if not data:
                break

            data = removePut(data)
            out.write(data)
            m_lines += 1

        f.close()
        connection.close()

        end = time.time()
        m_ptime += (end - start)
        m_connections += 1
        printmetrics()

    def removePut(line):
        if line.startswith('put '):
            return line[4:]
        else:
            return line

    try:
        if tcp_bridge_conf.port():
            PORT = tcp_bridge_conf.port()

        if tcp_bridge_conf.host():
            HOST = tcp_bridge_conf.host()

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((HOST, PORT))
        sock.listen(1)

    except socket.error as msg:
        utils.err('could not open socket: %s' % msg)
        sys.exit(1)

    try:
        flush_delay = tcp_bridge_conf.flush_delay()
    except AttributeError:
        flush_delay = 60

    try:
        try:
            while 1:
                connection, address = sock.accept()
                start_new_thread(clientthread, (connection,))

        except KeyboardInterrupt:
            utils.err("keyboard interrupt, exiting")

    finally:
        sock.close()

if __name__ == "__main__":
    main()

sys.exit(0)
