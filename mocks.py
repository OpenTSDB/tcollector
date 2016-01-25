# This file is part of tcollector.
# Copyright (C) 2014  The tcollector Authors.
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

import sys
import traceback
import socket
# for debugging
real_stderr = sys.stderr

class SocketDone(Exception):
    pass

class Socket():
    def __init__(self):
        self.AF_INET = 0
        self.SOCK_DGRAM = 0
        self.error = Exception
        self.state = { 'udp_in': [] }
        self._socketSingleton = self.SocketSingleton(self.state)

    def socket(self, *ignoredArgs):
        return self._socketSingleton

    class SocketSingleton():
        def __init__(self, state):
            self.state = state
            self.connect_called = False
            self.send_buff = ''
            self._sock = self

        def bind(self, host_and_port):
            return None

        def close(self):
            return None

        def recvfrom(self, inBytes):
            if (len(self.state['udp_in']) > 0):
                line = self.state['udp_in'].pop(0)
                return (line, None)
            else:
                raise SocketDone('stop reading from socket')

        def connect(self, sockaddr):
            self.connect_called = True

        def send(self, data):
            self.send_buff += data

        def recv(self, size):
            return "HTTP 200 Connection Established"

        def getsockopt(self, *args):
            return socket.SOCK_STREAM

class Sys():
    def __init__(self):
        self.stderr = self.Stderr()
        self.stdout = self.Stdout()

    def exit(self, exitCode):
        err = "\n".join(self.stderr.lines)
        trace = traceback.format_exc()
        msg = 'exit called with code %s\n  stderr: %s\n  trace: %s'
        raise Exception(msg % (exitCode, err, trace))

    class Stderr():
        def __init__(self):
            self.lines = []

        def write(self, outString):
            self.lines.append(outString)

    class Stdout():
        def __init__(self):
            self.lines = []

        def write(self, outString):
            self.lines.append(outString)

class Utils():
    def __init__(self):
        self.drop_privileges = lambda: None

    def err(self, msg):
        sys.stderr.write("%s\n" % msg)
