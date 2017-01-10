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
#

""" 
Zookeeper collector

Refer to the following zookeeper commands documentation for details:
http://zookeeper.apache.org/doc/trunk/zookeeperAdmin.html#sc_zkCommands
"""

import socket
import time
from subprocess import Popen, PIPE, CalledProcessError
import re
from collectors.lib import utils
from collectors.lib.collectorbase import CollectorBase


class Zookeeper(CollectorBase):
    def __init__(self, config, logger, readq):
        super(Zookeeper, self).__init__(config, logger, readq)
        self.KEYS = frozenset([
            "zk_avg_latency",
            "zk_max_latency",
            "zk_min_latency",
            "zk_packets_received",
            "zk_packets_sent",
            "zk_num_alive_connections",
            "zk_outstanding_requests",
            "zk_approximate_data_size",
            "zk_open_file_descriptor_count",
            "zk_max_open_file_descriptor_count",
            "zk_znode_count",
            "zk_watch_count",
            "zk_ephemerals_count",
            "zk_server_state",
            "zk_followers",
            "zk_synced_followers",
            "zk_pending_syncs"
        ])
        self.scan_interval = int(self.get_config("SCAN_INTERVAL", 600))
        self.last_scan = time.time() - self.scan_interval
        self.instances = []

    def __call__(self):
        ts = time.time()

        # We haven't looked for zookeeper instance recently, let's do that
        if ts - self.last_scan > self.scan_interval:
            self.instances = self.scan_zk_instances()
            self.last_scan = ts

        if not self.instances:
            self._readq.nput("zookeeper.state %s %s" % (int(time.time()), '1'))
            self.log_warn("no zookeeper instance found")
            return 13

        # Iterate over every zookeeper instance and get statistics
        for ip, port, tcp_version in self.instances:
            tags = "port=%s" % port

            sock = self.connect_socket(tcp_version, port)
            if sock is None:
                continue

            try:
                sock.send("mntr\n")
                data = sock.recv(1024)
                for stat in data.splitlines():
                    metric = stat.split()[0]
                    if metric in self.KEYS:
                        try:
                            valStr = stat.split()[1]
                            if metric == "zk_server_state":
                                value = self.convert_server_state_to_int(valStr)
                                self._readq.nput("%s %d %d %s" % (metric, ts, value, tags))
                            else:                           
                                value = int(valStr)
                                self._readq.nput("%s %d %d %s" % (metric, ts, value, tags))

                        except ValueError:
                            self.log_exception("failed to extract value for metric %s", metric)
                    else:
                        self.log_warn("%s not in KEYS" % (metric))
                        
            finally:
                sock.close()
            self._readq.nput("zookeeper.state %s %s" % (int(time.time()), '0'))

    def scan_zk_instances(self):
        """
        Finding out all the running instances of zookeeper
        - Using netstat, finds out all listening java processes.
        - Figures out ZK instances among java processes by looking for the
          string "org.apache.zookeeper.server.quorum.QuorumPeerMain" in cmdline.
        """
        self.log_info("scan for zookeeper instance")
        instances = []
        listen_sock = None
        try:
            netstat = Popen(["netstat", "-lnpt"], stderr=PIPE, stdout=PIPE)
            ret = netstat.wait()
            if ret:
                raise CalledProcessError(ret, "netstat -lnpt", "netstat returned code %i" % ret)
            listen_sock = netstat.stdout.read()
        except OSError:
            self._readq.nput("zookeeper.state %s %s" % (int(time.time()), '1'))
            self.log_exception("netstat is not in PATH")
            return instances
        except CalledProcessError:
            self._readq.nput("zookeeper.state %s %s" % (int(time.time()), '1'))
            self.log_exception("Error run netstat in subprocess")
            return instances

        for line in listen_sock.split("\n"):
            if "java" not in line:
                continue
            listen_sock = line.split()[3]
            tcp_version = line.split()[0]

            m = re.match("(.+):(\d+)", listen_sock)
            ip = m.group(1)
            port = int(m.group(2))

            pid = int(line.split()[6].split("/")[0])
            try:
                fd = open("/proc/%d/cmdline" % pid)
                cmdline = fd.readline()
                if "org.apache.zookeeper.server.quorum.QuorumPeerMain" in cmdline:
                    data = None
                    sock = None
                    try:
                        sock = self.connect_socket(tcp_version, port)
                        sock.settimeout(0.5)
                        sock.send("ruok\n")
                        data = sock.recv(1024)
                    except Exception as e:
                        self.log_warn(
                            "exception [%s] when connecting to zookeeper, tcp version %s, port %d. this maybe ok",
                            e, tcp_version, port)
                    finally:
                        if sock:
                            sock.close()
                    if data == "imok":
                        instances.append([ip, port, tcp_version])
            except Exception:
                self._readq.nput("zookeeper.state %s %s" % (int(time.time()), '1'))
                self.log_exception("error opening /proc/%d/cmdline", pid)
            finally:
                fd.close()
        return instances

    def connect_socket(self, tcp_version, port):
        if tcp_version == "tcp6":
            sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            ipaddr = '::1'
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ipaddr = '127.0.0.1'
        try:
            sock.connect((ipaddr, port))
        except Exception:
            self._readq.nput("zookeeper.state %s %s" % (int(time.time()), '1'))
            self.log_exception("exception when connecting to zookeeper")
        return sock

    def convert_server_state_to_int(self, server_state):
        if server_state == "leader":
           return 1
        elif server_state == "follower":
           return 2
        elif server_state == "observer":
           return 3
        elif server_state == "standalone":
           return 0
        else:
           return -1
  
if __name__ == "__main__":
    from Queue import Queue

    inst = Zookeeper(None, None, Queue())
    inst()
