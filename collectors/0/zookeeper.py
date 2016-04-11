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

import sys
import socket
import time
from subprocess import Popen, PIPE, CalledProcessError
import re
from collectors.lib import utils

COLLECTION_INTERVAL = 15  # seconds

# Every SCAN_INTERVAL seconds, we look for new zookeeper instances.
# Prevents the situation where you put up a new instance and we never notice.
SCAN_INTERVAL = 600

# If we are root, drop privileges to this user, if necessary.
# NOTE: if this is not root, this MUST be the user that you run zookeeper
# server under. If not, we will not be able to find your Zookeeper instances.
USER = "root"

KEYS = frozenset([
    "zk_avg_latency",
    "zk_max_latency",
    "zk_min_latency",
    "zk_packets_received",
    "zk_packets_sent",
    "zk_num_alive_connections",
    "zk_outstanding_requests",
    "zk_approximate_data_size",
    "zk_open_file_descriptor_count",
    ])

def scan_zk_instances():
    """ 
    Finding out all the running instances of zookeeper
    - Using netstat, finds out all listening java processes.	 
    - Figures out ZK instances among java processes by looking for the 
      string "org.apache.zookeeper.server.quorum.QuorumPeerMain" in cmdline.
    """

    instances = []
    try:
        netstat = Popen(["netstat", "-lnpt"], stderr=PIPE, stdout=PIPE)
        ret = netstat.wait()
        if ret:
            raise CalledProcessError(ret, "netstat -lnpt", "netstat returned code %i" % ret)
        listen_sock = netstat.stdout.read()
    except OSError:
        utils.err("netstat is not in PATH")
        return instances
    except CalledProcessError, err:
        utils.err("Error: %s" % err)

    for line in listen_sock.split("\n"):
        if not "java" in line:
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
                try:
                    sock = connect_socket(tcp_version, port)
                    sock.settimeout(0.5)
                    sock.send("ruok\n")
                    data = sock.recv(1024)
                except Exception, err:
                    utils.err(err)
                finally:
                    if sock: 
                        sock.close()
                if data == "imok":	
                    instances.append([ip, port, tcp_version])
                    data = ""
        except Exception, err:
            utils.err(err)
        finally:
            fd.close()
    return instances 

def print_stat(metric, ts, value, tags=""):
    if value is not None:
        print "zookeeper.%s %i %s %s" % (metric, ts, value, tags)

def connect_socket(tcp_version, port):
    sock = None
    if tcp_version == "tcp6":
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        ipaddr = '::1'
    else:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ipaddr = '127.0.0.1'
    try:
        sock.connect((ipaddr, port))
    except Exception, err:
        utils.err(err)
    return sock

def main():
    if USER != "root":
        utils.drop_privileges(user=USER)

    last_scan = time.time() - SCAN_INTERVAL

    while True:
        ts = time.time()

        # We haven't looked for zookeeper instance recently, let's do that
        if ts - last_scan > SCAN_INTERVAL:
            instances = scan_zk_instances()
            last_scan = ts

        if not instances:
            return 13  # Ask tcollector not to respawn us

        # Iterate over every zookeeper instance and get statistics
        for ip, port, tcp_version in instances:
            tags = "port=%s" % port

            sock = connect_socket(tcp_version, port)
            if sock is None:
                continue

            sock.send("mntr\n")
            data = sock.recv(1024)
            for stat in data.splitlines():
                metric = stat.split()[0]
                value = stat.split()[1]
                if metric in KEYS:
                    print_stat(metric, ts, value, tags)
            sock.close()

        time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    sys.exit(main())	
