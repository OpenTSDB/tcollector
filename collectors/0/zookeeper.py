#!/usr/bin/python

""" 
Zookeeper collector

Refer to the following zookeeper commands documentation for details:
http://zookeeper.apache.org/doc/trunk/zookeeperAdmin.html#sc_zkCommands
"""

import sys
import socket
import time
import subprocess
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
        listen_sock = subprocess.check_output(["netstat", "-lnpt"], stderr=subprocess.PIPE)
    except subprocess.CalledProcessError:
        utils.err("netstat directory doesn't exist in PATH variable")
        return instances

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
                try:
                    if tcp_version == "tcp6":
                        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                    else:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(0.5)
                    sock.connect((ip, port))
                    sock.send("ruok\n")
                    data = sock.recv(1024)
                except:
                    pass
                finally:
                    sock.close()
                if data == "imok":	
                    instances.append([ip, port, tcp_version])
                    data = ""
        except:
            continue
        finally:
            fd.close()
    return instances 

def print_stat(metric, ts, value, tags=""):
    if value is not None:
        print "zookeeper.%s %i %s %s" % (metric, ts, value, tags)

def main():
    if USER != "root":
        utils.drop_privileges(user=USER)

    last_scan = time.time()
    instances = scan_zk_instances()

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
            if tcp_version == "tcp6":
                sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect((ip, port))
            except:
                utils.err("ZK Instance listening at port %d went away" % port)
                instances.remove([ip, port, tcp_version])
                break

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
