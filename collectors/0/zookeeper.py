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

COLLECTION_INTERVAL = 15
SCAN_INTERVAL = 600

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
								
def err(e):
    print >> sys.stderr, e

def scan_zk_instances():
    """ 
    Finding out all the runnings instances of zookeeper 
    - Using netstat, finds out all listening java processes.	 
    - Figures out ZK instances among java processes by looking for the 
      string "org.apache.zookeeper.server.quorum.QuorumPeerMain" in cmdline.
    """

    instances = {}
    try:
        listen_sock = subprocess.check_output(["netstat", "-lnpt"])
    except subprocess.CalledProcessError:
        err("netstat directory doesn't exist in PATH variable")
        return instances

    for line in listen_sock.split("\n"):
        if not "java" in line:
            continue
        listen_sock = line.split()[3]
        ip = str(listen_sock.split(":")[0])
        port = int(listen_sock.split(":")[1])
        pid = int(line.split()[6].split("/")[0])
        try:
            fd = open("/proc/%d/cmdline" % pid)
            cmdline = fd.readline()
            if "org.apache.zookeeper.server.quorum.QuorumPeerMain" in cmdline:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    sock.settimeout(0.5)
                    sock.connect((ip, port))
                    sock.send("ruok\n")
                    data = sock.recv(1024)
                except:
                    pass
                finally:
                    sock.close()
                if data == "imok":	
                    instances[port] = ip
                    data = ""
        except:
            err("Java Process (pid %d) listening at port %d went away" % (pid, port))
            continue
        finally:
            fd.close()
    return instances 

def main():
    last_scan = time.time()
    instances = scan_zk_instances()
    if not instances:
        return 13			# Ask tcollector not to respawn us

    def print_stat(metric, value, tags=""):
        if value is not None:
            print "zookeeper.%s %i %s %s" % (metric, ts, value, tags)

    while True:
        ts = time.time()

        # We haven't looked for zookeeper instance recently, let's do that	
        if ts - last_scan > SCAN_INTERVAL:
            instances = scan_zk_instances()
            last_scan = ts

        # Iterate over every zookeeper instance and get statistics	
        for port in instances:
            tags = "port=%s" % port
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect((instances[port], port))
            except:
                err("ZK Instance listening at port %d went away" % port)
            sock.send("mntr\n")
            data = sock.recv(1024)
            for stat in data.splitlines():
                metric = stat.split()[0]
                value = stat.split()[1]
                if metric in KEYS:
                    print_stat(metric, value, tags)
            sock.close()

        time.sleep(COLLECTION_INTERVAL)
	
if __name__ == "__main__":
    sys.exit(main())	
