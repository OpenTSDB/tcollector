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
from socket import timeout

COLLECTION_INTERVAL = 15
SCAN_INTERVAL = 600

KEYS = frozenset( [
		  'zk_avg_latency',
		  'zk_max_latency',
		  'zk_min_latency',
		  'zk_packets_received',
		  'zk_packets_sent',
		  'zk_num_alive_connections',
		  'zk_outstanding_requests',
		  'zk_approximate_data_size',
		  'zk_open_file_descriptor_count',
		  ] )
								
def err(e):
	print >>sys.stderr, e

def scan_zk_instances():
	""" Finding out all the runnings instances of zookeeper 
	    - Using netstat, finds out all listening java processes.	 
	    - By sending "ruok" string to each listening port, figures out whether zookeeper is running on that port based on the reply.
	"""

	instances = []
	listen_proc = subprocess.Popen(["netstat", "-lnpt"], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
	stdout, stderr = listen_proc.communicate()
	if listen_proc.returncode != 0:
		err("Failed to find any listening process: %r" % listen_proc.returncode)
		return []

	for line in stdout.split("\n"):
		if not 'java' in line:
			continue
		listen_sock = line.split()[3]
		ip = str(listen_sock.split(":")[0])
		port = int(listen_sock.split(":")[1])
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
			instances.append(port)
	return instances 

def main():
	last_scan = time.time()
	instances = scan_zk_instances()
	if not len(instances):
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
				sock.connect(('127.0.0.1', port))
			except:
				err("zk instance running at port %d went away" % port)
			sock.send("mntr\n")
			data = sock.recv(1024)
			for stat in data.splitlines():
				metric = stat.split()[0]
				value = stat.split()[1]
				if metric in KEYS:
					print_stat(metric, value, tags)

		time.sleep(COLLECTION_INTERVAL)
	
if __name__ == "__main__":
	sys.exit(main())	
