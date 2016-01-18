#!/usr/bin/python
"""Send docker stats counters to TSDB"""

import json
import socket
import subprocess
import sys
import time
import re

#print(sys.path)

from collectors.lib import utils

# create a socket
client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
client.connect("/var/run/docker.sock")
client.settimeout(5)

interval = 30 # seconds

recv_data_size = 4096

# This prefix will be prepended to all metric names before being sent
metric_prefix = "docker"

def main():
    utils.drop_privileges()
    bad_regex = re.compile("[,()]+")  # avoid forbidden by TSD symbols
    
    while True:
        # Get a list of container names

        for containerName in ():         
            queryContainerStats(containerName)
            
        sys.stdout.flush()
        time.sleep(interval)

def queryContainerStats(containerName):
    try:
        # prepare command to query docker daemon.
        #cmd = "echo -e \"GET /containers/%s/stats?stream=0 HTTP/1.1\\r\\n\" | nc -q 5 -U /var/run/docker.sock" % containerName
        #cmd = "echo -e \"GET /info HTTP/1.1\\r\\n\" | echo -e \"\\n\" | nc -q 5 -U /var/run/docker.sock"

        httpRequest = "GET /containers/%s/stats?stream=0 HTTP/1.1\r\n" % containerName
        print("cmd=%s" % httpRequest)

        client.send(httpRequest)
        client.send("\n")        
        print("Done with executing cmd!")

        result = client.recv(recv_data_size)

        print("Result:%s" % result)

        process(result)
    except Exception, e:
        sys.stderr.write("Error: %s\n" % e)    
    
def process(content):
    ts = time.time()
    
    # message content is as following:

    #HTTP/1.1 200 OK
    #Content-Type: application/json
    #Server: Docker/1.9.1 (linux)
    #Date: Sun, 17 Jan 2016 06:16:55 GMT
    #Content-Length: 1638

    #{"read":"2016-01-16T22:16:55.349404243-08:00","precpu_stats":{"cpu_usage":{"total_usage":68031969,"percpu_usage":[28569353,22696001,3625094,13141521],"usage_in_kernelmode":20000000,"usage_in_usermode":40000000},"system_cpu_usage":81825360000000,"throttling_data":{"periods":0,"throttled_periods":0,"throttled_time":0}},"cpu_stats":{"cpu_usage":{"total_usage":68031969,"percpu_usage":[28569353,22696001,3625094,13141521],"usage_in_kernelmode":20000000,"usage_in_usermode":40000000},"system_cpu_usage":81829300000000,"throttling_data":{"periods":0,"throttled_periods":0,"throttled_time":0}},"memory_stats":{"usage":557056,"max_usage":8781824,"stats":{"active_anon":540672,"active_file":4096,"cache":57344,"hierarchical_memory_limit":18446744073709551615,"inactive_anon":12288,"inactive_file":0,"mapped_file":0,"pgfault":5673,"pgmajfault":0,"pgpgin":2042,"pgpgout":1906,"rss":499712,"rss_huge":0,"total_active_anon":540672,"total_active_file":4096,"total_cache":57344,"total_inactive_anon":12288,"total_inactive_file":0,"total_mapped_file":0,"total_pgfault":5673,"total_pgmajfault":0,"total_pgpgin":2042,"total_pgpgout":1906,"total_rss":499712,"total_rss_huge":0,"total_unevictable":0,"total_writeback":0,"unevictable":0,"writeback":0},"failcnt":0,"limit":12268515328},"blkio_stats":{"io_service_bytes_recursive":[],"io_serviced_recursive":[],"io_queue_recursive":[],"io_service_time_recursive":[],"io_wait_time_recursive":[],"io_merged_recursive":[],"io_time_recursive":[],"sectors_recursive":[]},"networks":{"eth0":{"rx_bytes":6299,"rx_packets":77,"rx_errors":0,"rx_dropped":0,"tx_bytes":648,"tx_packets":8,"tx_errors":0,"tx_dropped":0}}}

    contentSplit = content.strip().split('\n')
    
    print contentSplit
    print "---end of print contentSplit"

    stats = json.loads(contentSplit[len(contentSplit) -1])
    # Parse the json object

    print stats["read"]
    print "cpu_total_usage", stats["precpu_stats"]["cpu_usage"]["total_usage"]
    print "cpu_total_usage", stats["precpu_stats"]["cpu_usage"]["usage_in_usermode"]
    print "cpu_total_usage", stats["precpu_stats"]["cpu_usage"]["usage_in_kernelmode"]

def test():
    name = "host1"
    queryContainerStats(name)

def dryrun():
    while(True):
        main()
    time.sleep(10)

if __name__ == "__main__":
  #sys.exit(main())
  test()
