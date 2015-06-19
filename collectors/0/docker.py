#!/usr/bin/python
# More informations on https://docs.docker.com/articles/runmetrics/
"""Imports Docker stats from /sys/fs/cgroup."""

import os
import re
import socket
import sys
import time
import json

from collectors.lib import utils

COLLECTION_INTERVAL = 15  # seconds

# proc_names example:
# $ cat cpuacct.stat
# user 58
# system 72
proc_names = {
    "cpuacct.stat": (
  "user", "system",
    ),
    "memory.stat": (
  "cache", "rss", "mapped_file", "pgfault", "pgmajfault", "swap", "active_anon",
        "inactive_anon", "active_file", "inactive_file", "unevictable",
        "hierarchical_memory_limit", "hierarchical_memsw_limit",
    ),
}

# proc_names_to_agg example:
# $ cat blkio.io_service_bytes
# 8:0 Read 8523776
# 8:0 Write 1048576
# ...
# 8:1 Read 4223776
# 8:1 Write 1042576
# ...
proc_names_to_agg = {
    "blkio.io_service_bytes": (
        "Read", "Write",
    ),
}

def getnameandimage(containerid):

    # Retrieve container json configuration file
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        r = sock.connect_ex('/var/run/docker.sock')
        if (r != 0):
            print >>sys.stderr, "Can not connect to /var/run/docker.sock"
        else:
            message = 'GET /containers/' + containerid + '/json HTTP/1.1\n\n'
            sock.sendall(message)
            json_data = ""
            # "\r\n0\r\n" is raised on last chunk. See RFC 7230.
            while (re.search("\r\n0\r\n", json_data) == None):
                json_data += sock.recv(4096)
            sock.close()

            # Retrieve container name and image
            m = re.search("{(.+)}", json_data)
            if m:
                json_data = "{"+m.group(1)+"}"
            try:
                data = json.loads(json_data)
                try:
                    containernames[containerid] = data["Name"].lstrip('/')
                except:
                    print >>sys.stderr, containerid+" has no Name field"
                try:
                    containerimages[containerid] = data["Config"]["Image"].replace(':', '_')
                except:
                    print >>sys.stderr, containerid+" has no Image field"
            except:
                print >>sys.stderr, "Can not load json"

    except socket.timeout, e:
        print >>sys.stderr, "Socket: %s" % (e,)

def senddata(datatosend, containerid):
    if datatosend:
        datatosend += " containerid="+containerid
        if (containerid in containernames):
            datatosend += " containername="+containernames[containerid]
        if (containerid in containerimages):
            datatosend += " containerimage="+containerimages[containerid]
        print datatosend
    sys.stdout.flush()

def readdockerstats(path, containerid):

    # update containername and containerimage if needed
    if ((containerid not in containernames) or (containerid not in containerimages)):
        getnameandimage(containerid)

    # Retrieve and push stats
    for file_stat in os.listdir(path):
        if (os.path.isfile(path+"/"+file_stat)\
        and ((file_stat in proc_names.keys()) or (file_stat in proc_names_to_agg.keys()))):
            try:
                f_stat = open(path+"/"+file_stat)
            except IOError, e:
                print >>sys.stderr, "Failed to open input file: %s" % (e,)
                return 1
            ts = int(time.time())

            # proc_names
            if (file_stat in proc_names.keys()):
                datatosend = None
                f_stat.seek(0)
                for line in f_stat:
                    fields = line.split()
                    if fields[0] in proc_names[file_stat]:
                        datatosend = file_stat.split('.')[0]+"."+fields[0]+" %d %s" % (ts, fields[1])
                        senddata(datatosend, containerid)

            # proc_names_to_agg
            else:
                if (file_stat in proc_names_to_agg.keys()):
                    for field_to_match in proc_names_to_agg[file_stat]:
                        datatosend = None
                        f_stat.seek(0)
                        count = 0
                        for line in f_stat:
                            fields = line.split()
                            if fields[1] == field_to_match:
                                datatosend = file_stat+"."+fields[1].lower()
                                try:
                                    count += int(fields[2])
                                except:
                                    pass
                        if datatosend:
                            senddata(datatosend+" %d %s" % (ts, count), containerid)

            f_stat.close()

def main():
    """docker_cpu main loop"""
    global containernames
    global containerimages
    utils.drop_privileges()
    cache=0
    while True:

        # Connect to Docker socket to get informations about containers every 4 times
        if (cache == 0):
            containernames={}
            containerimages={}
        cache += 1
        if (cache == 4):
            cache = 0

        if os.path.isdir("/sys/fs/cgroup"):
            for level1 in os.listdir("/sys/fs/cgroup"):
                if (os.path.isdir("/sys/fs/cgroup/"+level1+"/docker")\
                # /sys/fs/cgroup/cpu and /sys/fs/cgroup/cpuacct are often links to /sys/fs/cgroup/cpu,cpuacct
                and not (((level1 == "cpu,cpuacct") or (level1 == "cpuacct")) and (os.path.isdir("/sys/fs/cgroup/cpu/docker")))):
                    for level2 in os.listdir("/sys/fs/cgroup/"+level1+"/docker"):
                        if os.path.isdir("/sys/fs/cgroup/"+level1+"/docker/"+level2):
                            readdockerstats("/sys/fs/cgroup/"+level1+"/docker/"+level2, level2)
                else:
                    # If Docker cgroup is handled by slice
                    # http://www.freedesktop.org/software/systemd/man/systemd.slice.html
                    for slicename in ("system.slice", "machine.slice", "user.slice"):
                        if (os.path.isdir("/sys/fs/cgroup/"+level1+"/"+slicename)\
                        # /sys/fs/cgroup/cpu and /sys/fs/cgroup/cpuacct are often links to /sys/fs/cgroup/cpu,cpuacct
                        and not (((level1 == "cpu,cpuacct") or (level1 == "cpuacct")) and (os.path.isdir("/sys/fs/cgroup/cpu/"+slicename)))):
                            for level2 in os.listdir("/sys/fs/cgroup/"+level1+"/"+slicename):
                                if os.path.isdir("/sys/fs/cgroup/"+level1+"/"+slicename+"/"+level2):
                                    m = re.search("^docker-(\w+)\.scope$", level2)
                                    if m:
                                        readdockerstats("/sys/fs/cgroup/"+level1+"/"+slicename+"/"+level2, m.group(1))
                                        break
        if os.path.isdir("/cgroup/lxc"):
            for level1 in os.listdir("/cgroup/lxc"):
                if os.path.isdir("/cgroup/lxc/"+level1):
                    readdockerstats("/cgroup/lxc/"+level1, level1)
        time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    sys.exit(main())
