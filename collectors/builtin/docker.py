#!/usr/bin/python
# More informations on https://docs.docker.com/articles/runmetrics/
"""Imports Docker stats from /sys/fs/cgroup."""

import os
import re
import socket
import sys
import time
import json
import platform
from Queue import Queue

from collectors.lib import utils
from collectors.lib.collectorbase import CollectorBase

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


class Docker(CollectorBase):
    def __init__(self, config, logger, readq):
        super(Docker, self).__init__(config, logger, readq)
        self.containernames = {}
        self.containerimages = {}
        with utils.lower_privileges(self._logger):
            self.cache = 0
            if platform.dist()[0] in ['centos', 'redhat'] and not platform.dist()[1].startswith("7."):
                self.cgroup_path = '/cgroup'
            else:
                self.cgroup_path = '/sys/fs/cgroup'
            self.socket_path = '/var/run/docker.sock'

    def __call__(self):
        with utils.lower_privileges(self._logger):
            # Connect to Docker socket to get informations about containers every 4 times
            if self.cache == 0:
                self.containernames = {}
                self.containerimages = {}
            self.cache += 1
            if self.cache == 4:
                self.cache = 0

            if os.path.isdir(self.cgroup_path):
                for level1 in os.listdir(self.cgroup_path):
                    if (os.path.isdir(self.cgroup_path + "/" + level1 + "/docker") and
                            # /cgroup/cpu and /cgroup/cpuacct are often links to /cgroup/cpu,cpuacct
                            not (((level1 == "cpu,cpuacct") or (level1 == "cpuacct")) and (
                                    os.path.isdir(self.cgroup_path + "/cpu/docker")))):
                        for level2 in os.listdir(self.cgroup_path + "/" + level1 + "/docker"):
                            if os.path.isdir(self.cgroup_path + "/" + level1 + "/docker/" + level2):
                                self.readdockerstats(self.cgroup_path + "/" + level1 + "/docker/" + level2, level2)
                    else:
                        # If Docker cgroup is handled by slice
                        # http://www.freedesktop.org/software/systemd/man/systemd.slice.html
                        for slicename in ("system.slice", "machine.slice", "user.slice"):
                            if (os.path.isdir(self.cgroup_path + "/" + level1 + "/" + slicename) and
                                    # /cgroup/cpu and /cgroup/cpuacct are often links to /cgroup/cpu,cpuacct
                                    not (((level1 == "cpu,cpuacct") or (level1 == "cpuacct")) and (
                                            os.path.isdir(self.cgroup_path + "/cpu/" + slicename)))):
                                for level2 in os.listdir(self.cgroup_path + "/" + level1 + "/" + slicename):
                                    if os.path.isdir(self.cgroup_path + "/" + level1 + "/" + slicename + "/" + level2):
                                        m = re.search("^docker-(\w+)\.scope$", level2)
                                        if m:
                                            self.readdockerstats(
                                                    self.cgroup_path + "/" + level1 + "/" + slicename + "/" + level2,
                                                    m.group(1))
                                            break
            if os.path.isdir(self.cgroup_path + "/lxc"):
                for level1 in os.listdir(self.cgroup_path + "/lxc"):
                    if os.path.isdir(self.cgroup_path + "/lxc/" + level1):
                        self.readdockerstats(self.cgroup_path + "/lxc/" + level1, level1)

    def readdockerstats(self, path, containerid):
        # update containername and containerimage if needed
        if (containerid not in self.containernames) or (containerid not in self.containerimages):
            self.getnameandimage(containerid)

        # Retrieve and push stats
        for file_stat in os.listdir(path):
            if os.path.isfile(path + "/" + file_stat) and (file_stat in proc_names.keys()) or (
                        file_stat in proc_names_to_agg.keys()):
                try:
                    f_stat = open(path + "/" + file_stat)
                except IOError:
                    self.log_exception("failed to open input file.")
                    return 1
                ts = int(time.time())

                with f_stat:
                    # proc_name
                    if file_stat in proc_names.keys():
                        f_stat.seek(0)
                        for line in f_stat:
                            subcattype = None
                            fields = line.split()
                            category = file_stat.split('.')[0]
                            subcategory = fields[0]
                            value = fields[1]
                            if subcategory in proc_names[file_stat]:
                                if category == 'memory':
                                    if subcategory in ['active_anon', 'inactive_anon']:
                                        subcattype = subcategory.split('_')[0]
                                        subcategory = 'anon'
                                    if subcategory in ['active_file', 'inactive_file']:
                                        subcattype = subcategory.split('_')[0]
                                        subcategory = 'file'
                                    tags = "type=%s" % subcategory
                                    if subcattype is not None:
                                        tags += " subtype=%s" % subcattype
                                    datatosend = "%s %d %s %s" % (category, ts, value, tags)
                                else:
                                    datatosend = "%s.%s %d %s" % (category, subcategory, ts, value)
                                self.senddata(datatosend, containerid)
                    # proc_names_to_agg
                    else:
                        if file_stat in proc_names_to_agg.keys():
                            for field_to_match in proc_names_to_agg[file_stat]:
                                datatosend = None
                                f_stat.seek(0)
                                count = 0
                                for line in f_stat:
                                    fields = line.split()
                                    if fields[1] == field_to_match:
                                        datatosend = "%s.%s" % (file_stat, fields[1].lower())
                                        try:
                                            count += int(fields[2])
                                        except:
                                            pass
                                if datatosend:
                                    self.senddata("%s %d %s" % (datatosend, ts, count), containerid)

    def getnameandimage(self, containerid):
        # Retrieve container json configuration file
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            r = sock.connect_ex(self.socket_path)
            if r != 0:
                self.log_error("Can not connect to %s", self.socket_path)
            else:
                message = 'GET /containers/' + containerid + '/json HTTP/1.1\n\n'
                sock.sendall(message)
                json_data = ""
                # "\r\n0\r\n" is raised on last chunk. See RFC 7230.
                while re.search("\r\n0\r\n", json_data) is None:
                    json_data += sock.recv(4096)
                sock.close()

                # Retrieve container name and image
                m = re.search("{(.+)}", json_data)
                if m:
                    json_data = "{" + m.group(1) + "}"
                try:
                    data = json.loads(json_data)
                    try:
                        self.containernames[containerid] = data["Name"].lstrip('/')
                    except:
                        self.log_exception(containerid + " has no Name field")
                    try:
                        self.containerimages[containerid] = data["Config"]["Image"].replace(':', '_')
                    except:
                        self.log_exception(containerid + " has no Image field")
                except:
                    self.log_exception("Can not load json")

        except socket.timeout, e:
            print >> sys.stderr, "Socket: %s" % (e,)

    def senddata(self, datatosend, containerid):
        if datatosend:
            datatosend += " containerid=" + containerid
            if containerid in self.containernames:
                datatosend += " containername=" + self.containernames[containerid]
            if containerid in self.containerimages:
                datatosend += " containerimage=" + self.containerimages[containerid]
            self._readq.nput("docker.%s" % datatosend)


if __name__ == "__main__":
    docker_inst = Docker(None, None, Queue())
    docker_inst()
