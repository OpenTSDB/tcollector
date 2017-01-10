#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2010  The tcollector Authors.
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
"""import various /proc stats from /proc into TSDB"""

import os
import re
import sys
import time
import glob
from Queue import Queue

from collectors.lib import utils
from collectors.lib.collectorbase import CollectorBase

NUMADIR = "/sys/devices/system/node"


def find_sysfs_numa_stats():
    """Returns a possibly empty list of NUMA stat file names."""
    try:
        nodes = os.listdir(NUMADIR)
    except OSError, (errno, msg):
        if errno == 2:  # No such file or directory
            return []   # We don't have NUMA stats.
        raise

    nodes = [node for node in nodes if node.startswith("node")]
    numastats = []
    for node in nodes:
        try:
            numastats.append(os.path.join(NUMADIR, node, "numastat"))
        except OSError, (errno, msg):
            if errno == 2:  # No such file or directory
                continue
            raise
    return numastats


class Procstats(CollectorBase):
    def __init__(self, config, logger, readq):
        super(Procstats, self).__init__(config, logger, readq)
        try:
            self.f_uptime = open("/proc/uptime", "r")
            self.f_meminfo = open("/proc/meminfo", "r")
            self.f_vmstat = open("/proc/vmstat", "r")
            self.f_stat = open("/proc/stat", "r")
            self.f_loadavg = open("/proc/loadavg", "r")
            self.f_entropy_avail = open("/proc/sys/kernel/random/entropy_avail", "r")
            self.f_interrupts = open("/proc/interrupts", "r")

            self.f_scaling = "/sys/devices/system/cpu/cpu%s/cpufreq/%s_freq"
            self.f_scaling_min = dict([])
            self.f_scaling_max = dict([])
            self.f_scaling_cur = dict([])
            self.f_softirqs = open("/proc/softirqs", "r")
            for cpu in glob.glob("/sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_cur_freq"):
                m = re.match("/sys/devices/system/cpu/cpu([0-9]*)/cpufreq/scaling_cur_freq", cpu)
                if not m:
                    continue
                cpu_no = m.group(1)
                sys.stderr.write(self.f_scaling % (cpu_no, "min"))
                self.f_scaling_min[cpu_no] = open(self.f_scaling % (cpu_no, "cpuinfo_min"), "r")
                self.f_scaling_max[cpu_no] = open(self.f_scaling % (cpu_no, "cpuinfo_max"), "r")
                self.f_scaling_cur[cpu_no] = open(self.f_scaling % (cpu_no, "scaling_cur"), "r")

            self.numastats = find_sysfs_numa_stats()
        except:
            self._readq.nput("procstats.state %s %s" % (int(time.time()), '1'))
            self.cleanup()
            raise

    def cleanup(self):
        self.safe_close(self.f_uptime)
        self.safe_close(self.f_meminfo)
        self.safe_close(self.f_vmstat)
        self.safe_close(self.f_stat)
        self.safe_close(self.f_loadavg)
        self.safe_close(self.f_entropy_avail)
        self.safe_close(self.f_interrupts)
        self.safe_close(self.f_softirqs)
        self._cleanup_dict(self.f_scaling_min)
        self._cleanup_dict(self.f_scaling_max)
        self._cleanup_dict(self.f_scaling_cur)

    def _cleanup_dict(self, d):
        for key, val in d.iteritems():
            self.safe_close(val)

    def __call__(self):
        with utils.lower_privileges(self._logger):
            # proc.uptime
            self.f_uptime.seek(0)
            ts = int(time.time())
            for line in self.f_uptime:
                m = re.match("(\S+)\s+(\S+)", line)
                if m:
                    self._readq.nput("proc.uptime.total %d %s" % (ts, m.group(1)))
                    self._readq.nput("proc.uptime.now %d %s" % (ts, m.group(2)))

            # proc.meminfo
            self.f_meminfo.seek(0)
            ts = int(time.time())
            for line in self.f_meminfo:
                m = re.match("(\w+):\s+(\d+)\s+(\w+)", line)
                if m:
                    if m.group(3).lower() == 'kb':
                        # convert from kB to B for easier graphing
                        value = str(int(m.group(2)) * 1024)
                    else:
                        value = m.group(2)
                    self._readq.nput("proc.meminfo.%s %d %s" % (m.group(1).lower(), ts, value))

            # proc.vmstat
            self.f_vmstat.seek(0)
            ts = int(time.time())
            for line in self.f_vmstat:
                m = re.match("(\w+)\s+(\d+)", line)
                if not m:
                    continue
                if m.group(1) in ("pgpgin", "pgpgout", "pswpin",
                                  "pswpout", "pgfault", "pgmajfault"):
                    self._readq.nput("proc.vmstat.%s %d %s" % (m.group(1), ts, m.group(2)))

            # proc.stat
            self.f_stat.seek(0)
            ts = int(time.time())
            for line in self.f_stat:
                m = re.match("(\w+)\s+(.*)", line)
                if not m:
                    continue
                if m.group(1).startswith("cpu"):
                    cpu_m = re.match("cpu(\d+)", m.group(1))
                    if cpu_m:
                        metric_percpu = '.percpu'
                        tags = ' cpu=%s' % cpu_m.group(1)
                    else:
                        metric_percpu = ''
                        tags = ''
                    fields = m.group(2).split()
                    cpu_types = ['user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq', 'guest', 'guest_nice']

                    # We use zip to ignore fields that don't exist.
                    for value, field_name in zip(fields, cpu_types):
                        self._readq.nput("proc.stat.cpu%s %d %s type=%s%s" % (metric_percpu, ts, value, field_name, tags))
                elif m.group(1) == "intr":
                    self._readq.nput("proc.stat.intr %d %s" % (ts, m.group(2).split()[0]))
                elif m.group(1) == "ctxt":
                    self._readq.nput("proc.stat.ctxt %d %s" % (ts, m.group(2)))
                elif m.group(1) == "processes":
                    self._readq.nput("proc.stat.processes %d %s" % (ts, m.group(2)))
                elif m.group(1) == "procs_blocked":
                    self._readq.nput("proc.stat.procs_blocked %d %s" % (ts, m.group(2)))

            self.f_loadavg.seek(0)
            ts = int(time.time())
            for line in self.f_loadavg:
                m = re.match("(\S+)\s+(\S+)\s+(\S+)\s+(\d+)/(\d+)\s+", line)
                if not m:
                    continue
                self._readq.nput("proc.loadavg.1min %d %s" % (ts, m.group(1)))
                self._readq.nput("proc.loadavg.5min %d %s" % (ts, m.group(2)))
                self._readq.nput("proc.loadavg.15min %d %s" % (ts, m.group(3)))
                self._readq.nput("proc.loadavg.runnable %d %s" % (ts, m.group(4)))
                self._readq.nput("proc.loadavg.total_threads %d %s" % (ts, m.group(5)))

            self.f_entropy_avail.seek(0)
            ts = int(time.time())
            for line in self.f_entropy_avail:
                self._readq.nput("proc.kernel.entropy_avail %d %s" % (ts, line.strip()))

            self.f_interrupts.seek(0)
            ts = int(time.time())
            # Get number of CPUs from description line.
            num_cpus = len(self.f_interrupts.readline().split())
            for line in self.f_interrupts:
                cols = line.split()

                irq_type = cols[0].rstrip(":")
                if irq_type.isalnum():
                    if irq_type.isdigit():
                        if cols[-2] == "PCI-MSI-edge" and "eth" in cols[-1]:
                            irq_type = cols[-1]
                        else:
                            continue  # Interrupt type is just a number, ignore.
                    for i, val in enumerate(cols[1:]):
                        if i >= num_cpus:
                            # All values read, remaining cols contain textual
                            # description
                            break
                        if not val.isdigit():
                            # something is weird, there should only be digit values
                            self.log_error("Unexpected interrupts value %r in %r: ", val, cols)
                            break
                        self._readq.nput("proc.interrupts %s %s type=%s cpu=%s" % (ts, val, irq_type, i))

            self.f_softirqs.seek(0)
            ts = int(time.time())
            # Get number of CPUs from description line.
            num_cpus = len(self.f_softirqs.readline().split())
            for line in self.f_softirqs:
                cols = line.split()

                irq_type = cols[0].rstrip(":")
                for i, val in enumerate(cols[1:]):
                    if i >= num_cpus:
                        # All values read, remaining cols contain textual
                        # description
                        break
                    if not val.isdigit():
                        # something is weird, there should only be digit values
                        self.log_error("Unexpected softirq value %r in %r: ", val, cols)
                        break
                    self._readq.nput("proc.softirqs %s %s type=%s cpu=%s" % (ts, val, irq_type, i))

            self._print_numa_stats(self.numastats)

            # Print scaling stats
            ts = int(time.time())
            for cpu_no in self.f_scaling_min.keys():
                f = self.f_scaling_min[cpu_no]
                f.seek(0)
                for line in f:
                    self._readq.nput("proc.scaling.min %d %s cpu=%s" % (ts, line.rstrip('\n'), cpu_no))
            ts = int(time.time())
            for cpu_no in self.f_scaling_max.keys():
                f = self.f_scaling_max[cpu_no]
                f.seek(0)
                for line in f:
                    self._readq.nput("proc.scaling.max %d %s cpu=%s" % (ts, line.rstrip('\n'), cpu_no))
            ts = int(time.time())
            for cpu_no in self.f_scaling_cur.keys():
                f = self.f_scaling_cur[cpu_no]
                f.seek(0)
                for line in f:
                    self._readq.nput("proc.scaling.cur %d %s cpu=%s" % (ts, line.rstrip('\n'), cpu_no))

            self._readq.nput("procstats.state %s %s" % (int(time.time()), '0'))

    def _print_numa_stats(self, numafiles):
        """From a list of files names, opens file, extracts and prints NUMA stats."""
        for numafilename in numafiles:
            with open(numafilename) as numafile:
                node_id = int(numafile.name[numafile.name.find("/node/node") + 10:-9])
                ts = int(time.time())
                stats = dict(line.split() for line in numafile.read().splitlines())
                for stat, tag in (  # hit: process wanted memory from this node and got it
                                  ("numa_hit", "hit"),
                                  # miss: process wanted another node and got it from
                                  # this one instead.
                                  ("numa_miss", "miss")):
                    self._readq.nput("sys.numa.zoneallocs %d %s node=%d type=%s" % (ts, stats[stat], node_id, tag))
                # Count this one as a separate metric because we can't sum up hit +
                # miss + foreign, this would result in double-counting of all misses.
                # See `zone_statistics' in the code of the kernel.
                # foreign: process wanted memory from this node but got it from
                # another node.  So maybe this node is out of free pages.
                self._readq.nput("sys.numa.foreign_allocs %d %s node=%d" % (ts, stats["numa_foreign"], node_id))
                # When is memory allocated to a node that's local or remote to where
                # the process is running.
                for stat, tag in (("local_node", "local"),
                                  ("other_node", "remote")):
                    self._readq.nput("sys.numa.allocation %d %s node=%d type=%s" % (ts, stats[stat], node_id, tag))
                # Pages successfully allocated with the interleave policy.
                self._readq.nput("sys.numa.interleave %d %s node=%d type=hit" % (ts, stats["interleave_hit"], node_id))


if __name__ == "__main__":
    procstats_inst = Procstats(None, None, Queue())
    procstats_inst()
