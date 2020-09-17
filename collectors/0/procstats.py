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

import errno
import os
import re
import sys
import time
import glob

from collectors.lib import utils

INTERRUPTS_INTVL_MULT = 4 # query softirqs every SOFTIRQS_INT_MULT * COLLECTION_INTERVAL seconds
SOFTIRQS_INTVL_MULT = 4 # query softirqs every SOFTIRQS_INT_MULT * COLLECTION_INTERVAL seconds
# Modern Linux:
CPUSET_PATH = "/sys/fs/cgroup/cpuset"
if os.path.isdir("/dev/cpuset"):
    # Older Linux:
    CPUSET_PATH = "/dev/cpuset"
COLLECTION_INTERVAL = 15  # seconds
NUMADIR = "/sys/devices/system/node"


def find_sysfs_numa_stats():
    """Returns a possibly empty list of NUMA stat file names."""
    try:
        nodes = os.listdir(NUMADIR)
    except OSError as exc:
        if exc.errno == 2:  # No such file or directory
            return []   # We don't have NUMA stats.
        raise

    nodes = [node for node in nodes if node.startswith("node")]
    numastats = []
    for node in nodes:
        try:
            numastats.append(os.path.join(NUMADIR, node, "numastat"))
        except OSError as exc:
            if exc.errno == 2:  # No such file or directory
                continue
            raise
    return numastats


def print_numa_stats(numafiles):
    """From a list of files names, opens file, extracts and prints NUMA stats."""
    for numafilename in numafiles:
        numafile = open(numafilename)
        node_id = int(numafile.name[numafile.name.find("/node/node")+10:-9])
        ts = int(time.time())
        stats = dict(line.split() for line in numafile.read().splitlines())
        for stat, tag in (# hit: process wanted memory from this node and got it
                          ("numa_hit", "hit"),
                          # miss: process wanted another node and got it from
                          # this one instead.
                          ("numa_miss", "miss")):
            print("sys.numa.zoneallocs %d %s node=%d type=%s"
                   % (ts, stats[stat], node_id, tag))
        # Count this one as a separate metric because we can't sum up hit +
        # miss + foreign, this would result in double-counting of all misses.
        # See `zone_statistics' in the code of the kernel.
        # foreign: process wanted memory from this node but got it from
        # another node.  So maybe this node is out of free pages.
        print("sys.numa.foreign_allocs %d %s node=%d"
               % (ts, stats["numa_foreign"], node_id))
        # When is memory allocated to a node that's local or remote to where
        # the process is running.
        for stat, tag in (("local_node", "local"),
                          ("other_node", "remote")):
            print("sys.numa.allocation %d %s node=%d type=%s"
                   % (ts, stats[stat], node_id, tag))
        # Pages successfully allocated with the interleave policy.
        print("sys.numa.interleave %d %s node=%d type=hit"
               % (ts, stats["interleave_hit"], node_id))
        numafile.close()

def expand_numlist(s):
    """return a list of numbers from a list with ranges,
       e.g. 4,5-10,14-16"""
    r = []
    for i in s.split(','):
        if '-' not in i:
            r.append(int(i))
        else:
            l,h = map(int, i.split('-'))
            r+= range(l,h+1)
    return r

def cpus_csets(cpuset_path):
    """Return a hash of cpu_id_as_string->cset_name"""
    try:
        csets = os.listdir(cpuset_path)
    except OSError as e:
        if e.errno == errno.ENOENT: # No such file or directory
           return {}   # We don't have csets
        raise

    csets = [cset for cset in csets if os.path.isdir(os.path.join(cpuset_path, cset))]

    cpu2cset = {}
    for cset in csets:
       cpuspath = os.path.join(cpuset_path, cset, 'cpuset.cpus')
       if not os.path.isfile(cpuspath):
          cpuspath = os.path.join(cpuset_path, cset, 'cpus')
       if not os.path.isfile(cpuspath):
          # No such file?? Ignore csets
          sys.stderr.write("No 'cpuset.cpus' or 'cpus' file in %s!" % os.path.join(cpuset_path, cset))
          continue

       try:
           f_cpus = open(cpuspath)
       except:
           # Ignore that one and continue
           sys.stderr.write("Could not open %s!" % cpuspath)
           continue

       format_errors = 0
       for line in f_cpus:
           m = re.match('^[-0-9,]+$', line)
           if m:
               for c in expand_numlist(line):
                   cpu2cset[str(c)] = cset
           else:
               format_errors += 1
       if format_errors > 0:
           sys.stderr.write("%d line(s) of %s were not in the expected format" % (format_errors, cpuspath))

    return cpu2cset

def main():
    """procstats main loop"""

    f_uptime = open("/proc/uptime", "r")
    f_meminfo = open("/proc/meminfo", "r")
    f_vmstat = open("/proc/vmstat", "r")
    f_stat = open("/proc/stat", "r")
    f_loadavg = open("/proc/loadavg", "r")
    f_entropy_avail = open("/proc/sys/kernel/random/entropy_avail", "r")
    f_interrupts = open("/proc/interrupts", "r")

    f_scaling = "/sys/devices/system/cpu/cpu%s/cpufreq/%s_freq"
    f_scaling_min  = dict([])
    f_scaling_max  = dict([])
    f_scaling_cur  = dict([])
    f_softirqs = open("/proc/softirqs", "r")
    for cpu in glob.glob("/sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_cur_freq"):
        m = re.match("/sys/devices/system/cpu/cpu([0-9]*)/cpufreq/scaling_cur_freq", cpu)
        if not m:
            continue
        cpu_no = m.group(1)
        sys.stderr.write(f_scaling % (cpu_no,"min"))
        f_scaling_min[cpu_no] = open(f_scaling % (cpu_no,"cpuinfo_min"), "r")
        f_scaling_max[cpu_no] = open(f_scaling % (cpu_no,"cpuinfo_max"), "r")
        f_scaling_cur[cpu_no] = open(f_scaling % (cpu_no,"scaling_cur"), "r")

    numastats = find_sysfs_numa_stats()
    utils.drop_privileges()

    iteration = -1
    while True:
        iteration += 1
        # proc.uptime
        f_uptime.seek(0)
        ts = int(time.time())
        for line in f_uptime:
            m = re.match("(\S+)\s+(\S+)", line)
            if m:
                print("proc.uptime.total %d %s" % (ts, m.group(1)))
                print("proc.uptime.now %d %s" % (ts, m.group(2)))

        # proc.meminfo
        f_meminfo.seek(0)
        ts = int(time.time())
        for line in f_meminfo:
            m = re.match("([^\s:]+):\s+(\d+)(\s+(\w+))?", line)
            if m:
                if m.group(4) is not None and m.group(4).lower() == 'kb':
                    # convert from kB to B for easier graphing
                    value = str(int(m.group(2)) * 1024)
                else:
                    value = m.group(2)
                name = re.sub("\W", "_", m.group(1)).lower().strip("_")
                print("proc.meminfo.%s %d %s"
                        % (name, ts, value))

        # proc.vmstat
        f_vmstat.seek(0)
        ts = int(time.time())
        for line in f_vmstat:
            m = re.match("(\w+)\s+(\d+)", line)
            if not m:
                continue
            if m.group(1) in ("pgpgin", "pgpgout", "pswpin",
                              "pswpout", "pgfault", "pgmajfault"):
                print("proc.vmstat.%s %d %s" % (m.group(1), ts, m.group(2)))

        # proc.stat
        f_stat.seek(0)
        ts = int(time.time())
        cpu2cset = cpus_csets(CPUSET_PATH)
        for line in f_stat:
            m = re.match("(\w+)\s+(.*)", line)
            if not m:
                continue
            if m.group(1).startswith("cpu"):
                cpu_m = re.match("cpu(\d+)", m.group(1))
                if cpu_m:
                    metric_percpu = '.percpu'
                    cpu_i = cpu_m.group(1)
                    if cpu_i in cpu2cset:
                        tags = ' cpu=%s cpuset=%s' % (cpu_i, cpu2cset[cpu_i])
                    else:
                        tags = ' cpu=%s cpuset=none' % cpu_m.group(1)
                else:
                    metric_percpu = ''
                    tags = ''
                fields = m.group(2).split()
                cpu_types = ['user', 'nice', 'system', 'idle', 'iowait',
                    'irq', 'softirq', 'guest', 'guest_nice']

                # We use zip to ignore fields that don't exist.
                for value, field_name in zip(fields, cpu_types):
                    print("proc.stat.cpu%s %d %s type=%s%s" % (metric_percpu,
                        ts, value, field_name, tags))
            elif m.group(1) == "intr":
                print(("proc.stat.intr %d %s"
                        % (ts, m.group(2).split()[0])))
            elif m.group(1) == "ctxt":
                print("proc.stat.ctxt %d %s" % (ts, m.group(2)))
            elif m.group(1) == "processes":
                print("proc.stat.processes %d %s" % (ts, m.group(2)))
            elif m.group(1) == "procs_blocked":
                print("proc.stat.procs_blocked %d %s" % (ts, m.group(2)))

        f_loadavg.seek(0)
        ts = int(time.time())
        for line in f_loadavg:
            m = re.match("(\S+)\s+(\S+)\s+(\S+)\s+(\d+)/(\d+)\s+", line)
            if not m:
                continue
            print("proc.loadavg.1min %d %s" % (ts, m.group(1)))
            print("proc.loadavg.5min %d %s" % (ts, m.group(2)))
            print("proc.loadavg.15min %d %s" % (ts, m.group(3)))
            print("proc.loadavg.runnable %d %s" % (ts, m.group(4)))
            print("proc.loadavg.total_threads %d %s" % (ts, m.group(5)))

        f_entropy_avail.seek(0)
        ts = int(time.time())
        for line in f_entropy_avail:
            print("proc.kernel.entropy_avail %d %s" % (ts, line.strip()))

        # Only get softirqs stats every INTERRUPTS_INT_MULT iterations
        if iteration % INTERRUPTS_INTVL_MULT == 0:
            print_interrupts(f_interrupts)

        # Only get softirqs stats every SOFTIRQS_INT_MULT iterations
        if iteration % SOFTIRQS_INTVL_MULT == 0:
            print_irqs(f_softirqs)

        print_numa_stats(numastats)

        # Print scaling stats
        ts = int(time.time())
        for cpu_no in f_scaling_min.keys():
            f = f_scaling_min[cpu_no]
            f.seek(0)
            for line in f:
                print("proc.scaling.min %d %s cpu=%s" % (ts, line.rstrip('\n'), cpu_no))
        ts = int(time.time())
        for cpu_no in f_scaling_max.keys():
            f = f_scaling_max[cpu_no]
            f.seek(0)
            for line in f:
                print("proc.scaling.max %d %s cpu=%s" % (ts, line.rstrip('\n'), cpu_no))
        ts = int(time.time())
        for cpu_no in f_scaling_cur.keys():
            f = f_scaling_cur[cpu_no]
            f.seek(0)
            for line in f:
                print("proc.scaling.cur %d %s cpu=%s" % (ts, line.rstrip('\n'), cpu_no))

        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)


def print_interrupts(f_interrupts):
    f_interrupts.seek(0)
    ts = int(time.time())
    # Get number of CPUs from description line.
    num_cpus = len(f_interrupts.readline().split())

    interrupt_dict = {}
    for line in f_interrupts:
        cols = line.split()

        irq_type = cols[0].rstrip(":")
        if irq_type.isalnum():
            if irq_type.isdigit():
                if cols[-2] == "PCI-MSI-edge" and "eth" in cols[-1]:
                    irq_type = cols[-1]
                else:
                    continue  # Interrupt type is just a number, ignore.
            # Strip the thread number from the irq_type, e.g. eth8-8 -> eth8
            m = re.match('^(.*)-\d+$', irq_type)
            if m:
                irq_type = m.group(1)

            for i, val in enumerate(cols[1:]):
                if i >= num_cpus:
                    # All values read, remaining cols contain textual
                    # description
                    break
                if not val.isdigit():
                    # something is weird, there should only be digit values
                    sys.stderr.write("Unexpected interrupts value %r in"
                                     " %r: " % (val, cols))
                    break
                k = "type=%s cpu=%s" % (irq_type, i)
                if k in interrupt_dict:
                    interrupt_dict[k] += int(val)
                else:
                    interrupt_dict[k] = int(val)

    for k in interrupt_dict:
        print ("proc.interrupts %s %d %s" % (ts, interrupt_dict[k], k))


def print_irqs(f_softirqs):
    f_softirqs.seek(0)
    ts = int(time.time())
    # Get number of CPUs from description line.
    num_cpus = len(f_softirqs.readline().split())
    for line in f_softirqs:
        cols = line.split()

        irq_type = cols[0].rstrip(":")

        for i, val in enumerate(cols[1:]):
            if i >= num_cpus:
                # All values read, remaining cols contain textual
                # description
                break
            if not val.isdigit():
                # something is weird, there should only be digit values
                sys.stderr.write("Unexpected softirq value %r in"
                                    " %r: " % (val, cols))
                break
            print ("proc.softirqs %s %s type=%s cpu=%s"
                    % (ts, val, irq_type, i))


if __name__ == "__main__":
    main()

