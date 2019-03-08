#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2012  The tcollector Authors.
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

'''
CPU detailed statistics for TSDB

This plugin tracks, for all CPUs:

- user %
- nice %
- system %
- interrupt %
- idle %

Requirements :
- FreeBSD : top
- Linux : mpstat

In addition, for FreeBSD, it reports :
- load average (1m, 5m, 15m)
- number of processes (total, starting, running, sleeping, stopped, zombie, waiting, lock)
- memory statistics (bytes) (active, inact, wired, cache, buf, free)
- arc statistics (bytes) (total, mru, mfu, anon, header, other)
- swap statistics (bytes) (total, free, inuse, in/s, out/s)
'''

import errno
import sys
import time
import subprocess
import re
import signal
import os
import platform

from collectors.lib import utils

PY3 = sys.version_info[0] > 2
if PY3:
    long = int

try:
    from collectors.etc import sysload_conf
except ImportError:
    sysload_conf = None

DEFAULT_COLLECTION_INTERVAL=15

def convert_to_bytes(string):
    """Take a string in the form 1234K, and convert to bytes"""
    factors = {
       "K": 1024,
       "M": 1024 * 1024,
       "G": 1024 * 1024 * 1024,
       "T": 1024 * 1024 * 1024 * 1024,
       "P": 1024 * 1024 * 1024 * 1024 * 1024,
       "E": 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
    }
    for f, fm in factors.items():
        if string.endswith(f):
            number = float(string[:-1])
            number = number * fm
            return long(number)
    return long(string)

signal_received = None
def handlesignal(signum, stack):
    global signal_received
    signal_received = signum

def main():
    """top main loop"""

    collection_interval=DEFAULT_COLLECTION_INTERVAL
    collect_every_cpu=True
    if(sysload_conf):
        config = sysload_conf.get_config()
        collection_interval=config['collection_interval']
        collect_every_cpu=config['collect_every_cpu']

    global signal_received

    signal.signal(signal.SIGTERM, handlesignal)
    signal.signal(signal.SIGINT, handlesignal)

    try:
        if platform.system() == "FreeBSD":
            if(collect_every_cpu):
                p_top = subprocess.Popen(
                    ["top", "-S", "-P", "-n", "-s"+str(collection_interval), "-dinfinity", "0"],
                    stdout=subprocess.PIPE,
                )
            else:
                p_top = subprocess.Popen(
                    ["top", "-S", "-n", "-s"+str(collection_interval), "-dinfinity", "0"],
                    stdout=subprocess.PIPE,
                )            
        else:
            if(collect_every_cpu):
                p_top = subprocess.Popen(
                    ["mpstat", "-P", "ALL", str(collection_interval)],
                    stdout=subprocess.PIPE,
                )
            else:
                p_top = subprocess.Popen(
                    ["mpstat", str(collection_interval)],
                    stdout=subprocess.PIPE,
                )
    except OSError as e:
        if e.errno == errno.ENOENT:
            # it makes no sense to run this collector here
            sys.exit(13) # we signal tcollector to not run us
        raise

    timestamp = 0

    while signal_received is None:
        try:
            line = p_top.stdout.readline()
        except (IOError, OSError) as e:
            if e.errno in (errno.EINTR, errno.EAGAIN):
                break
            raise

        if not line:
            # end of the program, die
            break

        # CPU: --> CPU all:  : FreeBSD, to match the all CPU
        # %( [uni][a-z]+,?)? : FreeBSD, so that top output matches mpstat output
        # AM                 : Linux, mpstat output depending on locale
        # PM                 : Linux, mpstat output depending on locale
        # .* load            : FreeBSD, to correctly match load averages
        # ,                  : FreeBSD, to correctly match processes: Mem: ARC: and Swap:
        fields = re.sub("CPU:", "CPU all:", re.sub(r"%( [uni][a-z]+,?)?| AM | PM |.* load |,", "", line)).split()
        if len(fields) <= 0:
            continue

        if (((fields[0] == "CPU") or (re.match("[0-9][0-9]:[0-9][0-9]:[0-9][0-9]",fields[0]))) and ((collect_every_cpu and re.match("[0-9]+:?",fields[1])) or ((not collect_every_cpu) and re.match("all:?",fields[1])))):
            if((fields[1] == "all") or (fields[1] == "0")):
                timestamp = int(time.time())
            cpuid=fields[1].replace(":","")
            cpuuser=fields[2]
            cpunice=fields[3]
            cpusystem=fields[4]
            cpuinterrupt=fields[6]
            cpuidle=fields[-1]
            print("cpu.usr %s %s cpu=%s" % (timestamp, float(cpuuser), cpuid))
            print("cpu.nice %s %s cpu=%s" % (timestamp, float(cpunice), cpuid))
            print("cpu.sys %s %s cpu=%s" % (timestamp, float(cpusystem), cpuid))
            print("cpu.irq %s %s cpu=%s" % (timestamp, float(cpuinterrupt), cpuid))
            print("cpu.idle %s %s cpu=%s" % (timestamp, float(cpuidle), cpuid))
        
        elif(fields[0] == "averages:"):
            timestamp = int(time.time())
            print("load.1m %s %s" % (timestamp, fields[1]))
            print("load.5m %s %s" % (timestamp, fields[2]))
            print("load.15m %s %s" % (timestamp, fields[3]))

        elif (re.match("[0-9]+ processes:",line)):
            starting=0
            running=0
            sleeping=0
            stopped=0
            zombie=0
            waiting=0
            lock=0
            for i in range(len(fields)):
                if(fields[i] == "starting"):
                    starting=fields[i-1]
                if(fields[i] == "running"):
                    running=fields[i-1]
                if(fields[i] == "sleeping"):
                    sleeping=fields[i-1]
                if(fields[i] == "stopped"):
                    stopped=fields[i-1]
                if(fields[i] == "zombie"):
                    zombie=fields[i-1]
                if(fields[i] == "waiting"):
                    waiting=fields[i-1]
                if(fields[i] == "lock"):
                    lock=fields[i-1]
            print("ps.all %s %s" % (timestamp, fields[0]))
            print("ps.start %s %s" % (timestamp, starting))
            print("ps.run %s %s" % (timestamp, running))
            print("ps.sleep %s %s" % (timestamp, sleeping))
            print("ps.stop %s %s" % (timestamp, stopped))
            print("ps.zomb %s %s" % (timestamp, zombie))
            print("ps.wait %s %s" % (timestamp, waiting))
            print("ps.lock %s %s" % (timestamp, lock))

        elif(fields[0] == "Mem:"):
            active=0
            inact=0
            wired=0
            cache=0
            buf=0
            free=0
            for i in range(len(fields)):
                if(fields[i] == "Active"):
                    active=convert_to_bytes(fields[i-1])
                if(fields[i] == "Inact"):
                    inact=convert_to_bytes(fields[i-1])
                if(fields[i] == "Wired"):
                    wired=convert_to_bytes(fields[i-1])
                if(fields[i] == "Cache"):
                    cache=convert_to_bytes(fields[i-1])
                if(fields[i] == "Buf"):
                    buf=convert_to_bytes(fields[i-1])
                if(fields[i] == "Free"):
                    free=convert_to_bytes(fields[i-1])
            print("mem.active %s %s" % (timestamp, active))
            print("mem.inact %s %s" % (timestamp, inact))
            print("mem.wired %s %s" % (timestamp, wired))
            print("mem.cache %s %s" % (timestamp, cache))
            print("mem.buf %s %s" % (timestamp, buf))
            print("mem.free %s %s" % (timestamp, free))

        elif(fields[0] == "ARC:"):
            total=0
            mru=0
            mfu=0
            anon=0
            header=0
            other=0
            for i in range(len(fields)):
                if(fields[i] == "Total"):
                    total=convert_to_bytes(fields[i-1])
                if(fields[i] == "MRU"):
                    mru=convert_to_bytes(fields[i-1])
                if(fields[i] == "MFU"):
                    mfu=convert_to_bytes(fields[i-1])
                if(fields[i] == "Anon"):
                    anon=convert_to_bytes(fields[i-1])
                if(fields[i] == "Header"):
                    header=convert_to_bytes(fields[i-1])
                if(fields[i] == "Other"):
                    other=convert_to_bytes(fields[i-1])
            print("arc.total %s %s" % (timestamp, total))
            print("arc.mru %s %s" % (timestamp, mru))
            print("arc.mfu %s %s" % (timestamp, mfu))
            print("arc.anon %s %s" % (timestamp, anon))
            print("arc.header %s %s" % (timestamp, header))
            print("arc.other %s %s" % (timestamp, other))

        elif(fields[0] == "Swap:"):
            total=0
            used=0
            free=0
            inuse=0
            inps=0
            outps=0
            for i in range(len(fields)):
                if(fields[i] == "Total"):
                    total=convert_to_bytes(fields[i-1])
                if(fields[i] == "Used"):
                    used=convert_to_bytes(fields[i-1])
                if(fields[i] == "Free"):
                    free=convert_to_bytes(fields[i-1])
                if(fields[i] == "Inuse"):
                    inuse=convert_to_bytes(fields[i-1])
                if(fields[i] == "In"):
                    inps=convert_to_bytes(fields[i-1])/collection_interval
                if(fields[i] == "Out"):
                    outps=convert_to_bytes(fields[i-1])/collection_interval
            print("swap.total %s %s" % (timestamp, total))
            print("swap.used %s %s" % (timestamp, used))
            print("swap.free %s %s" % (timestamp, free))
            print("swap.inuse %s %s" % (timestamp, inuse))
            print("swap.inps %s %s" % (timestamp, inps))
            print("swap.outps %s %s" % (timestamp, outps))

        sys.stdout.flush()

    if signal_received is None:
        signal_received = signal.SIGTERM
    try:
        os.kill(p_top.pid, signal_received)
    except Exception:
        pass
    p_top.wait()

if __name__ == "__main__":
    main()
