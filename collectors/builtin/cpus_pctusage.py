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
'''

import time
import subprocess
import re
import platform

from collectors.lib.collectorbase import CollectorBase


class CpusPctusage(CollectorBase):
    def __init__(self, config, logger, readq):
        super(CpusPctusage, self).__init__(config, logger, readq)
        collection_interval = self.get_config('interval')
        if platform.system() == "FreeBSD":
            self.p_top = subprocess.Popen(
                ["top", "-t", "-I", "-P", "-n", "-s" + str(collection_interval), "-d" + str((365*24*3600) / collection_interval)],
                stdout=subprocess.PIPE,
            )
        else:
            self.p_top = subprocess.Popen(
                ["mpstat", "-P", "ALL", str(collection_interval)],
                stdout=subprocess.PIPE,
            )

    def __call__(self):
        line = self.p_top.stdout.readline()
        while line:
            fields = re.sub(r"%( [uni][a-z]+,?)? | AM | PM ", "", line).split()
            if len(fields) <= 0:
                continue

            if (((fields[0] == "CPU") or (re.match("[0-9][0-9]:[0-9][0-9]:[0-9][0-9]",fields[0]))) and (re.match("[0-9]+:?",fields[1]))):
                timestamp = int(time.time())
                cpuid=fields[1].replace(":","")
                cpuuser=fields[2]
                cpunice=fields[3]
                cpusystem=fields[4]
                cpuinterrupt=fields[6]
                cpuidle=fields[-1]
                self._readq.nput("cpu.usr %s %s cpu=%s" % (timestamp, cpuuser, cpuid))
                self._readq.nput("cpu.nice %s %s cpu=%s" % (timestamp, cpunice, cpuid))
                self._readq.nput("cpu.sys %s %s cpu=%s" % (timestamp, cpusystem, cpuid))
                self._readq.nput("cpu.irq %s %s cpu=%s" % (timestamp, cpuinterrupt, cpuid))
                self._readq.nput("cpu.idle %s %s cpu=%s" % (timestamp, cpuidle, cpuid))
            line = self.p_top.stdout.readline()

    def close(self):
        self.close_subprocess_async(self.p_top, __name__)


if __name__ == "__main__":
    from Queue import Queue
    cpus_pctusage_inst = CpusPctusage(None, None, Queue())
    cpus_pctusage_inst()

