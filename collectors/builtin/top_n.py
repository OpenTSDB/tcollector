#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2010-2013  The tcollector Authors.
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
"""TopN cpu and memory stats"""

import calendar
import time
import re
import requests
from Queue import Queue
from subprocess import Popen, PIPE, CalledProcessError, STDOUT

from collectors.lib import utils
from collectors.lib.collectorbase import CollectorBase

# We collect top N usages of CPU,MEM,IO processes by calling "ps -Ao xxx".
# Then we will send metric="cpu.topn", tags:(proc=<pid>_<cmd>), ts=currTimeInSec.

class TopN(CollectorBase):
    def print_metric(self, metric, ts, value, tags=""):
        if value is not None:
            self._readq.nput("%s %d %s %s" % (metric, ts, value, tags))

    def __init__(self, config, logger, readq):
        super(TopN, self).__init__(config, logger, readq)

    def __call__(self):
	self.get_top_N("cpu.topN", "pcpu", "pcpu");
	self.get_top_N("mem.topN", "pmem", "rss");

    def get_top_N(self, metric, ps_field, ps_sort_by):
        try:
            p = Popen("ps -Ao comm,pid,%s --sort=-%s | head -n 6"%(ps_field, ps_sort_by), shell=True, stdout=PIPE, stderr=STDOUT)
            for line in p.stdout.readlines():
                self.process(line, metric)
            
            retval = p.wait()
            if retval:
                raise CalledProcessError(ret, "ps -Ao comm,pid,%s --sort=-%s | head -n 6"%(ps_field, ps_sort_by), "ps returned code %i" % retval)
        except OSError as e1:
            self.log_exception("ps -Ao comm,pid,%s --sort=-%s | head -n 6. [%s]"%(ps_field, ps_sort_by, e1))
            return
                                                      
        except CalledProcessError as e:
            self.log_exception("Error run ps in subprocess. [%s]", e)                            
            return
       
    def process(self, line, metric):
	if not ("COMMAND" in line and "PID" in line):
            tokens = line.split()
            cmd = utils.remove_invalid_characters(tokens[0])
	    pid = tokens[1]
            #print cmd, pid, tokens[2] 
            value = float(tokens[2]) # cpu or mem
            tag = "pid_cmd=%s_%s"%(pid, cmd)
            self.print_metric(metric, (int(time.time())), value, tag)


if __name__ == "__main__":
    from Queue import Queue
    from collectors.lib.utils import TestQueue
    
    inst = TopN(None, None, TestQueue())
    inst()
 
