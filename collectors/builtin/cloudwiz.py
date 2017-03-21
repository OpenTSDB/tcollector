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
"""Cloudwiz Client stats for TSDB"""

import subprocess
import time
import re
import os
from Queue import Queue

from collectors.lib.collectorbase import CollectorBase

collector_pid_file = '/opt/cloudwiz-agent/altenv/var/run/collector.pid'
supervisord_pid_file = '/opt/cloudwiz-agent/altenv/var/run/supervisord.pid'
#uagent_pid_file = ...


class Cloudwiz(CollectorBase):

    def __init__(self, config, logger, readq):
        super(Cloudwiz, self).__init__(config, logger, readq)

        self.cpu_total = {}
        self.cpu_stime = {}
        self.cpu_utime = {}

    def __call__(self):
        self.get_pids()
        self.cleanup()
        ts_curr = int(time.time())
        self.collect_all_metrics(ts_curr)

    def get_pids(self):
        self.metrics = {}

        pid = self.get_pid_from_file(collector_pid_file)
        if pid == 0:
            pid = self.get_pid_from_pgrep('/opt/cloudwiz-agent/agent/runner.py')
        if pid != 0:
            self.metrics["cloudwiz.collector"] = pid
        else:
            self.log_error("Can't get pid of collector")

        pid = self.get_pid_from_file(supervisord_pid_file)
        if pid == 0:
            pid = self.get_pid_from_pgrep('/opt/cloudwiz-agent/altenv/bin/supervisord')
        if pid != 0:
            self.metrics["cloudwiz.supervisord"] = pid
        else:
            self.log_error("Can't get pid of supervisord")

        pid = self.get_pid_from_pgrep('/opt/cloudwiz-agent/filebeat-1.3.1/filebeat -c')
        if pid != 0:
            self.metrics["cloudwiz.filebeat"] = pid
        else:
            self.log_error("Can't get pid of filebeat")

        pid = self.get_pid_from_pgrep('/opt/cloudwiz-agent/uagent/daemon.py')
        if pid != 0:
            self.metrics["cloudwiz.uagent"] = pid
        else:
            self.log_error("Can't get pid of uagent")

    def cleanup(self):
        # Can't delete entries while looping through it
        # So we just delete one at a time.
        for pid in self.cpu_total:
            found = False
            for metric in self.metrics:
                if int(pid) == self.metrics[metric]:
                    found = True
                    break
            if not found:
                self.cpu_total.pop(pid)
                self.cpu_stime.pop(pid)
                self.cpu_utime.pop(pid)
                break

    def get_pid_from_file(self, pidfile):
        try:
            f = open(pidfile, 'r')
            pid = f.read()
            f.close()
            return int(pid)
        except Exception as e:
            return 0

    def get_pid_from_pgrep(self, cmd):
        proc = subprocess.Popen(['/usr/bin/pgrep', '-f', str(cmd)], stdout=subprocess.PIPE)
        line = proc.stdout.readline()
        if line == '':
            return 0
        else:
            return int(line)

    def collect_memory(self, metric, pid, ts_curr):
        last = None
        proc = subprocess.Popen(['/usr/bin/pmap', '-x', str(pid)], stdout=subprocess.PIPE)
        while True:
            line = proc.stdout.readline()
            if line == '':
                break
            last = line
        if last != None:
            arr = last.split()
            self._readq.nput("%s.memory.total %s %s" % (metric, ts_curr, arr[2]))
            self._readq.nput("%s.memory.rss %s %s" % (metric, ts_curr, arr[3]))
            self._readq.nput("%s.memory.dirty %s %s" % (metric, ts_curr, arr[4]))

    def get_cpu_total(self):
        total = 0
        try:
            f = open(os.path.join('/', 'proc', 'stat'), 'r')
            for line in f:
                if line.startswith('cpu '):
                    tokens = line.split()
                    for i in range(1, len(tokens)-1):
                        total += int(tokens[i])
            f.close()
        except Exception as e:
            self.log_error("Can't read /proc/stat: " + str(e))
        return total

    def get_cpu_stime(self, pid):
        stime = 0
        try:
            f = open(os.path.join('/', 'proc', str(pid), 'stat'), 'r')
            line = f.readline()
            tokens = line.split()
            stime = int(tokens[14]) + int(tokens[16])
            f.close()
        except Exception as e:
            self.log_error("Can't read /proc/"+str(pid)+"/stat")
        return stime

    def get_cpu_utime(self, pid):
        utime = 0
        try:
            f = open(os.path.join('/', 'proc', str(pid), 'stat'), 'r')
            line = f.readline()
            tokens = line.split()
            utime = int(tokens[13]) + int(tokens[15])
            f.close()
        except Exception as e:
            self.log_error("Can't read /proc/"+str(pid)+"/stat")
        return utime

    def collect_cpu(self, metric, pid, ts_curr):
        total = self.get_cpu_total()
        stime = self.get_cpu_stime(pid)
        utime = self.get_cpu_utime(pid)

        if str(pid) in self.cpu_total:
            sys = 100 * (stime - int(self.cpu_stime[str(pid)])) / (total - int(self.cpu_total[str(pid)]))
            usr = 100 * (utime - int(self.cpu_utime[str(pid)])) / (total - int(self.cpu_total[str(pid)]))
            self._readq.nput("%s.cpu.sys %s %s" % (metric, ts_curr, sys))
            self._readq.nput("%s.cpu.usr %s %s" % (metric, ts_curr, usr))

        self.cpu_total[str(pid)] = str(total)
        self.cpu_stime[str(pid)] = str(stime)
        self.cpu_utime[str(pid)] = str(utime)

    #def collect_net(self, metric, pid, ts_curr):

    def collect_io(self, metric, pid, ts_curr):
        try:
            f = open(os.path.join('/', 'proc', str(pid), 'io'), 'r')
            for line in f:
                if line.startswith('read_bytes: '):
                    token = line.split()
                    self._readq.nput("%s.io.read %s %s" % (metric, ts_curr, token[1]))
                elif line.startswith('write_bytes: '):
                    token = line.split()
                    self._readq.nput("%s.io.write %s %s" % (metric, ts_curr, token[1]))
            f.close()
        except Exception as e:
            self.log_error("Can't get io stat for pid " + str(pid))

    def collect_one_metric(self, metric, pid, ts_curr):
        # collect CPU usage
        self.collect_cpu(metric, pid, ts_curr)
        # collect Memory usage
        self.collect_memory(metric, pid, ts_curr)
        # collect Network usage
        #self.collect_net(metric, pid, ts_curr)
        # collect IO usage
        self.collect_io(metric, pid, ts_curr)

    def collect_all_metrics(self, ts_curr):
        try:
            for metric in self.metrics:
                pid = self.metrics[metric]
                self.collect_one_metric(metric, pid, ts_curr)
        except Exception as e:
            self._readq.nput("cloudwiz.state %s %s" % (ts_curr, '1'))
            self.log_error("Exception when collecting cloudwiz metrics\n%s" % e)

