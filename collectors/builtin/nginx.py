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
"""Nginx stats for TSDB"""

import calendar
import time
import re
import requests
from Queue import Queue

from collectors.lib.collectorbase import CollectorBase

nginx_status_url = '/nginx_status'


# There are two ways to collect Nginx's stats.
# 1. [yi-ThinkPad-T430 scripts (master)]$ curl http://localhost:8080/nginx_status
# Active connections: 2 
# server accepts handled requests
#  4 4 11 
# Reading: 0 Writing: 1 Waiting: 1

class Nginx(CollectorBase):
    def print_metric(self, metric, ts, value, tags=""):
        if value is not None:
            self._readq.nput("nginx.%s %d %s %s" % (metric, ts, value, tags))

    def __init__(self, config, logger, readq):
        super(Nginx, self).__init__(config, logger, readq)
        self.port = self.get_config('port', 8080)
        self.host = self.get_config('host', "localhost")
        self.http_prefix = 'http://%s:%s' % (self.host, self.port)


    def __call__(self):
        ts_curr = int(time.time())
        self.collect_nginx_status(ts_curr)

    def collect_nginx_status(self, ts_curr):
        try:
            stats = requests.get('%s%s' % (self.http_prefix, nginx_status_url))
            m = re.match(r"Active connections:\s+(\d+)\s+"
                         "\nserver accepts handled requests\n\s+(\d+)\s+(\d+)\s+(\d+)\s+"
                         "\nReading:\s+(\d+)\s+Writing:\s+(\d+)\s+Waiting:\s+(\d+)\s+\n",
                         stats)

            self.print_metric("active_connections", ts_curr, m.group(1))
            self.print_metric("total_accepted_connections", ts_curr, m.group(2))
            self.print_metric("total_handled_connections", ts_curr, m.group(3))
            self.print_metric("total_number_handled_requests", ts_curr, m.group(4))
            self.print_metric("number_requests_reading", ts_curr, m.group(5))
            self.print_metric("number_requests_writing", ts_curr, m.group(6))
            self.print_metric("number_requests_waiting", ts_curr, m.group(7))
            self._readq.nput("nginx.state %s %s" % (int(time.time()), '0'))
        except Exception as e:
            self._readq.nput("nginx.state %s %s" % (int(time.time()), '1'))
            self.log_error("Exception when collector nginx metrics from /nginx_status \n %s" % e)

