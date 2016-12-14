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

from collectors.lib import utils
from collectors.lib.collectorbase import CollectorBase

NUM_USERS_TO_DEL_EMPTY_STATS = 100
nginx_status_url = 'http://127.0.0.1:80/nginx_status'


# There are two ways to collect Nginx's stats.
# 1. [yi-ThinkPad-T430 scripts (master)]$ curl http://localhost:8080/nginx_status
# Active connections: 2 
# server accepts handled requests
#  4 4 11 
# Reading: 0 Writing: 1 Waiting: 1 
#
# 2. Parsing logs.
# 

# We can parse the nginx.config which is usaully at /etc/nginx/.
# It is written as:
# server {
#   error_log /var/log/nginx/error.log
#   access_log /var/log/nginx/access.log
# }
# For now, just return the hardcoded path
def find_access_log():
    return "/var/log/nginx/access.log"


class Nginx(CollectorBase):
    def print_metric(self, metric, ts, value, tags=""):
        if value is not None:
            self._readq.nput("nginx/%s %d %s %s" % (metric, ts, value, tags))

    def __init__(self, config, logger, readq):
        super(Nginx, self).__init__(config, logger, readq)
        try:
            self.access_log = open(find_access_log())
            with utils.lower_privileges(self._logger):
                self.nginx_server = requests.Session()
                self.num_req_per_resp_code_per_user = {}

                # We just care about ethN and emN interfaces.  We specifically
                # want to avoid bond interfaces, because interface
                # stats are still kept on the child interfaces when
                # you bond.  By skipping bond we avoid double counting.
                self.access_log.seek(0)
        except:
            self.cleanup()

    def cleanup(self):
        self.safe_close(self.access_log)

    def __init__(self):
        ts_curr = int(time.time())
        # To collect status from http://nignx_server/nginx_status first.
        self.collect_nginx_status(ts_curr)

        # To parse nginx logs.
        # To continue with the current position in each loop.
        self.access_log.seek(0, 1)

        # Reset all the statistics each time when collecting metrics.
        # Note that we should explicitly set each value to zero. 
        # Otherwise, Opentsdb will not know and try interpolation instead.
        for metric in self.num_req_per_resp_code_per_user.keys():
            # There may be too many remote address and we don't want to keep so many empty metrics for memory efficiency.
            if len(self.num_req_per_resp_code_per_user[metric]) > NUM_USERS_TO_DEL_EMPTY_STATS:
                self.num_req_per_resp_code_per_user[metric] = {}
            else:
                for remote_addr in self.num_req_per_resp_code_per_user[metric]:
                    self.num_req_per_resp_code_per_user[metric][remote_addr] = 0

        for line in self.access_log:
            # This pattern is hardcoded to match this string
            # 172.17.0.1 - - [20/Mar/2016:18:14:01 +0000] "GET /api HTTP/1.1" 304 0 "http://localhost:8080/" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36" "-" "0.002"
            # which is based on the log_format:
            # 
            # log_format  main  '$remote_addr - $remote_user [$time_local] "$request" '
            #                   '$status $body_bytes_sent "$http_referer" '
            #                   '"$http_user_agent" "$http_x_forwarded_for" "$request_time"';
            # 
            # TODO: To make it more general
            #
            m = re.match(r"([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}) "  # $remote_addr
                         ".* "  # - $remote_user
                         "\[([0-9][0-9]/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/[0-9]{4}:.*)\] "  # [$time_local]
                         "\"((GET|POST|PUT|DELETE) .*)\" "  # "$request"
                         "([1-5][0-9]{2}) "  # $status
                         "([0-9]+) "  # $body_bytes_sent
                         "\"(.+)\" "  # $http_referer
                         "\"(.+)\" "  # $http_user_agent
                         "\"(.+)\" "  # $http_x_forwarded_for
                         "\"(.+)\"",  # $request_time
                         line)

            if not m:
                continue

            # m.group(0) is same as line.
            remote_addr = m.group(1)
            time_local = m.group(2)
            time_month = m.group(3)
            request = m.group(4)
            request_type = m.group(5)
            response_status = m.group(6)
            body_bytes_sent = m.group(7)
            request_time = int(float(m.group(11)) * 1000)

            ts_struct = time.strptime(time_local, "%d/%b/%Y:%H:%M:%S +0000")
            ts = calendar.timegm(ts_struct)

            self.print_metric("%s/request_time" % request_type, ts, request_time,
                              tags="nginx_remote_addr=%s nginx_status=%s" % (remote_addr, response_status))

            self.print_metric("%s/body_bytes_sent" % request_type, ts, body_bytes_sent,
                              tags="nginx_remote_addr=%s" % remote_addr)

            # Count how many request per response status in this period.
            tmp_metric_name = "%s/%s" % (request_type, response_status)
            if tmp_metric_name in self.num_req_per_resp_code_per_user:
                if remote_addr in self.num_req_per_resp_code_per_user[tmp_metric_name]:
                    self.num_req_per_resp_code_per_user[tmp_metric_name][remote_addr] += 1
                else:
                    self.num_req_per_resp_code_per_user[tmp_metric_name][remote_addr] = 1

            else:
                self.num_req_per_resp_code_per_user[tmp_metric_name] = {}
                self.num_req_per_resp_code_per_user[tmp_metric_name][remote_addr] = 1

        # Print statistics of how many requests with specific response code for each user.
        for tmp_metric_name in self.num_req_per_resp_code_per_user.keys():
            for tmp_user in self.num_req_per_resp_code_per_user[tmp_metric_name].keys():
                tmp_pair = tmp_metric_name.split('/')
                tmp_req_type = tmp_pair[0]
                tmp_resp_status = tmp_pair[1]
                self.print_metric(tmp_req_type, ts_curr, self.num_req_per_resp_code_per_user[tmp_metric_name][tmp_user],
                                  tags='nginx_remote_addr=%s nginx_status=%s nginx_status_prefix=%s' % (
                                  tmp_user, tmp_resp_status, tmp_resp_status[0]))

    def collect_nginx_status(self, ts_curr):
        stats = request(self.nginx_server, nginx_status_url)
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


class HTTPError(RuntimeError):
    """Exception raised if we don't get a 200 OK from ElasticSearch."""

    def __init__(self, resp):
        RuntimeError.__init__(self, str(resp))
        self.resp = resp


def request(server, uri):
    """Does a GET request of the given uri"""
    # print 'To send request: %s%s' % (HTTPS_PREFIX, uri)
    resp = server.get(uri)

    if resp.status_code != 200:
        raise HTTPError(resp)

    return resp.text


if __name__ == "__main__":
    inst = Nginx(None, None, Queue())
    inst()
