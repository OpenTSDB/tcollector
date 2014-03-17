#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2013 The tcollector Authors.
# Additions Copyright (C) 2014 Elastisys AB.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
# General Public License for more details. You should have received a copy
# of the GNU Lesser General Public License along with this program. If not,
# see <http://www.gnu.org/licenses/>.

# Script uses UNIX socket opened by haproxy, you need to setup one with
# "stats socket" config parameter.
#
# You need to ensure that "stats timeout" (socket timeout) is big
# enough to work well with collector COLLECTION_INTERVAL constant.
# The default timeout on the "stats socket" is set to 10 seconds!
#
# See haproxy documentation for details:
# http://haproxy.1wt.eu/download/1.4/doc/configuration.txt
# section 3.1. Process management and security.

"""HAproxy collector """

import os
import socket
import sys
import time
import stat
import subprocess
import csv
from collectors.lib import utils

COLLECTION_INTERVAL = 15

# Which statistics to report. See Section 9.1 of the the following URL
# for information:
# http://haproxy.1wt.eu/download/1.4/doc/configuration.txt
METRICS_TO_REPORT = ["scur", "dreq", "ereq", "rate"]

METRIC_NAMES = {
    "pxname": "proxy_name",
    "qcur": "current_queued_requests",
    "qmax": "max_queued_requests",
    "scur": "current_sessions",
    "smax": "max_sessions",
    "slim": "sessions_limit",
    "stot": "total_sessions",
    "bin": "bytes_in",
    "bout": "bytes_out",
    "dreq": "denied_requests",
    "dresp": "denied_responses",
    "ereq": "request_errors",
    "econ": "connection_errors",
    "eresp": "response_errors",
    "wretr": "retries_warning",
    "wredis": "redispatches_warning",
    "weight": "server_weight",
    "chkfail": "number_of_failed_checks",
    "chkdown": "number_of_UP_to_DOWN_transitions",
    "lastchg": "last_status_change_in_seconds",
    "downtime": "total_downtime_in_seconds",
    "qlimit": "queue_limit",
    "pid": "process_id",
    "iid": "unique_proxy_id",
    "sid": "service_id",
    "throttle": "warm_up_status",
    "lbtot": "load_balancer_selection_count",
    "tracked": "id_of_tracked_server",
    "rate": "sessions_per_second",
    "rate_lim": "limit_on_new_sessions_per_second",
    "rate_max": "max_number_of_new_sessions_per_second",
    "check_code": "health_check_code",
    "check_duration": "health_check_duration_in_milliseconds",
    "hrsp_1xx": "http_responses_with_1xx_code",
    "hrsp_2xx": "http_responses_with_2xx_code",
    "hrsp_3xx": "http_responses_with_3xx_code",
    "hrsp_4xx": "http_responses_with_4xx_code",
    "hrsp_5xx": "http_responses_with_5xx_code",
    "hrsp_other": "http_responses_with_other_codes",
    "req_rate": "http_request_rate",
    "req_rate_max": "max_observed_http_requests",
    "req_tot": "http_requests_received",
    "cli_abrt": "client_aborted_data_transfers",
    "srv_abrt": "server_aborted_data_transfers"
}

def haproxy_pid():
  """Finds out the pid of haproxy process"""
  try:
     pid = subprocess.check_output(["pidof", "haproxy"])
  except subprocess.CalledProcessError:
     return None
  return pid.rstrip()

def find_conf_file(pid):
  """Returns the conf file of haproxy."""
  try:
     output = subprocess.check_output(["ps", "--no-headers", "-o", "cmd", pid])
  except subprocess.CalledProcessError, e:
     utils.err("HAProxy (pid %s) went away? %s" % (pid, e))
     return None
  return output.split("-f")[1].split()[0]

def find_sock_file(conf_file):
  """Returns the unix socket file of haproxy."""
  try:
    fd = open(conf_file)
  except IOError, e:
    utils.err("Error: %s. Config file path is relative: %s" % (e, conf_file))
    return None
  try:
    for line in fd:
      if line.lstrip(" \t").startswith("stats socket"):
        sock_file = line.split()[2]
        if utils.is_sockfile(sock_file):
          return sock_file
  finally:
    fd.close()


def collect_stats(sock):
  """Collects stats from haproxy unix domain socket"""
  sock.send("show stat\n")
  statlines = sock.recv(10240).split('\n')
  ts = time.time()

  headers = statlines[1][4:].split(',')

  reader = csv.DictReader(statlines[2:], fieldnames=headers)

  for line in reader:
      if line["svname"] in ["FRONTEND", "BACKEND"]:
          continue  # skip output not related to specific server
      for key in METRICS_TO_REPORT:
          value = line[key]
          if not value:
              value = 0
          print ("haproxy.%s %i %s server=%s cluster=%s"
                 % (METRIC_NAMES[key], ts, value, line["svname"], line["pxname"]))


def main():
  pid = haproxy_pid()
  if not pid:
    utils.err("Error: HAProxy is not running")
    return 13  # Ask tcollector to not respawn us.

  conf_file = find_conf_file(pid)
  if not conf_file:
    return 13

  sock_file = find_sock_file(conf_file)
  if sock_file is None:
    utils.err("Error: HAProxy is not listening on any unix domain socket")
    return 13

  sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
  sock.connect(sock_file)

  # put haproxy to interactive mode, otherwise haproxy closes
  # connection after first command.
  # See haproxy documentation section 9.2. Unix Socket commands.
  sock.send("prompt\n")

  while True:
    collect_stats(sock)
    time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
  sys.exit(main())
