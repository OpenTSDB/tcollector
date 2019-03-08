#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2013 The tcollector Authors.
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
METRICS_TO_REPORT = {
    "FRONTEND": ["scur", "stot", "rate", "req_tot", "req_rate"],
    "BACKEND": ["scur", "stot", "rate", "req_tot", "req_rate"],
    "servers": ["scur", "stot", "rate", "req_tot", "req_rate"]
}

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
    "act": "active_servers",
    "bck": "backup_servers",
    "chkfail": "failed_checks",
    "chkdown": "UP_to_DOWN_transitions",
    "lastchg": "last_status_change_in_seconds",
    "downtime": "total_downtime_in_seconds",
    "qlimit": "queue_limit",
    "pid": "process_id",
    "iid": "unique_proxy_id",
    "sid": "service_id",
    "throttle": "warm_up_status",
    "lbtot": "load_balancer_selection_count",
    "tracked": "id_of_tracked_server",
    "rate": "session_rate",
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
     pid = subprocess.check_output(["pidof", "-s", "haproxy"])
  except subprocess.CalledProcessError:
     return None
  return pid.rstrip()

def find_conf_file(pid):
  """Returns the conf file of haproxy."""
  try:
     output = subprocess.check_output(["ps", "--no-headers", "-o", "cmd", pid])
  except subprocess.CalledProcessError as e:
     utils.err("HAProxy (pid %s) went away? %s" % (pid, e))
     return None
  return output.split("-f")[1].split()[0]

def find_sock_file(conf_file):
  """Returns the unix socket file of haproxy."""
  try:
    fd = open(conf_file)
  except IOError as e:
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


def collect_stats(sock_file):
    """Collects stats from haproxy unix domain socket"""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)  
    try:
      sock.settimeout(COLLECTION_INTERVAL)
      sock.connect(sock_file)
      sock.send("show stat\n")
      statlines = sock.recv(10240).split('\n')
    finally:
      sock.close()

    ts = time.time()
    # eat up any empty lines that may be present
    statlines = [line for line in statlines if line != ""]

    # headers are given first, with or without the prompt present
    headers = None
    if statlines[0].startswith("> # "):
        headers = statlines[0][4:].split(',')
    elif statlines[0].startswith("# "):
        headers = statlines[0][2:].split(',')
    else:
        utils.err("No headers found in HAProxy output: %s" % (statlines[0],))
        return

    reader = csv.DictReader(statlines[1:], fieldnames=headers)

    # each line is a dict, due to the use of DictReader
    for line in reader:
        if "svname" not in line:
            continue  # skip output from non-expected lines
        if line["svname"] in ["FRONTEND", "BACKEND"]:
            for key in METRICS_TO_REPORT[line["svname"]]:
                print_metric(line, key, ts)
        else:  # svname apparently points to individual server
            for key in METRICS_TO_REPORT["servers"]:
                print_metric(line, key, ts)

    # make sure that we get our output quickly
    sys.stdout.flush()


def print_metric(line, metric, timestamp):
    """Print metric to stdout in tcollector format.

    :param line: The HAProxy output line, as a dict.
    :type line: dict
    :param metric: The HAProxy name of the metric, i.e. the key.
    :type metric: str
    :param timestamp: The time stamp of the metric.
    :type timestamp: float
    """

    if not line["svname"]:
        return  # no associated server, junk output
    value = line[metric]
    if not value:
        value = 0
    print("haproxy.%s %i %s source=%s cluster=%s"
           % (METRIC_NAMES[metric],
              timestamp,
              value,
              line["svname"],
              line["pxname"]))


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


  while True:
    collect_stats(sock_file)
    time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
  sys.exit(main())
