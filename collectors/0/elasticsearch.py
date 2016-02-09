#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2011-2013  The tcollector Authors.
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

"""ElasticSearch collector"""  # Because ES is cool, bonsai cool.
# Tested with ES 0.16.5, 0.17.x, 0.90.1 .

import errno
import httplib
try:
  import json
except ImportError:
  json = None  # Handled gracefully in main.  Not available by default in <2.6
import socket
import sys
import time
import re

from collectors.lib import utils
from collectors.etc import elasticsearch_conf


COLLECTION_INTERVAL = 15  # seconds
DEFAULT_TIMEOUT = 10.0    # seconds

# regexes to separate differences in version numbers
PRE_VER1 = re.compile(r'^0\.')
VER1 = re.compile(r'^1\.')

STATUS_MAP = {
  "green": 0,
  "yellow": 1,
  "red": 2,
}


class ESError(RuntimeError):
  """Exception raised if we don't get a 200 OK from ElasticSearch."""

  def __init__(self, resp):
    RuntimeError.__init__(self, str(resp))
    self.resp = resp


def request(server, uri):
  """Does a GET request of the given uri on the given HTTPConnection."""
  server.request("GET", uri)
  resp = server.getresponse()
  if resp.status != httplib.OK:
    raise ESError(resp)
  return json.loads(resp.read())


def cluster_health(server):
  return request(server, "/_cluster/health")


def cluster_state(server):
  return request(server, "/_cluster/state"
                 + "?filter_routing_table=true&filter_metadata=true&filter_blocks=true")


def node_status(server):
  return request(server, "/")


def node_stats(server, version):
  # API changed in v1.0
  if PRE_VER1.match(version):
    url = "/_cluster/nodes/_local/stats"
  # elif VER1.match(version):
  #   url = "/_nodes/_local/stats"
  else:
    url = "/_nodes/_local/stats"
  return request(server, url)

def printmetric(metric, ts, value, tags):
  if tags:
    tags = " " + " ".join("%s=%s" % (name, value)
                          for name, value in tags.iteritems())
  else:
    tags = ""
  print ("%s %d %s %s"
         % (metric, ts, value, tags))

def _traverse(metric, stats, ts, tags):
  #print metric,stats,ts,tags
  if isinstance(stats,dict):
    if "timestamp" in stats:
      ts = stats["timestamp"] / 1000 # ms -> s
    for key in stats.keys():
      if key != "timestamp":
        _traverse(metric + "." + key, stats[key], ts, tags)
  if isinstance(stats, (list, set, tuple)):
    count = 0
    for value in stats:
      _traverse(metric + "." + str(count), value, ts, tags)
      count += 1
  if utils.is_numeric(stats):
    printmetric(metric, ts, stats, tags)
  return

def _collect_server(server, version):
  ts = int(time.time())
  rootmetric = "elasticsearch"
  nstats = node_stats(server, version)
  cluster_name = nstats["cluster_name"]
  nodeid, nstats = nstats["nodes"].popitem()
  node_name = nstats["name"]
  tags = {"cluster": cluster_name, "node": node_name}

  is_master = nodeid == cluster_state(server)["master_node"]
  printmetric(rootmetric + ".is_master", ts, int(is_master), tags)
  if is_master:
    ts = int(time.time())  # In case last call took a while.
    cstats = cluster_health(server)
    for stat, value in cstats.iteritems():
      if stat == "status":
        value = STATUS_MAP.get(value, -1)
      elif not utils.is_numeric(value):
        continue
      printmetric(rootmetric + ".cluster." + stat, ts, value, tags)

  _traverse(rootmetric, nstats, ts, tags)

def main(argv):
  utils.drop_privileges()
  socket.setdefaulttimeout(DEFAULT_TIMEOUT)
  servers = []

  if json is None:
    utils.err("This collector requires the `json' Python module.")
    return 1

  for conf in elasticsearch_conf.get_servers():
    server = httplib.HTTPConnection( *conf )
    try:
      server.connect()
    except socket.error, (erno, e):
      if erno == errno.ECONNREFUSED:
        continue
      raise
    servers.append( server )

  if len( servers ) == 0:
    return 13  # No ES running, ask tcollector to not respawn us.

  status = node_status(server)
  version = status["version"]["number"]

  while True:
    for server in servers:
      _collect_server(server, version)
    time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
  sys.exit(main(sys.argv))
