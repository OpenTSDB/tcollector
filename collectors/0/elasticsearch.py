#!/usr/bin/python
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

def printmetric(metric, ts, value, **tags):
  if tags:
    tags = " " + " ".join("%s=%s" % (name, value)
                          for name, value in tags.iteritems())
  else:
    tags = ""
  print ("elasticsearch.%s %d %s %s"
         % (metric, ts, value, tags))

def _collect_server(server, version):
  ts = int(time.time())
  nstats = node_stats(server, version)
  cluster_name = nstats["cluster_name"]
  nodeid, nstats = nstats["nodes"].popitem()
  node_name = nstats["name"]

  is_master = nodeid == cluster_state(server)["master_node"]
  printmetric("is_master", ts, int(is_master), node=node_name, cluster=cluster_name)
  if is_master:
    ts = int(time.time())  # In case last call took a while.
    cstats = cluster_health(server)
    for stat, value in cstats.iteritems():
      if stat == "status":
        value = STATUS_MAP.get(value, -1)
      elif not utils.is_numeric(value):
        continue
      printmetric("cluster." + stat, ts, value, cluster=cluster_name)

  if "os" in nstats:
     ts = nstats["os"]["timestamp"] / 1000  # ms -> s
  if "timestamp" in nstats:
     ts = nstats["timestamp"] / 1000  # ms -> s

  if "indices" in nstats:
     indices = nstats["indices"]
     if  "docs" in indices:
        printmetric("num_docs", ts, indices["docs"]["count"], node=node_name, cluster=cluster_name)
     if  "store" in indices:
        printmetric("indices.size", ts, indices["store"]["size_in_bytes"], node=node_name, cluster=cluster_name)
     if  "indexing" in indices:
        d = indices["indexing"]
        printmetric("indexing.index_total", ts, d["index_total"], node=node_name, cluster=cluster_name)
        printmetric("indexing.index_time", ts, d["index_time_in_millis"], node=node_name, cluster=cluster_name)
        printmetric("indexing.index_current", ts, d["index_current"], node=node_name, cluster=cluster_name)
        printmetric("indexing.delete_total", ts, d["delete_total"], node=node_name, cluster=cluster_name)
        printmetric("indexing.delete_time_in_millis", ts, d["delete_time_in_millis"], node=node_name, cluster=cluster_name)
        printmetric("indexing.delete_current", ts, d["delete_current"], node=node_name, cluster=cluster_name)
        del d
     if  "get" in indices:
        d = indices["get"]
        printmetric("get.total", ts, d["total"], node=node_name, cluster=cluster_name)
        printmetric("get.time", ts, d["time_in_millis"], node=node_name, cluster=cluster_name)
        printmetric("get.exists_total", ts, d["exists_total"], node=node_name, cluster=cluster_name)
        printmetric("get.exists_time", ts, d["exists_time_in_millis"], node=node_name, cluster=cluster_name)
        printmetric("get.missing_total", ts, d["missing_total"], node=node_name, cluster=cluster_name)
        printmetric("get.missing_time", ts, d["missing_time_in_millis"], node=node_name, cluster=cluster_name)
        del d
     if  "search" in indices:
        d = indices["search"]
        printmetric("search.query_total", ts, d["query_total"], node=node_name, cluster=cluster_name)
        printmetric("search.query_time", ts, d["query_time_in_millis"], node=node_name, cluster=cluster_name)
        printmetric("search.query_current", ts, d["query_current"], node=node_name, cluster=cluster_name)
        printmetric("search.fetch_total", ts, d["fetch_total"], node=node_name, cluster=cluster_name)
        printmetric("search.fetch_time", ts, d["fetch_time_in_millis"], node=node_name, cluster=cluster_name)
        printmetric("search.fetch_current", ts, d["fetch_current"], node=node_name, cluster=cluster_name)
        del d
     if "cache" in indices:
        d = indices["cache"]
        printmetric("cache.field.evictions", ts, d["field_evictions"], node=node_name, cluster=cluster_name)
        printmetric("cache.field.size", ts, d["field_size_in_bytes"], node=node_name, cluster=cluster_name)
        printmetric("cache.filter.count", ts, d["filter_count"], node=node_name, cluster=cluster_name)
        printmetric("cache.filter.evictions", ts, d["filter_evictions"], node=node_name, cluster=cluster_name)
        printmetric("cache.filter.size", ts, d["filter_size_in_bytes"], node=node_name, cluster=cluster_name)
        del d
     if "merges" in indices:
        d = indices["merges"]
        printmetric("merges.current", ts, d["current"], node=node_name, cluster=cluster_name)
        printmetric("merges.total", ts, d["total"], node=node_name, cluster=cluster_name)
        printmetric("merges.total_time", ts, d["total_time_in_millis"] / 1000., node=node_name, cluster=cluster_name)
        del d
     del indices
  if "process" in nstats:
     process = nstats["process"]
     ts = process["timestamp"] / 1000  # ms -> s
     open_fds = process.get("open_file_descriptors")  # ES 0.17
     if open_fds is None:
       open_fds = process.get("fd")  # ES 0.16
       if open_fds is not None:
         open_fds = open_fds["total"]
     if open_fds is not None:
       printmetric("process.open_file_descriptors", ts, open_fds, node=node_name, cluster=cluster_name)
     if "cpu" in process:
        d = process["cpu"]
        printmetric("process.cpu.percent", ts, d["percent"], node=node_name, cluster=cluster_name)
        printmetric("process.cpu.sys", ts, d["sys_in_millis"] / 1000., node=node_name, cluster=cluster_name)
        printmetric("process.cpu.user", ts, d["user_in_millis"] / 1000., node=node_name, cluster=cluster_name)
        del d
     if "mem" in process:
        d = process["mem"]
        printmetric("process.mem.resident", ts, d["resident_in_bytes"], node=node_name, cluster=cluster_name)
        printmetric("process.mem.shared", ts, d["share_in_bytes"], node=node_name, cluster=cluster_name)
        printmetric("process.mem.total_virtual", ts, d["total_virtual_in_bytes"], node=node_name, cluster=cluster_name)
        del d
     del process
  if "jvm" in nstats:
     jvm = nstats["jvm"]
     ts = jvm["timestamp"] / 1000  # ms -> s
     if "mem" in jvm:
        d = jvm["mem"]
        printmetric("jvm.mem.heap_used", ts, d["heap_used_in_bytes"], node=node_name, cluster=cluster_name)
        printmetric("jvm.mem.heap_committed", ts, d["heap_committed_in_bytes"], node=node_name, cluster=cluster_name)
        printmetric("jvm.mem.non_heap_used", ts, d["non_heap_used_in_bytes"], node=node_name, cluster=cluster_name)
        printmetric("jvm.mem.non_heap_committed", ts, d["non_heap_committed_in_bytes"], node=node_name, cluster=cluster_name)
        del d
     if "threads" in jvm:
        d = jvm["threads"]
        printmetric("jvm.threads.count", ts, d["count"], node=node_name, cluster=cluster_name)
        printmetric("jvm.threads.peak_count", ts, d["peak_count"], node=node_name, cluster=cluster_name)
        del d
     for gc, d in jvm["gc"]["collectors"].iteritems():
       printmetric("jvm.gc.collection_count", ts, d["collection_count"], gc=gc, node=node_name, cluster=cluster_name)
       printmetric("jvm.gc.collection_time", ts,
                   d["collection_time_in_millis"] / 1000., gc=gc, node=node_name, cluster=cluster_name)
     del jvm
     del d
  if "network" in nstats:
     for stat, value in nstats["network"]["tcp"].iteritems():
       if utils.is_numeric(value):
         printmetric("network.tcp." + stat, ts, value, node=node_name, cluster=cluster_name)
     for stat, value in nstats["transport"].iteritems():
       if utils.is_numeric(value):
         printmetric("transport." + stat, ts, value, node=node_name, cluster=cluster_name)
  # New in ES 0.17:
  for stat, value in nstats.get("http", {}).iteritems():
    if utils.is_numeric(value):
      printmetric("http." + stat, ts, value, node=node_name, cluster=cluster_name)
  del nstats

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
