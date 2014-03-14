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


COLLECTION_INTERVAL = 15  # seconds
DEFAULT_TIMEOUT = 10.0    # seconds
ES_HOST = "localhost"
ES_PORT = 9200  # TCP port on which ES listens.

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


def main(argv):
  utils.drop_privileges()
  socket.setdefaulttimeout(DEFAULT_TIMEOUT)
  server = httplib.HTTPConnection(ES_HOST, ES_PORT)
  try:
    server.connect()
  except socket.error, (erno, e):
    if erno == errno.ECONNREFUSED:
      return 13  # No ES running, ask tcollector to not respawn us.
    raise
  if json is None:
    utils.err("This collector requires the `json' Python module.")
    return 1

  status = node_status(server)
  version = status["version"]["number"]
  nstats = node_stats(server, version)
  cluster_name = nstats["cluster_name"]
  nodeid, nstats = nstats["nodes"].popitem()

  ts = None
  def printmetric(metric, value, **tags):
    if tags:
      tags = " " + " ".join("%s=%s" % (name, value)
                            for name, value in tags.iteritems())
    else:
      tags = ""
    print ("elasticsearch.%s %d %s cluster=%s%s"
           % (metric, ts, value, cluster_name, tags))

  while True:
    ts = int(time.time())
    nstats = node_stats(server, version)
    # Check that the node's identity hasn't changed in the mean time.
    if nstats["cluster_name"] != cluster_name:
      utils.err("cluster_name changed from %r to %r"
          % (cluster_name, nstats["cluster_name"]))
      return 1
    this_nodeid, nstats = nstats["nodes"].popitem()
    if this_nodeid != nodeid:
      utils.err("node ID changed from %r to %r" % (nodeid, this_nodeid))
      return 1

    is_master = nodeid == cluster_state(server)["master_node"]
    printmetric("is_master", int(is_master))
    if is_master:
      ts = int(time.time())  # In case last call took a while.
      cstats = cluster_health(server)
      for stat, value in cstats.iteritems():
        if stat == "status":
          value = STATUS_MAP.get(value, -1)
        elif not utils.is_numeric(value):
          continue
        printmetric("cluster." + stat, value)

    if "os" in nstats:
       ts = nstats["os"]["timestamp"] / 1000  # ms -> s
    if "timestamp" in nstats:
       ts = nstats["timestamp"] / 1000  # ms -> s

    if "indices" in nstats:
       indices = nstats["indices"]
       if  "docs" in indices:
          printmetric("num_docs", indices["docs"]["count"])
       if  "store" in indices:
          printmetric("indices.size", indices["store"]["size_in_bytes"])
       if  "indexing" in indices:
          d = indices["indexing"]
          printmetric("indexing.index_total", d["index_total"])
          printmetric("indexing.index_time", d["index_time_in_millis"])
          printmetric("indexing.index_current", d["index_current"])
          printmetric("indexing.delete_total", d["delete_total"])
          printmetric("indexing.delete_time_in_millis", d["delete_time_in_millis"])
          printmetric("indexing.delete_current", d["delete_current"])
          del d
       if  "get" in indices:
          d = indices["get"]
          printmetric("get.total", d["total"])
          printmetric("get.time", d["time_in_millis"])
          printmetric("get.exists_total", d["exists_total"])
          printmetric("get.exists_time", d["exists_time_in_millis"])
          printmetric("get.missing_total", d["missing_total"])
          printmetric("get.missing_time", d["missing_time_in_millis"])
          del d
       if  "search" in indices:
          d = indices["search"]
          printmetric("search.query_total", d["query_total"])
          printmetric("search.query_time", d["query_time_in_millis"])
          printmetric("search.query_current", d["query_current"])
          printmetric("search.fetch_total", d["fetch_total"])
          printmetric("search.fetch_time", d["fetch_time_in_millis"])
          printmetric("search.fetch_current", d["fetch_current"])
          del d
       if "cache" in indices:
          d = indices["cache"]
          printmetric("cache.field.evictions", d["field_evictions"])
          printmetric("cache.field.size", d["field_size_in_bytes"])
          printmetric("cache.filter.count", d["filter_count"])
          printmetric("cache.filter.evictions", d["filter_evictions"])
          printmetric("cache.filter.size", d["filter_size_in_bytes"])
          del d
       if "merges" in indices:
          d = indices["merges"]
          printmetric("merges.current", d["current"])
          printmetric("merges.total", d["total"])
          printmetric("merges.total_time", d["total_time_in_millis"] / 1000.)
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
         printmetric("process.open_file_descriptors", open_fds)
       if "cpu" in process:
          d = process["cpu"]
          printmetric("process.cpu.percent", d["percent"])
          printmetric("process.cpu.sys", d["sys_in_millis"] / 1000.)
          printmetric("process.cpu.user", d["user_in_millis"] / 1000.)
          del d
       if "mem" in process:
          d = process["mem"]
          printmetric("process.mem.resident", d["resident_in_bytes"])
          printmetric("process.mem.shared", d["share_in_bytes"])
          printmetric("process.mem.total_virtual", d["total_virtual_in_bytes"])
          del d
       del process
    if "jvm" in nstats:
       jvm = nstats["jvm"]
       ts = jvm["timestamp"] / 1000  # ms -> s
       if "mem" in jvm:
          d = jvm["mem"]
          printmetric("jvm.mem.heap_used", d["heap_used_in_bytes"])
          printmetric("jvm.mem.heap_committed", d["heap_committed_in_bytes"])
          printmetric("jvm.mem.non_heap_used", d["non_heap_used_in_bytes"])
          printmetric("jvm.mem.non_heap_committed", d["non_heap_committed_in_bytes"])
          del d
       if "threads" in jvm:
          d = jvm["threads"]
          printmetric("jvm.threads.count", d["count"])
          printmetric("jvm.threads.peak_count", d["peak_count"])
          del d
       for gc, d in jvm["gc"]["collectors"].iteritems():
         printmetric("jvm.gc.collection_count", d["collection_count"], gc=gc)
         printmetric("jvm.gc.collection_time",
                     d["collection_time_in_millis"] / 1000., gc=gc)
       del jvm
       del d
    if "network" in nstats:
       for stat, value in nstats["network"]["tcp"].iteritems():
         if utils.is_numeric(value):
           printmetric("network.tcp." + stat, value)
       for stat, value in nstats["transport"].iteritems():
         if utils.is_numeric(value):
           printmetric("transport." + stat, value)
    # New in ES 0.17:
    for stat, value in nstats.get("http", {}).iteritems():
      if utils.is_numeric(value):
        printmetric("http." + stat, value)
    del nstats
    time.sleep(COLLECTION_INTERVAL)


if __name__ == "__main__":
  sys.exit(main(sys.argv))
