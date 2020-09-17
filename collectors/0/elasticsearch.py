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
try:
  import json
except ImportError:
  json = None  # Handled gracefully in main.  Not available by default in <2.6
import socket
import sys
import threading
import time
import re

from collectors.lib import utils
from collectors.etc import elasticsearch_conf

try:
  from http.client import HTTPConnection, OK
except ImportError:
  from httplib import HTTPConnection, OK


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


def request(server, uri, json_in = True):
  """Does a GET request of the given uri on the given HTTPConnection."""
  server.request("GET", uri)
  resp = server.getresponse()
  if resp.status != OK:
    raise ESError(resp)
  if json_in:
    return json.loads(resp.read())
  else:
    return resp.read()


def cluster_health(server):
  return request(server, "/_cluster/health")


def cluster_stats(server):
  return request(server, "/_cluster/stats")


def cluster_master_node(server):
  return request(server, "/_cat/master", json_in = False).split()[0]


def index_stats(server):
  return request(server, "/_cat/indices?v&bytes=b", json_in = False)


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
  # Warning, this should be called inside a lock
  if tags:
    tags = " " + " ".join("%s=%s" % (name.replace(" ",""), value.replace(" ",""))
                          for name, value in tags.items())
  else:
    tags = ""
  # Convert any bool values to int, as opentsdb only accepts int or float.
  if isinstance(value, bool):
      value = int(value)
  print("%s %d %s %s"
         % (metric, ts, value, tags))

def _traverse(metric, stats, ts, tags):
  """
     Recursively traverse the json tree and print out leaf numeric values
     Please make sure you call this inside a lock and don't add locking
     inside this function
  """
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
  if utils.is_numeric(stats) and not isinstance(stats, bool):
    if isinstance(stats, int):
      stats = int(stats)
    printmetric(metric, ts, stats, tags)
  return

def _collect_indices(server, metric, tags, lock):
  ts = int(time.time())
  rawtable = index_stats(server).split("\n")
  header = rawtable.pop(0).strip()
  headerlist = [x.strip() for x in header.split()]
  for line in rawtable:
    # Copy the cluster tag
    newtags = {"cluster": tags["cluster"]}
    # Now parse each input
    values = line.split()
    count = 0
    for value in values:
      try:
        value = float(value)
        if int(value) == value:
          value = int(value)
        # now print value
        with lock:
          printmetric(metric + ".cluster.byindex." + headerlist[count], ts, value, newtags)
      except ValueError:
        # add this as a tag
        newtags[headerlist[count]] = value
      count += 1

def _collect_master(server, nodeid, metric, tags, lock):
  ts = int(time.time())
  chealth = cluster_health(server)
  if "status" in chealth:
    with lock:
      printmetric(metric + ".cluster.status", ts,
        STATUS_MAP.get(chealth["status"], -1), tags)
  with lock:
    _traverse(metric + ".cluster", chealth, ts, tags)

  ts = int(time.time())  # In case last call took a while.
  cstats = cluster_stats(server)
  with lock:
    _traverse(metric + ".cluster", cstats, ts, tags)

def _collect_server(server, version, lock):
  ts = int(time.time())
  rootmetric = "elasticsearch"
  nstats = node_stats(server, version)
  cluster_name = nstats["cluster_name"]
  nodeid, nstats = nstats["nodes"].popitem()
  node_name = nstats["name"]
  tags = {"cluster": cluster_name, "node": node_name}
  #tags.update(nstats["attributes"])

  if nodeid == cluster_master_node(server):
      is_master = 1
  else:
      is_master = 0
  with lock:
    printmetric(rootmetric + ".is_master", ts, is_master, tags)
  if is_master:
    _collect_master(server, nodeid, rootmetric, tags, lock)

    _collect_indices(server, rootmetric, tags, lock)

  with lock:
    _traverse(rootmetric, nstats, ts, tags)


def main(argv):
  utils.drop_privileges()
  socket.setdefaulttimeout(DEFAULT_TIMEOUT)
  servers = []

  if json is None:
    utils.err("This collector requires the `json' Python module.")
    return 1

  for conf in elasticsearch_conf.get_servers():
    server = HTTPConnection( *conf )
    try:
      server.connect()
    except socket.error as exc:
      if exc.errno == errno.ECONNREFUSED:
        continue
      raise
    servers.append( server )

  if len( servers ) == 0:
    return 13  # No ES running, ask tcollector to not respawn us.

  lock = threading.Lock()
  while True:
    threads = []
    for server in servers:
      status = node_status(server)
      version = status["version"]["number"]
      t = threading.Thread(target = _collect_server, args = (server, version, lock))
      t.start()
      threads.append(t)
    for thread in threads:
      thread.join()
    time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
  sys.exit(main(sys.argv))
