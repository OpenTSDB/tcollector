#!/usr/bin/python

"""
    flume stats collector

Connect to flume agents over http and gather metrics 
and make them suitable for OpenTSDB to consume

Need to config flume-ng to spit out json formatted metrics over http 
See http://flume.apache.org/FlumeUserGuide.html#json-reporting

Tested with flume-ng 1.4.0 only. So far

Based on the elastichsearch collector

"""  

import errno
import httplib
try:
  import json
except ImportError:
  json = None  # Handled gracefully in main.  Not available by default in <2.6
import socket
import sys
import time

from collectors.lib import utils


COLLECTION_INTERVAL = 15  # seconds
DEFAULT_TIMEOUT = 10.0    # seconds
FLUME_HOST = "localhost"
FLUME_PORT = 34545

# Exclude values that are not really metrics and totally pointless to keep track of
EXCLUDE = [ 'StartTime', 'StopTime', 'Type' ]

def err(msg):
  print >>sys.stderr, msg

class FlumeError(RuntimeError):
  """Exception raised if we don't get a 200 OK from Flume webserver."""
  def __init__(self, resp):
    RuntimeError.__init__(self, str(resp))
    self.resp = resp

def request(server, uri):
  """Does a GET request of the given uri on the given HTTPConnection."""
  server.request("GET", uri)
  resp = server.getresponse()
  if resp.status != httplib.OK:
    raise FlumError(resp)
  return json.loads(resp.read())


def flume_metrics(server):
  return request(server, "/metrics")

def main(argv):
  utils.drop_privileges()
  socket.setdefaulttimeout(DEFAULT_TIMEOUT)
  server = httplib.HTTPConnection(FLUME_HOST, FLUME_PORT)
  try:
    server.connect()
  except socket.error, (erno, e):
    if erno == errno.ECONNREFUSED:
      return 13  # No Flume server available, ask tcollector to not respawn us.
    raise
  if json is None:
    err("This collector requires the `json' Python module.")
    return 1

  def printmetric(metric, value, **tags):
    if tags:
      tags = " " + " ".join("%s=%s" % (name, value)
                            for name, value in tags.iteritems())
    else:
      tags = ""
    print ("flume.%s %d %s %s" % (metric, ts, value, tags))

  while True:
    # Get the metrics
    ts = int(time.time())  # In case last call took a while.
    stats = flume_metrics(server)

    for metric in stats:
	(component, name) = metric.split(".")
	tags = {component.lower(): name}
	for key,value in stats[metric].items():
	   if key not in EXCLUDE:
	       printmetric(key.lower(), value, **tags)

    time.sleep(COLLECTION_INTERVAL)


if __name__ == "__main__":
  sys.exit(main(sys.argv))
