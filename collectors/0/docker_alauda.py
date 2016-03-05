#!/usr/bin/python
"""Send docker stats counters to TSDB"""

import httplib
import json
import socket
import subprocess
import sys
import time
import re

#print(sys.path)

from collectors.lib import utils

COLLECTION_INTERVAL = 30  # seconds
DEFAULT_TIMEOUT = 10.0    # seconds
ALAUDA_HOST = "localhost"
ALAUDA_PORT = 9200  # TCP port on which Alauda entpoint listens.

recv_data_size = 8192

def main():
    # create a socket
    utils.drop_privileges()
    socket.setdefaulttimeout(DEFAULT_TIMEOUT)
    alauda_server = httplib.HTTPConnection(ALAUDA_HOST, ALAUDA_PORT)
    try:
      alauda.connect()
    except socket.error, (erno, e):
      if erno == errno.ECONNREFUSED:
        return 13  # No ES running, ask tcollector to not respawn us.
      raise
    
    exceptionHit = False
    while True:
        try:
            get_container_stats(alauda_server, "demoapp_ns")

        except Exception, e:
            sys.stderr.write("Error:%s\n" % e)
            exceptionHit = True
            break

        finally:
            if exceptionHit:
                alauda_server.close()

        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)

class HTTPError(RuntimeError):
  """Exception raised if we don't get a 200 OK from ElasticSearch."""

  def __init__(self, resp):
    RuntimeError.__init__(self, str(resp))
    self.resp = resp


def request(server, uri):
  """Does a GET request of the given uri on the given HTTPConnection."""
  server.request("GET", uri)
  resp = server.getresponse()
  if resp.status != httplib.OK:
    raise HTTPError(resp)
  return json.loads(resp.read())

# Not used yet until /GET namesapces are supported.
def get_all_containers_stats(alauda_server):
    namespaces = get_namespaces(alauda_server)
    for namespace in namespaces:
        namespace_name = namespace[u'instance_name']
        get_container_stats(alauda_server, namespace_name)

# Get container metrics in one namespace
def get_container_stats(alauda_server, namespace):
    services = get_services(alauda_server, namespace_name)

    for service in services[u'results']:
        service_name=service[u'service_name']
        for instance in service[u'instances']:
            metrics = get_instance_metrics(alauda_server, namespace_name, service_name, instance[u'uuid'])
            # print metric
            metric_columns = metrics[u'columns']
            for metric_point in metrics[u'points']:
                print_metric_point(metric_columns, metric_point, \
                        tags="namespace=%s service=%s instance=%s" \
                        % (namespace_name, service_name, instance_uuid))



# Not supported yet
def get_namespaces(alauda_server):
    return request(alauda_server, "/v1/services")

def get_services(alauda_server, namespace_name):
    return request(alauda_server, "/v1/services/%s" % namespace_name)

def get_instances(alauda_server, namespace_name, service_name):
    return request(alauda_server, "/v1/services/%s/%s" % (namespace_name, service_name))
    
def get_instance_metrics(alauda_server, namespace_name, service_name, instance_uuid):
    # collecting the metrics in the last collection interval (i.e., 30s)
    end_time=int(time.time())
    start_time=end_time - COLLECTION_INTERVAL * 1000
    return request(alauda_server, \
            "/v1/services/%s/%s/instances/%s/metrics?start_time=%d&end_time=%d&point_per_period=%ds" \
            % (namespace_name, service_name, instance_uuid, start_time, end_time, COLLECTION_INTERVAL))

# Metric_columns contains metric names.
# "columns": [
#        "time",
#        "sequence_number",
#        "cpu_cumulative_usage",
#        "cpu_utilization",
#        "memory_usage",
#        "memory_utilization",
#        "rx_bytes",
#        "rx_errors",
#        "tx_bytes",
#        "tx_errors"
#  ],
# Each metric point has a list of metrics.
# [
#        1433753089,
#        0,
#        310527696,
#        0.012714308333173054,
#        16072704,
#        2.9937744140625,
#        84560,
#        0,
#        45786,
#        0
# ]
def print_metric_point(metric_columns, metric_point, tags):
    # first get the timestamp of the metric point.
    i = 0
    ts = 0    
    for metric_name in metric_columns:
        if metric_name = "time":
            ts = metric_point[i]

        i++

    if ts == 0:
        sys.stderr.write("No timestamp found for tags %s\n" % tags)

def print_metric(metric, ts, value, tags=""):
    if value is not None:
        print "%s %d %s %s" % (metric, ts, value, tags)

def test():
    name = "host1"
    get_all_containers_stats(name)

def dryrun():
    while(True):
        main()
    time.sleep(10)

if __name__ == "__main__":
  sys.exit(main())
  #test()
