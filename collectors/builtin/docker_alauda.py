#!/usr/bin/python
"""Send docker stats counters to TSDB"""

import calendar
import socket
import sys
import time
import requests


from collectors.lib import utils
from collectors.lib.collectorbase import CollectorBase

COLLECTION_INTERVAL = 60  # seconds
DEFAULT_TIMEOUT = 10.0    # seconds

ALAUDA_HOST = "api.alauda.io"
ALAUDA_PORT = 443  # TCP port on which Alauda entpoint listens.
HTTPS_PREFIX='https://%s:%s' % (ALAUDA_HOST, ALAUDA_PORT)

DEFAULT_NAMESPACE="ylin30"
DEFAULT_TOKEN="2abaf27f019a124ef216db7c8a6f114e88d1d7d8"

MAX_EXCEPTION_HIT=5

def main():
    # create a socket
    utils.drop_privileges()
    socket.setdefaulttimeout(DEFAULT_TIMEOUT)
    
    alauda_session = requests.Session()
    alauda_session.headers.update({'Authorization': 'Token %s' % DEFAULT_TOKEN})

    numExceptionHit = 0
    while True:
        try:
            get_container_stats(alauda_session, DEFAULT_NAMESPACE, DEFAULT_TOKEN)

        except Exception, e:
            sys.stderr.write("Error:%s\n" % e)
            numExceptionHit += 1
            if numExceptionHit > MAX_EXCEPTION_HIT:                
                break

        finally:
            if numExceptionHit > MAX_EXCEPTION_HIT:
                alauda_session.close()

        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)

class HTTPError(RuntimeError):
  """Exception raised if we don't get a 200 OK from ElasticSearch."""

  def __init__(self, resp):
    RuntimeError.__init__(self, str(resp))
    self.resp = resp


def request(server, uri, token):
  """Does a GET request of the given uri, with token for the namespace."""
  server.headers.update({'Authorization': 'Token %s' % token})
  #print 'To send request: %s%s' % (HTTPS_PREFIX, uri)
  resp=server.get('%s%s' % (HTTPS_PREFIX, uri))

  if resp.status_code != 200:
    raise HTTPError(resp)

  #print 'resp: %s' % resp.json()

  return resp.json()

# Not used yet until /GET namesapces are supported.
def get_all_containers_stats(alauda_server):
    namespaces = get_namespaces(alauda_server)
    for namespace in namespaces:
        namespace_name = namespace[u'instance_name']
        get_container_stats(alauda_server, namespace_name)

# Get container metrics in one namespace
def get_container_stats(alauda_server, namespace_name, token):
    services = get_services(alauda_server, namespace_name, token)

    curr_time=int(time.time())
    num_running_services = 0
    num_target_started_services = 0
    for service in services[u'results']:
        service_name=service[u'service_name']

        # Let's print some numberical values for the service.
        service_tags="namespace=%s service=%s" % (namespace_name, service_name)

        print_metric("target_num_instances", curr_time, service[u'target_num_instances'], service_tags)
        print_metric("current_num_instances", curr_time, service[u'current_num_instances'], service_tags)
        print_metric("staged_num_instances", curr_time, service[u'staged_num_instances'], service_tags)
        print_metric("started_num_instances", curr_time, service[u'started_num_instances'], service_tags)

        # Number of services to be started
        if (service[u'target_state'] == u'STARTED'):
            num_target_started_services += 1

        # Number of running services.
        if (service[u'current_status'] == u'Running'):
            num_running_services += 1

        # Some metrics of time are in the format of "y-m-dTH:M:S.fz".
        service_start_time_sec=get_seconds_since(service, u'started_at', curr_time)
        service_stop_time_sec=get_seconds_since(service, u'stopped_at', curr_time)
        service_create_time_sec=get_seconds_since(service, u'created_at', curr_time)
        service_last_redeploy_time_sec=get_seconds_since(service, u'last_redeployed_at', curr_time)
        if (service_start_time_sec == 0):
            print_metric("service_start_time_sec", curr_time, 0, service_tags)
            print_metric("service_stop_time_sec", curr_time, service_stop_time_sec, service_tags)
        elif (service_stop_time_sec == 0):
            print_metric("service_start_time_sec", curr_time, service_start_time_sec, service_tags)
            print_metric("service_stop_time_sec", curr_time, 0, service_tags)
        elif (service_stop_time_sec > service_start_time_sec):
            print_metric("service_start_time_sec", curr_time, service_start_time_sec, service_tags)
            print_metric("service_stop_time_sec", curr_time, 0, service_tags)
        elif (service_stop_time_sec < service_start_time_sec):
            print_metric("service_start_time_sec", curr_time, 0, service_tags)
            print_metric("service_stop_time_sec", curr_time, service_stop_time_sec, service_tags)
            
        
        print_metric("service_create_time_sec", curr_time, service_create_time_sec, service_tags)
        print_metric("service_last_redeploy_time_sec", curr_time, service_last_redeploy_time_sec, service_tags)

        if u'instances' in service:
            for instance in service[u'instances']:
                metrics = get_instance_metrics(alauda_server, namespace_name, token, service_name, instance[u'uuid'])
                # print metric
                instance_tags="namespace=%s service=%s instance=%s" % (namespace_name, service_name, instance[u'uuid'])
                metric_columns = metrics[u'columns']
                for metric_point in metrics[u'points']:
                    print_metric_point(metric_columns, metric_point, instance_tags)

                instance_start_time_sec=get_seconds_since(instance, u'started_at', curr_time)
                print_metric("instance_start_time_sec", curr_time, instance_start_time_sec, instance_tags)

    # After check all services. Print number of running services and number of services to start
    print_metric("num_running_services", curr_time, num_running_services, tags="namespace=%s" % namespace_name)
    print_metric("num_target_started_services", curr_time, num_target_started_services, tags="namespace=%s" % namespace_name)



# Get seconds between from and to.
# from: a string e.g., "2015-04-14T09:47:26.895Z"
# to: a time object
def get_seconds_since(service, field, to_time):
    if (field in service and service[field] != None):        
        from_time = time.strptime(service[field], "%Y-%m-%dT%H:%M:%S.%fZ")
        return int(to_time - calendar.timegm(from_time))
    else:
        return 0;

# Not supported yet
#def get_namespaces(alauda_server):
#    return request(alauda_server, "/v1/services")

def get_services(alauda_server, namespace_name, token):
    return request(alauda_server, "/v1/services/%s" % namespace_name, token)
    

# Note that valid point_per_period are 1s, 1m, 5m, 15m, 30m, 1h, 4h, 12h, 1d, 7d, 30d
# We are using 1m. 
def get_instance_metrics(alauda_server, namespace_name, token, service_name, instance_uuid):
    # collecting the metrics in the last collection interval (i.e., 1m)
    end_time=int(time.time())
    start_time=end_time - COLLECTION_INTERVAL # time is in second.
    return request(alauda_server, \
            "/v1/services/%s/%s/instances/%s/metrics?start_time=%d&end_time=%d&point_per_period=%dm" \
            % (namespace_name, service_name, instance_uuid, start_time, end_time, COLLECTION_INTERVAL / 60), \
            token)

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
        if metric_name == "time":
            ts = metric_point[i]

        i += 1

    if ts == 0:
        sys.stderr.write("No timestamp found for tags %s\n" % tags)
    else:
        i = 0
        for metric_name in metric_columns:
            if (metric_name != "time"):
                print_metric(metric_name, ts, metric_point[i], tags)

            i += 1

def print_metric(metric, ts, value, tags=""):
    if value is not None:
        print "alauda/%s %d %s %s" % (metric, ts, value, tags)

def test():
    get_all_containers_stats(name)

def dryrun():
    while(True):
        main()
    time.sleep(10)

if __name__ == "__main__":
  sys.exit(main())
  #test()
