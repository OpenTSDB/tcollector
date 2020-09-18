#!/usr/bin/env python

import datetime
from datetime import timedelta
import sys
import platform
import sys
import time
from string import Template
import re
from collectors.etc import mapr_metrics_conf
from collectors.lib import utils


try:
  import requests
except ImportError:
  print >>sys.stderr, "Please install the requests module."
  sys.exit(1)

try:
  from collectors.etc import mapr_metrics_conf
except ImportError:
  utils.err("No mapr_metrics configuration found!")
  sys.exit(13)

CONFIG = mapr_metrics_conf.get_config()


def get_metrics(webserver_url, username, password, params):
  try:
    r = requests.get(webserver_url, auth=(username,password), verify=False, params=params)
  except requests.exceptions.ConnectionError as error:
    print >>sys.stderr, "Error connecting: %s" % error
    utils.err("Connection error: %s" % error)
    raise

  try:
    r.raise_for_status()
  except requests.exceptions.HTTPError as error:
    print >>sys.stderr, "Request was not successful: %s" % error
    utils.err("HTTP error getting metrics from '%s' - %s" % (webserver_url, error))
    return 13 # tell tcollector to not respawn

  response = r.json()
  try:
    data = response['data']
  except KeyError as e:
    print >>sys.stderr, "Did not get a 'data' key in the response."
    print >>sys.stderr, response
    raise
  return data

def main():
  schema = "https"

  username = CONFIG['username']
  password = CONFIG['password']
  webserver = CONFIG['webserver']
  port = CONFIG['port']
  if CONFIG['no_ssl']:
    schema = "http"
  webserver_url = "%s://%s:%d/rest/node/metrics" % (schema, webserver, port)

  m = Metrics2TSD(webserver_url, username, password)
  m.run()

class Metrics2TSD:
  def __init__(self, webserver_url, username='mapr', password='mapr'):
    self.metric_template = Template('mapr.$grouping.$metric')
    self.webserver_url = webserver_url
    self.username = username
    self.password = password
    self.failed_attempts = 0
    self.last_values = { }

    self.cluster_name = self.get_cluster_name()

  def get_cluster_name(self):
    cluster_name = None
    with open('/opt/mapr/conf/mapr-clusters.conf', 'r') as clusters_conf:
      firstline = clusters_conf.readline()
      cluster_name = re.split('\s+', firstline)[0]
    return re.sub('\.', '_', cluster_name)

  def run(self):
    seconds_delay = CONFIG['interval']

    while True:
      end = datetime.datetime.now()
      start = end - timedelta(seconds=seconds_delay)
      ms_start = int(start.strftime('%s')) * 1000
      ms_end = int(end.strftime('%s')) * 1000
      nodename = platform.node().split('.')[0] # if node() returns the fqdn, the metrics can't be retrieved
      params = { 'nodes': nodename, 'start': ms_start, 'end': ms_end }

      try:
        all_metrics = get_metrics(self.webserver_url, self.username, self.password, params)
        self.failed_attempts = 0
      except requests.exceptions.ConnectionError as error:
        self.failed_attempts += 1
        utils.err("Error connecting to %s, have experienced %d errors so far." % (self.webserver_url, self.failed_attempts))
        if self.failed_attempts > 5:
          print >>sys.stderr, "Failed 5 times, exiting."
          return 13
        continue

      if len(all_metrics) > 0:
        for d in all_metrics[-1:]:
          node = d['NODE']
          timestamp = int(d['TIMESTAMP']) / 1000
          tags = {
            'node': node,
            'cluster': self.cluster_name
          }

          for group in ('DISKS','CPUS','NETWORK'):
            if group in d:
              self.group_metrics(group, self.last_values, d, tags=tags)
          try:
            self.send_gauge('mapr.memory.used', int(d['MEMORYUSED']) * (1024*1024), timestamp, tags=tags)
          except KeyError as e:
            utils.err('%s not in metrics data.' % e)

          try:
            self.send_gauge('mapr.mfs.available', int(d['SERVAVAILSIZEMB']) * (1024 * 1024), timestamp, tags=tags)
          except KeyError as e:
            utils.err('%s not in metrics data.' % e)

          try:
            self.send_gauge('mapr.mfs.used', int(d['SERVUSEDSIZEMB']) * (1024 * 1024), timestamp, tags=tags)
          except KeyError as e:
            utils.err('%s not in metrics data.' % e)

          try:
            rpccount_metric = self.metric_template.substitute(grouping='rpc', metric='count')
            if rpccount_metric in self.last_values:
              self.send_counter(rpccount_metric, self.last_values[rpccount_metric], d['RPCCOUNT'], timestamp, tags=tags)
            self.last_values[rpccount_metric] = d['RPCCOUNT']
          except KeyError as e:
            utils.err('%s is not in metrics data.' % e)

          try:
            rpcinbytes_metric = self.metric_template.substitute(grouping='rpc', metric='inbytes')
            if rpcinbytes_metric in self.last_values:
              self.send_counter(rpcinbytes_metric, self.last_values[rpcinbytes_metric], d['RPCINBYTES'], timestamp, tags=tags)
            self.last_values[rpcinbytes_metric] = d['RPCINBYTES']
          except KeyError as e:
            utils.err('%s is not in metrics data.' % e)

          try:
            rpcoutbytes_metric = self.metric_template.substitute(grouping='rpc', metric='outbytes')
            if rpcoutbytes_metric in self.last_values:
              self.send_counter(rpcoutbytes_metric, self.last_values[rpcoutbytes_metric], d['RPCOUTBYTES'], timestamp, tags=tags)
            self.last_values[rpcoutbytes_metric] = d['RPCOUTBYTES']
          except KeyError as e:
            utils.err('%s is not in metrics data.' % e)
      time.sleep(seconds_delay)


  def group_metrics(self, group, last_values, all_metrics, tags={}):
    node = all_metrics['NODE']
    timestamp = int(all_metrics['TIMESTAMP']) / 1000

    for (obj, obj_metrics) in all_metrics[group].items():
      for (metric_name, value) in obj_metrics.items():
        t = tags.copy()
        if group == 'DISKS':
          t['disk'] = obj
          if metric_name.endswith('KB'):
            metric_name = re.sub("KB", "BYTES", metric_name)
            value = value * 1024
        if group == 'CPUS':
          t['cpu'] = obj
        if group == 'NETWORK':
          t['interface'] = obj
        metric = self.metric_template.substitute(grouping=group.lower(), metric=metric_name)
        self.print_opentsdb_message(metric, timestamp, value, t)

  def print_opentsdb_message(self, metric, timestamp, value, tags):
    tag_string = " ".join(map(lambda x: "%s=%s" % x, tags.items()))
    print("%s %i %d %s" % (metric, timestamp, value, tag_string))

  def send_gauge(self, metric, value, timestamp, tags={}):
    self.print_opentsdb_message(metric, timestamp, value, tags)

  def send_counter(self, metric, last_value, value, timestamp, tags={}):
    delta = value - last_value
    self.print_opentsdb_message(metric, timestamp, delta, tags)


if __name__ == "__main__":
  if mapr_metrics_conf.enabled():
    sys.stdin.close()
    sys.exit(main())
  else:
    utils.err("Enable the mapr_metrics collector if you want MapR stats.")
    sys.exit(13)
