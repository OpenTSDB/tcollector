#!/usr/bin/env python

import datetime
from datetime import timedelta
import sys
import platform
import sys
import time
from string import Template
import logging
import re
from collectors.lib import utils


try:
  import requests
except ImportError:
  print >>sys.stderr, "Please install the requests module."
  sys.exit(1)

logging.basicConfig(
  filename='/opt/mapr/logs/mapr_metrics_opentsdb.log',
  level=logging.WARN,
  format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('mapr_metrics_opentsdb')
logger.info('starting up!' )

try:
  from collectors.etc import mapr_metrics_conf
except ImportError:
  logger.warn("No configuration found!")
  mapr_metrics_conf = None


def get_metrics(webserver_url, username, password, params):
  try:
    logger.debug("getting metrics from '%s' - params = %s" % (webserver_url, params))
    r = requests.get(webserver_url, auth=(username,password), verify=False, params=params)
  except requests.exceptions.ConnectionError as error:
    print >>sys.stderr, "Error connecting: %s" % error
    logger.warn("Connection error: %s" % error)
    raise

  try:
    r.raise_for_status()
  except requests.exceptions.HTTPError as error:
    print >>sys.stderr, "Request was not successful: %s" % error
    logger.error("HTTP error getting metrics from '%s' - %s" % (webserver_url, error))
    return 13 # tell tcollector to not respawn

  response = r.json()
  logger.debug("Got some JSON, 'data' key has %d objects", len(response['data']))
  data = response['data']
  return data

def main():
  schema = "https"

  username = mapr_metrics_conf.username
  password = mapr_metrics_conf.password
  webserver = mapr_metrics_conf.webserver
  port = mapr_metrics_conf.port
  if mapr_metrics_conf.no_ssl:
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
    with file('/opt/mapr/conf/mapr-clusters.conf') as clusters_conf:
      firstline = clusters_conf.readline()
      cluster_name = re.split('\s+', firstline)[0]
      logger.debug("cluster name is '%s'", cluster_name)
    return re.sub('\.', '_', cluster_name)

  def run(self):
    seconds_delay = 10

    while True:
      end = datetime.datetime.now()
      start = end - timedelta(seconds=seconds_delay)
      ms_start = int(start.strftime('%s')) * 1000
      ms_end = int(end.strftime('%s')) * 1000
      params = { 'nodes': platform.node(), 'start': ms_start, 'end': ms_end }

      try:
        all_metrics = get_metrics(self.webserver_url, self.username, self.password, params)
        self.failed_attempts = 0
      except requests.exceptions.ConnectionError as error:
        self.failed_attempts += 1
        logger.warn("Error connecting to %s, have experienced %d errors so far.", self.webserver_url, self.failed_attempts)
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
            self.send_gauge('node.memory.used', d['MEMORYUSED'], timestamp, tags=tags)
          except KeyError as e:
            logger.warn('%s not in metrics data.', e)

          try:
            self.send_gauge('node.capacity.available', d['SERVAVAILSIZEMB'], timestamp, tags=tags)
          except KeyError as e:
            logger.warn('%s not in metrics data.', e)

          try:
            self.send_gauge('node.capacity.used', d['SERVUSEDSIZEMB'], timestamp, tags=tags)
          except KeyError as e:
            logger.warn('%s not in metrics data.', e)

          try:
            rpccount_metric = self.metric_template.substitute(grouping='node', obj='rpc', metric='count')
            if rpccount_metric in self.last_values:
              self.send_counter(rpccount_metric, self.last_values[rpccount_metric], d['RPCCOUNT'], timestamp, tags=tags)
            self.last_values[rpccount_metric] = d['RPCCOUNT']
          except KeyError as e:
            logger.warn('%s is not in metrics data.', e)

          try:
            rpcinbytes_metric = self.metric_template.substitute(grouping='node', obj='rpc', metric='inbytes')
            if rpcinbytes_metric in self.last_values:
              self.send_counter(rpcinbytes_metric, self.last_values[rpcinbytes_metric], d['RPCINBYTES'], timestamp, tags=tags)
            self.last_values[rpcinbytes_metric] = d['RPCINBYTES']
          except KeyError as e:
            logger.warn('%s is not in metrics data.', e)

          try:
            rpcoutbytes_metric = self.metric_template.substitute(grouping='node', obj='rpc', metric='outbytes')
            if rpcoutbytes_metric in self.last_values:
              self.send_counter(rpcoutbytes_metric, self.last_values[rpcoutbytes_metric], d['RPCOUTBYTES'], timestamp, tags=tags)
            self.last_values[rpcoutbytes_metric] = d['RPCOUTBYTES']
          except KeyError as e:
            logger.warn('%s is not in metrics data.', e)
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
        #if metric in last_values:
        #	self.send_counter(metric, last_values[metric], value, timestamp, t)
        last_values[metric] = value

  def print_opentsdb_message(self, metric, timestamp, value, tags):
    tag_string = " ".join(map(lambda x: "%s=%s" % x, tags.items()))
    message = "%s %i %d %s" % (metric, timestamp, value, tag_string)
    logger.debug(message)
    print "%s\n" % message

  def send_gauge(self, metric, value, timestamp, tags={}):
    self.print_opentsdb_message(metric, timestamp, value, tags)

  def send_counter(self, metric, last_value, value, timestamp, tags={}):
    delta = value - last_value
    self.print_opentsdb_message(metric, timestamp, delta, tags)


if __name__ == "__main__":
  if mapr_metrics_conf.enabled:
    sys.stdin.close()
    sys.exit(main())
  else:
    logger.info("Enable the mapr_metrics collector if you want MapR stats.")
    sys.exit(13)
