#!/usr/bin/env python
"""Imports Docker stats from the docker-api"""

import json
import sys
import time
import requests

from feedparser import _parse_date as parse_date
from prometheus_client.parser import text_string_to_metric_families

from collectors.etc import docker_engine_conf
from collectors.lib import utils

CONFIG = docker_engine_conf.get_config()
COLLECTION_INTERVAL = CONFIG['interval']
DEFAULT_DIMS = CONFIG['default_dims']
ENABLED = docker_engine_conf.enabled()
METRICS_PATH = CONFIG['metrics_path']

if not ENABLED:
  sys.stderr.write("Docker-engine collector is not enabled")
  sys.exit(13)

class Metric(object):
    def __init__(self, name, etime, value, dims=None):
        self.name = name
        self.value = value
        self.event_time = etime
        if dims is None:
            self.dims = set([])
        else:
            self.dims = set(dims)
        self.dims.update(set(DEFAULT_DIMS))

    def add_dims(self, dims):
        self.dims.update(set(dims))

    def getMetricLines(self):
        """ return in OpenTSDB format
        <name> <time_epoch> <value> [key=val] [key1=val1]...
        """
        m = "%s %s %s" % (self.name, int(time.mktime(self.event_time)), self.value)
        return "%s %s" % (m, " ".join(sorted(list(self.dims))))

class Stats(object):
    def __init__(self, container, mtime):
        self.dims = [
            "container_name=%s" % trimContainerName(container),
            "container_id=%s" % container['Id'],
            "image_name=%s" % container['Image'],
            "image_id=%s" % container['ImageID']
        ]
        self.event_time = mtime

def trimContainerName(container):
    return container["Names"][0].strip("/")

def evalPrometheusLine(etime, line):
    ret = []
    for family in text_string_to_metric_families(line):
        for sample in family.samples:
            dims = []
            for kv in sample[1].items():
                dims.append("%s=%s" % kv)
            m = Metric("docker.{0}".format(*sample), etime, sample[2], dims)
            ret.append(m)
    return ret

class DockerMetrics(object):
    def __init__(self, url):
        self._url = url
        self.event_time = time.gmtime()

    def getEndpoint(self):
        """ Fetches the endpoint """
        ret = []
        r = requests.get(self._url)
        if r.status_code != 200:
            print "Error %s: %s" % (r.status_code, r.text)
        else:
            for line in r.iter_lines():
                if not line.startswith("#"):
                    ret.extend(evalPrometheusLine(self.event_time, line))
        return ret

def main():
    """docker_cpu main loop"""
    cli = DockerMetrics(METRICS_PATH)

    while True:
        for m in cli.getEndpoint():
            print m.getMetricLines()
        break

if __name__ == "__main__":
    sys.exit(main())
