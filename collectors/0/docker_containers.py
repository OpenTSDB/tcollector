#!/usr/bin/env python
"""Imports Docker stats from the docker-api"""

import json
import sys
import time

from feedparser import _parse_date as parse_date
from docker import Client

from collectors.etc import docker_containers_conf
from collectors.lib import utils

CONFIG = docker_containers_conf.get_config()
print CONFIG
COLLECTION_INTERVAL = CONFIG['interval']
DEFAULT_DIMS = CONFIG['default_dims']
ENABLED = docker_containers_conf.enabled()
DOCKER_SOCK = CONFIG['socket_path']

if not ENABLED:
  sys.stderr.write("Docker collector is not enabled")
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

    def evalCpu(self, pre, cur):
        ret = []
        system_usage_ms = float(cur["system_cpu_usage"]-pre["system_cpu_usage"])
        cnt_user_ms = cur["cpu_usage"]["usage_in_usermode"]-pre["cpu_usage"]["usage_in_usermode"]
        cnt_user_percent = cnt_user_ms*100/system_usage_ms
        cnt_kernel_ms = cur["cpu_usage"]["usage_in_kernelmode"]-pre["cpu_usage"]["usage_in_kernelmode"]
        cnt_kernel_percent = cnt_kernel_ms*100/system_usage_ms
        m = Metric("cpu.total.kernel", self.event_time, cnt_kernel_percent, self.dims)
        ret.append(m)
        m = Metric("cpu.total.user", self.event_time, cnt_user_percent, self.dims)
        ret.append(m)
        m = Metric("cpu.total.all", self.event_time, cnt_kernel_percent+cnt_user_percent, self.dims)
        ret.append(m)
        for i, cur_percpu in list(enumerate(cur["cpu_usage"]["percpu_usage"])):
            dims = [item for item in self.dims]
            dims.append("cpu=%d" % i)
            cnt_user_ms = cur_percpu - pre["cpu_usage"]["percpu_usage"][i]
            cnt_kernel_ms = cur_percpu - pre["cpu_usage"]["percpu_usage"][i]
            percpu_user_percent = cnt_user_ms*100/system_usage_ms
            m = Metric("cpu.cpu%d.user" % i, self.event_time, percpu_user_percent, dims)
            ret.append(m)
            percpu_kernel_percent = cnt_kernel_ms*100/system_usage_ms
            m = Metric("cpu.cpu%d.kernel" % i, self.event_time, percpu_kernel_percent, dims)
            ret.append(m)
            m = Metric("cpu.cpu%d.all" % i, self.event_time, percpu_kernel_percent+percpu_user_percent, dims)
            ret.append(m)
        return ret

    def evalMem(self, stats):
        ret = []
        for k,v in stats["memory_stats"].items():
            if isinstance(v, dict):
                for sk, sv in v.items():
                    m = Metric("memory.%s.%s" % (k,sk), self.event_time, sv, self.dims)
                    ret.append(m)
            else:
                m = Metric("memory.%s" % k, self.event_time, v, self.dims)
                ret.append(m)
        return ret

    def evalNet(self, stats):
        ret = []
        for dev,dval in stats["networks"].items():
            for k, v in dval.items():
                m = Metric("net.%s.%s" % (dev,k), self.event_time, v, self.dims)
                ret.append(m)
        return ret


def trimContainerName(container):
    return container["Names"][0].strip("/")



def main():
    """docker_cpu main loop"""
    cli=Client(base_url=DOCKER_SOCK)
    metrics = []

    while True:
        for container in cli.containers():
            stats = cli.stats(container['Id'],stream=False)
            mtime = parse_date(stats["read"])
            s = Stats(container, mtime)
            for m in s.evalCpu(stats["precpu_stats"], stats["cpu_stats"]):
                print m.getMetricLines()
            for m in s.evalMem(stats):
                print m.getMetricLines()
            for m in s.evalNet(stats):
                print m.getMetricLines()

        if COLLECTION_INTERVAL > 0:
            time.sleep(COLLECTION_INTERVAL)
        else:
            break

if __name__ == "__main__":
    sys.exit(main())
