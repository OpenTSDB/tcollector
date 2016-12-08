#!/usr/bin/env python

import time
import os
import sys

class _dummy(object):
    pass
me = os.path.split(os.path.realpath(sys.modules[_dummy.__module__].__file__))[0]
sys.path.insert(0, os.path.join(os.path.split(me)[0], '0'))
import docker_containers

def test_trimContainerName():
    cnt = {"Names": ["/name"]}
    assert docker_containers.trimContainerName(cnt) == "name"

def test_Metric():
    """ Check assembly of metric """
    now = time.gmtime()
    m = docker_containers.Metric("metric", now, 10)
    assert m.name == "metric"
    assert m.value == 10
    assert m.event_time == now
    assert m.dims == set([])
    m.add_dims(["hello=world"])
    assert m.dims == set(["hello=world"])
    got = m.getMetricLines()
    assert got == "metric %s 10 hello=world" % int(time.mktime(now))


def test_Stats():
    """ Check if the constructor of Stats works correctly """
    now = time.gmtime()
    cnt = {
        "Names": ["/name"],
        "Id": "cntHASH",
        "Image": "qnib/test",
        "ImageID": "sha256:123"
    }
    exp_dims = [
        "container_name=name",
        "container_id=cntHASH",
        "image_name=qnib/test",
        "image_id=sha256:123"
    ]
    s = docker_containers.Stats(cnt, now)
    assert s.dims == exp_dims
    assert s.event_time == now
    # Memory
    stats = {
        "memory_stats": {
            "usage": 368640
        }
    }
    m = s.evalMem(stats)
    got = m[0].getMetricLines()
    exp = "memory.usage %s 368640 container_id=cntHASH container_name=name image_id=sha256:123 image_name=qnib/test" % int(time.mktime(now))
    assert got == exp
    # Network
    stats = {
        "networks": {
            "eth0": {
                "rx_packets": 368640
            }
        }
    }
    m = s.evalNet(stats)
    got = m[0].getMetricLines()
    exp = "net.eth0.rx_packets %s 368640 container_id=cntHASH container_name=name image_id=sha256:123 image_name=qnib/test" % int(time.mktime(now))
    assert got == exp
    # CPU
    pre = {
        "system_cpu_usage": 500,
        "cpu_usage": {
            "usage_in_usermode": 200,
            "usage_in_kernelmode": 200,
            "percpu_usage": [ 100, 100 ]
            }
    }
    cur = {
        "system_cpu_usage": 520,
        "cpu_usage": {
            "usage_in_usermode": 210,
            "usage_in_kernelmode": 210,
            "percpu_usage": [ 110, 110 ]
            }
    }
    m = s.evalCpu(pre, cur)
    got = m[0].getMetricLines()
    exp = "cpu.total.kernel %s 50.0 container_id=cntHASH container_name=name image_id=sha256:123 image_name=qnib/test" % int(time.mktime(now))
    assert got == exp
