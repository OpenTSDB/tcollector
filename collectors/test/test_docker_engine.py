#!/usr/bin/env python

import time
import os
import sys

class _dummy(object):
    pass
me = os.path.split(os.path.realpath(sys.modules[_dummy.__module__].__file__))[0]
sys.path.insert(0, os.path.join(os.path.split(me)[0], '0'))
import docker_engine

def test_trimContainerName():
    cnt = {"Names": ["/name"]}
    assert docker_engine.trimContainerName(cnt) == "name"

def test_Metric():
    """ Check assembly of metric """
    now = time.gmtime()
    m = docker_engine.Metric("metric", now, 10)
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
    s = docker_engine.Stats(cnt, now)
    assert s.dims == exp_dims
    assert s.event_time == now

def test_evalPrometheusLine():
    # Prometheus line
    now = time.gmtime()
    line = 'engine_daemon_network_actions_seconds_count{action="connect"} 2'
    gotMetric = docker_engine.evalPrometheusLine(now, line)
    got = gotMetric[0].getMetricLines()
    exp = "docker.engine_daemon_network_actions_seconds_count %s 2.0 action=connect" % int(time.mktime(now))
    assert exp == got
