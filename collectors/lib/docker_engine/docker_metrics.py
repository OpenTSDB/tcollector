#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2010-2013  The tcollector Authors.
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

from __future__ import print_function
import time
import requests
from prometheus_client.parser import text_string_to_metric_families
from collectors.lib.docker_engine.metric import Metric

class DockerMetrics(object):
    def __init__(self, url):
        self._url = url
        self.event_time = time.gmtime()

    def get_endpoint(self):
        """ Fetches the endpoint """
        ret = []
        r = requests.get(self._url)
        if r.status_code != 200:
            print("Error %s: %s" % (r.status_code, r.text))
        else:
            for line in r.iter_lines(decode_unicode=True):
                if not line.startswith('#'):
                    ret.extend(self.eval_prometheus_line(self.event_time, line))
        return ret

    @staticmethod
    def eval_prometheus_line(etime, line):
        ret = []
        for family in text_string_to_metric_families(line):
            for sample in family.samples:
                dims = []
                for kv in sample[1].items():
                    dims.append("%s=%s" % kv)
                m = Metric("docker.{0}".format(*sample), etime, sample[2], dims)
                ret.append(m)
        return ret
