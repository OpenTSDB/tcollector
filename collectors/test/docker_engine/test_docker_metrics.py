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

import unittest

import time

from collectors.lib.docker_engine.docker_metrics import DockerMetrics
from collectors.lib.docker_engine.metric import Metric


class TestDockerMetrics(unittest.TestCase):
    def setUp(self):
        self.now = time.gmtime()
        self.line = 'engine_daemon_network_actions_seconds_count{action="connect"} 2'

    def test_eval_prometheus_line(self):
        expected = Metric("docker.engine_daemon_network_actions_seconds_count", self.now, 2.0)
        expected.add_dims(["action=connect"])
        expected_line = expected.get_metric_lines()
        provided = DockerMetrics.eval_prometheus_line(self.now, self.line)[0]
        provided_line = provided.get_metric_lines()
        self.assertEqual(expected_line, provided_line)

if __name__ == '__main__':
    unittest.main()
