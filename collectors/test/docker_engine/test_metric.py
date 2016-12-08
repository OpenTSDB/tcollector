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
from unittest import TestCase

from collectors.lib.docker_engine.metric import Metric


class TestMetric(TestCase):

    def setUp(self):
        self.now = time.gmtime()
        self.metric = Metric("metric", self.now, 10)

    def test_metric_name(self):
        self.assertEqual(self.metric.name, "metric")

    def test_metric_value(self):
        self.assertEqual(self.metric.value, 10)

    def test_metric_event_time(self):
        self.assertEqual(self.metric.event_time, self.now)

    def test_metric_dims(self):
        self.assertEqual(self.metric.dims, set([]))

    def test_add_dims(self):
        self.metric.add_dims(["hello=world"])
        self.assertEqual(self.metric.dims, {"hello=world"})

    def test_get_metric_lines(self):
        expected = "metric %s 10 " % int(time.mktime(self.now))
        self.assertEqual(expected, self.metric.get_metric_lines())

    def test_get_metric_lines_with_single_dim(self):
        self.metric.add_dims(["hello=world"])
        expected = "metric %s 10 hello=world" % int(time.mktime(self.now))
        self.assertEqual(expected, self.metric.get_metric_lines())

    def test_get_metric_lines_with_multiple_dim(self):
        self.metric.add_dims(["hello=world", "foo=bar"])
        expected = "metric %s 10 foo=bar hello=world" % int(time.mktime(self.now))
        self.assertEqual(expected, self.metric.get_metric_lines())


if __name__ == '__main__':
    unittest.main()
