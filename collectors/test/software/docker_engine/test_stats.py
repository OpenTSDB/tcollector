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

from collectors.lib.docker_engine.stats import Stats


class TestStats(unittest.TestCase):
    def setUp(self):
        self.now = time.gmtime()
        self.container = {
            "Names": ["/name"],
            "Id": "cntHASH",
            "Image": "qnib/test",
            "ImageID": "sha256:123"
        }

    def test_stats_dims(self):
        expected_dims = [
            "container_name=name",
            "container_id=cntHASH",
            "image_name=qnib/test",
            "image_id=sha256:123"
        ]

        s = Stats(self.container, self.now)

        self.assertEqual(expected_dims, s.dims)

    def test_stats_etime(self):
        s = Stats(self.container, self.now)
        self.assertEqual(self.now, s.event_time)

    def test_trim_container_name(self):
        # cnt = {"Names": ["/name"]}
        expected = "name"
        self.assertEqual(expected, Stats.trim_container_name(self.container))


if __name__ == '__main__':
    unittest.main()
