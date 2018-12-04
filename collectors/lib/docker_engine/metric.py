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

import time

from collectors.etc import docker_engine_conf

CONFIG = docker_engine_conf.get_config()
DEFAULT_DIMS = CONFIG['default_dims']


class Metric(object):
    def __init__(self, name, etime, value, tags=None):
        self.name = name
        self.value = value
        self.event_time = etime
        if tags is None:
            self.dims = set([])
        else:
            self.dims = set(tags)
        self.dims.update(set(DEFAULT_DIMS))

    def add_dims(self, dims):
        self.dims.update(set(dims))

    def get_metric_lines(self):
        """ return in OpenTSDB format
        <name> <time_epoch> <value> [key=val] [key1=val1]...
        """
        m = "%s %s %s" % (self.name, int(time.mktime(self.event_time)), self.value)
        return "%s %s" % (m, " ".join(sorted(list(self.dims))))
