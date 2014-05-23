#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2011-2013  The tcollector Authors.
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

import httplib
try:
    import json
except ImportError:
    json = None
try:
    from collections import OrderedDict  # New in Python 2.7
except ImportError:
    from ordereddict import OrderedDict  # Can be easy_install'ed for <= 2.6
from collectors.lib.utils import is_numeric

EXCLUDED_KEYS = (
    "Name",
    "name"
)

class HadoopHttp(object):
    def __init__(self, service, daemon, host, port, uri="/jmx"):
        self.service = service
        self.daemon = daemon
        self.port = port
        self.host = host
        self.uri = uri
        self.server = httplib.HTTPConnection(self.host, self.port)
        self.server.connect()

    def request(self):
        self.server.request('GET', self.uri)
        resp = self.server.getresponse()
        return json.loads(resp.read())

    def poll(self):
        """
        Get metrics from the http server's /jmx page, and transform them into normalized tupes

        @return: array of tuples ([u'Context', u'Array'], u'metricName', value)
        """
        json_arr = self.request()['beans']
        kept = []
        for bean in json_arr:
            if (not bean['name']) or (not "name=" in bean['name']):
                continue
            #split the name string
            context = bean['name'].split("name=")[1].split(",sub=")
            # Create a set that keeps the first occurrence
            context = OrderedDict.fromkeys(context).keys()
            # lower case and replace spaces.
            context = [c.lower().replace(" ", "_") for c in context]
            # don't want to include the service or daemon twice
            context = [c for c in context if c != self.service and c != self.daemon]

            for key, value in bean.iteritems():
                if key in EXCLUDED_KEYS:
                    continue
                if not is_numeric(value):
                    continue
                kept.append((context, key, value))
        return kept

    def emit_metric(self, context, current_time, metric_name, value, tag_dict=None):
        if not tag_dict:
            print "%s.%s.%s.%s %d %d" % (self.service, self.daemon, ".".join(context), metric_name, current_time, value)
        else:
            tag_string = " ".join([k + "=" + v for k, v in tag_dict.iteritems()])
            print "%s.%s.%s.%s %d %d %s" % \
                  (self.service, self.daemon, ".".join(context), metric_name, current_time, value, tag_string)

    def emit(self):
        pass
