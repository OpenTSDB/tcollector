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
import time
import sys
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
    def __init__(self, service, daemon, host, port, readq, logger, uri="/jmx"):
        self.service = service
        self.daemon = daemon
        self.port = port
        self.host = host
        self.uri = uri
        self.server = httplib.HTTPConnection(self.host, self.port)
        self.server.auto_open = True
        self._readq = readq
        self.logger = logger

    def request(self):
        try:
            self.server.request('GET', self.uri)
            resp = self.server.getresponse().read()
        except:
            resp = '{}'
            self.logger.warn('hadoop_http request failed: %s', sys.exc_info()[0])
        finally:
            self.server.close()
        return json.loads(resp)

    def poll(self):
        """
        Get metrics from the http server's /jmx page, and transform them into normalized tupes

        @return: array of tuples ([u'Context', u'Array'], u'metricName', value)
        """
        json_arr = self.request().get('beans', [])
        kept = []
        for bean in json_arr:
            try:
                if (bean['name']) and (bean['name'].startswith('java.lang:type=GarbageCollector')):
                    self.process_gc_collector(bean, kept)
                elif (bean['name']) and (bean['name'].startswith('java.lang:type=')):
                    self.process_java_lang_metrics(bean, kept)
                elif (bean['name']) and ("name=" in bean['name']):
                    # split the name string
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
            except Exception as e:
                self.logger.exception("exception in HadoopHttp when collecting %s", bean['name'])

        return kept

    def emit_metric(self, context, current_time, metric_name, value, tag_dict=None):
        if not tag_dict:
            self._readq.nput("%s.%s.%s.%s %d %d" % (self.service, self.daemon, ".".join(context), metric_name, current_time, value))
        else:
            tag_string = " ".join([k + "=" + v for k, v in tag_dict.iteritems()])
            self._readq.nput("%s.%s.%s.%s %d %d %s" % (self.service, self.daemon, ".".join(context), metric_name, current_time, value, tag_string))

    def emit(self):
        pass

    def process_gc_collector(self, bean, kept):
        context = bean['name'].split("java.lang:type=")[1].split(",name=")
        for key, value in bean.iteritems():
            if key in EXCLUDED_KEYS:
                continue
            if value is None:
                continue
            if key == 'LastGcInfo':
                context.append(key)
                for lastgc_key, lastgc_val in bean[key].iteritems():
                    if lastgc_key == 'memoryUsageAfterGc' or lastgc_key == 'memoryUsageBeforeGc':
                        context.append(lastgc_key)
                        for memusage in lastgc_val:      # lastgc_val is a list
                            context.append(memusage["key"])
                            for final_key, final_val in memusage["value"].iteritems():
                                safe_context, safe_final_key = self.safe_replace(context, final_key)
                                kept.append((safe_context, safe_final_key, final_val))
                            context.pop()
                        context.pop()
                    elif is_numeric(lastgc_val):
                        safe_context, safe_lastgc_key = self.safe_replace(context, lastgc_key)
                        kept.append((safe_context, safe_lastgc_key, lastgc_val))
                context.pop()
            elif is_numeric(value):
                safe_context, safe_key = self.safe_replace(context, key)
                kept.append((safe_context, safe_key, value))

    def process_java_lang_metrics(self, bean, kept):
        context = bean['name'].split("java.lang:type=")[1].split(",name=")
        for key, value in bean.iteritems():
            if key in EXCLUDED_KEYS:
                continue
            if value is None:
                continue
            if is_numeric(value):
                safe_context, safe_key = self.safe_replace(context, key)
                kept.append((safe_context, safe_key, value))
            elif isinstance(value, dict):   # only go one level deep since there is no other level empirically
                for subkey, subvalue in value.iteritems():
                    if is_numeric(subvalue):
                        safe_context, final_key = self.safe_replace(context, key + "." + subkey)
                        kept.append((safe_context, final_key, subvalue))

    def safe_replace(self, context, key):
        context = [c.replace(" ", "_") for c in context]
        key = key.replace(" ", "_")
        return context, key


class HadoopNode(HadoopHttp):
    """
    Class that will retrieve metrics from an Apache Hadoop DataNode's jmx page.

    This requires Apache Hadoop 1.0+ or Hadoop 2.0+.
    Anything that has the jmx page will work but the best results will com from Hadoop 2.1.0+
    """

    def __init__(self, service, daemon, host, port, replacements, readq, logger):
        super(HadoopNode, self).__init__(service, daemon, host, port, readq, logger)
        self.replacements = replacements

    def emit(self):
        current_time = int(time.time())
        metrics = self.poll()
        for context, metric_name, value in metrics:
            for k, v in self.replacements.iteritems():
                if any(c.startswith(k) for c in context):
                    context = v
            self.emit_metric(context, current_time, metric_name, value)
