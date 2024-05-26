#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2013  The tcollector Authors.
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
#

import sys
import time
from http.client import HTTPConnection
import json

from collectors.lib import utils

try:
    import schedule
except ImportError:
    utils.err("schedule library is not installed")
    sys.exit(13)  # ask tcollector to not re-start us

try:
    from prometheus_client.parser import text_string_to_metric_families
except ImportError:
    utils.err("prometheus_client.parser is not installed")
    sys.exit(13)  # ask tcollector to not re-start us

try:
    from collectors.etc import prometheus_conf
except ImportError:
    prometheus_conf = None

COLLECTION_INTERVAL = 15  # seconds
DEFAULT_TIMEOUT = 10.0  # seconds
TARGET_HOST = "localhost"
TARGET_PORT = 8080
BASE_LABELS = ""
SETTINGS = {}


class PrometheusCollector(object):
    def __init__(self, service, daemon, host, port, uri="/metrics"):
        self.service = service
        self.daemon = daemon
        self.port = port
        self.host = host
        self.uri = uri
        self.server = HTTPConnection(self.host, self.port)
        self.server.auto_open = True

    def request(self):
        try:
            self.server.request('GET', self.uri)
            resp = self.server.getresponse().read().decode('utf-8')
        except:
            resp = '{}'
        finally:
            self.server.close()
        return text_string_to_metric_families(resp)

    def poll(self):
        """
        Get metrics from the http server's /jmx page, and transform them into normalized tupes
        @return: array of tuples ([u'Context', u'Array'], u'metricName', value)
        """
        global SETTINGS
        global BASE_LABELS
        defaultLabels = BASE_LABELS
        if 'include_service_tags' in SETTINGS.keys():
            defaultLabels = "service=%s daemon=%s " % (
                self.service.replace(" ", "_").replace(":", "_").replace("+", "_").replace("'", "_").replace("__", "_"),
                self.daemon.replace(" ", "_").replace(":", "_").replace("+", "_").replace("'", "_").replace("__", "_")
            )

        convertedMetrics = []
        metricFamilies = self.request()
        for family in metricFamilies:
            for sample in family.samples:
                metric = sample.name.replace("_", ".").replace(" ", "_")
                value = sample.value
                labels = defaultLabels
                labelsObj = sample.labels

                for tag in labelsObj:
                    ## This bit is potentially Hazelcast specific or prone to break in other cases. It should be looked into.
                    if tag == 'tag0':
                        subTags = labelsObj[tag].replace("tag0=", "").replace("\"", "").split("=")
                        tagK = subTags[0]
                        tagV = subTags[1]
                    else:
                        tagK = tag
                        tagV = labelsObj[tag]

                    labels = ("%s %s=%s" % (
                        labels,
                        tagK.replace(" ", "_").replace(":", "_").replace("+", "_"),
                        tagV.replace(" ", "_").replace(":", "_").replace("+", "_").replace("'", "_").replace("__", "_")
                    ))
                convertedMetrics.append([
                    metric,
                    value,
                    labels
                ])
        return convertedMetrics

    def emit(self):
        current_time = int(time.time())
        metrics = self.poll()

        for metric, value, tags in metrics:
            print("%s %s %s %s" % (metric, current_time, float(value), tags))
            sys.stdout.flush()


def query_target(service, instance, host, port):
    httpClient = PrometheusCollector(service, instance, host, port)
    httpClient.emit()


def main(argv):
    global TARGET_HOST
    global TARGET_PORT
    global TARGET_SERVICE
    global TARGET_INSTANCE
    global COLLECTION_INTERVAL

    if not (prometheus_conf and prometheus_conf.enabled() and prometheus_conf.get_settings()):
        sys.exit(13)

    settings = prometheus_conf.get_settings()

    if 'default_timeout' in settings.keys():
        DEFAULT_TIMEOUT = settings['default_timeout']

    if 'collection_interval' in settings.keys():
        COLLECTION_INTERVAL = settings['collection_interval']

    if 'target_host' in settings.keys():
        TARGET_HOST = settings['target_host']

    if 'target_port' in settings.keys():
        TARGET_PORT = settings['target_port']

    TARGET_SERVICE = 'prometheus'
    TARGET_INSTANCE = 'bridge'

    if 'target_name' in settings.keys():
        TARGET_SERVICE = settings['target_name']

    if 'target_instance' in settings.keys():
        TARGET_INSTANCE = settings['target_instance']

    if 'targets' in settings.keys():
        for target in settings['targets']:
            service = TARGET_SERVICE
            instance = TARGET_INSTANCE
            host = TARGET_HOST
            port = TARGET_PORT
            interval = COLLECTION_INTERVAL

            if 'target_service' in target.keys():
                service = target['target_service']

            if 'target_host' in target.keys():
                host = target['target_host']

            if 'target_port' in target.keys():
                port = target['target_port']

            if 'collection_interval' in target.keys():
                interval = target['collection_interval']

            if 'target_instance' in target.keys():
                instance = target['target_instance']
            elif target['target_host'] and target['target_port']:
                instance = "%s:%s" % (target['target_host'], target['target_port'])

            schedule.every(interval).seconds.do(query_target, service=service, instance=instance, host=host, port=port)
    else:
        schedule.every(COLLECTION_INTERVAL).seconds.do(
            query_target, service=TARGET_SERVICE, instance=TARGET_INSTANCE, host=TARGET_HOST, port=TARGET_PORT)

    # Loop so that the scheduling task
    # keeps on running all time.
    while True:
        # Checks whether a scheduled task
        # is pending to run or not
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
