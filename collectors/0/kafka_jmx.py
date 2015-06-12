#!/usr/bin/python

import sys

from collectors.lib import utils
from collectors.lib.jmx_monitor import JmxMonitor

MONITORED_MBEANS = ["kafka.", "",                       # All kafka metrics.
                    "Threading", "Count|Time$",        # Number of threads and CPU time.
                    "OperatingSystem", "OpenFile",     # Number of open files.
                    "GarbageCollector", "Collection"]  # GC runs and time spent GC-ing.


class KafkaJmxMonitor(JmxMonitor):
    USER = "kafka"
    PROCESS_NAME = "kafka.Kafka"
    METRIC_PREFIX = "kafka"

    def __init__(self, pid, cmd):
        super(KafkaJmxMonitor, self).__init__(pid,  cmd, MONITORED_MBEANS, self.PROCESS_NAME)

    # Override
    def process_metric(self, timestamp, metric, tags, value, mbean_domain, mbean_properties):
        if not mbean_domain.startswith("kafka") and not mbean_domain == "java.lang":
            utils.err("Unexpected mbean domain = %r" % mbean_domain)
            return

        if mbean_domain == "java.lang":
            jmx_service = mbean_properties.pop("type", "jvm")
        elif mbean_domain.startswith("kafka."):
            domain_parts = mbean_domain.split(".")
            # drop the kafka prefix
            mbean_domain = mbean_domain[len("kafka."):]
            jmx_service = mbean_properties.get("type", domain_parts[-1])
        else:
            return

        if mbean_properties:
            tags += " " + " ".join(k + "=" + v for k, v in
                                   mbean_properties.iteritems())

        jmx_service = JmxMonitor.SHORT_SERVICE_NAMES.get(jmx_service, jmx_service)
        metric = mbean_domain + "." + jmx_service.lower() + "." + metric

        self.emit(metric, timestamp, value, tags)


def main():
    return KafkaJmxMonitor.start_monitors()

if __name__ == "__main__":
    sys.exit(main())
