#!/usr/bin/python

import sys

from collectors.lib.kafka_jmx_monitor import KafkaJmxMonitor

MONITORED_MBEANS = ["kafka.server", "",                # Only get Kafka server, cluster, network metrics
                    "kafka.cluster", "",
                    "kafka.network", "",
                    "Threading", "Count|Time$",        # Number of threads and CPU time.
                    "OperatingSystem", "OpenFile",     # Number of open files.
                    "GarbageCollector", "Collection"]  # GC runs and time spent GC-ing.


class KafkaBrokerJmxMonitor(KafkaJmxMonitor):
    USER = "kafka"
    PROCESS_NAME = "kafka.Kafka"
    METRIC_PREFIX = "kafka"

    def __init__(self, pid, cmd):
        super(KafkaBrokerJmxMonitor, self).__init__(pid,  cmd, MONITORED_MBEANS, self.PROCESS_NAME)


def main():
    return KafkaBrokerJmxMonitor.start_monitors()

if __name__ == "__main__":
    sys.exit(main())
