#!/usr/bin/python
import sys

from collectors.lib.kafka_jmx_monitor import KafkaJmxMonitor

MONITORED_MBEANS = ["kafka.producer", ""]


class FlafkaProducerJmxMonitor(KafkaJmxMonitor):
    USER = "flume"
    PROCESS_NAME = "org.apache.flume.node.Application"
    METRIC_PREFIX = "flume"

    def __init__(self, pid, cmd):
        super(FlafkaProducerJmxMonitor, self).__init__(pid, cmd, MONITORED_MBEANS, self.PROCESS_NAME)


def main():
    return FlafkaProducerJmxMonitor.start_monitors()

if __name__ == "__main__":
    sys.exit(main())
