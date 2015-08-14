#!/usr/bin/python
import sys

from collectors.lib.kafka_jmx_monitor import KafkaJmxMonitor

MONITORED_MBEANS = ["kafka.consumer", ""]


class FlafkaConsumerJmxMonitor(KafkaJmxMonitor):
    USER = "flume"
    PROCESS_NAME = "org.apache.flume.node.Application"
    METRIC_PREFIX = "flume"

    def __init__(self, pid, cmd):
        super(FlafkaConsumerJmxMonitor, self).__init__(pid, cmd, MONITORED_MBEANS, self.PROCESS_NAME)


def main():
    return FlafkaConsumerJmxMonitor.start_monitors()

if __name__ == "__main__":
    sys.exit(main())
