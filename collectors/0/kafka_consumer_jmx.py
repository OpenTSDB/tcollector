#!/usr/bin/python
import re
import sys

from collectors.lib.jmx_monitor import JmxMonitor

MONITORED_MBEANS = ["kafka.consumer", ""]


class KafkaConsumerJmxMonitor(JmxMonitor):
    USER = "flume"
    PROCESS_NAME = "org.apache.flume.node.Application"
    METRIC_PREFIX = "flume"

    def __init__(self, pid, cmd):
        super(KafkaConsumerJmxMonitor, self).__init__(pid, cmd, MONITORED_MBEANS,
                                              KafkaConsumerJmxMonitor.PROCESS_NAME)

    # Override
    def process_metric(self, timestamp, metric, tags, value, mbean_domain, mbean_properties):
        # Somewhat of a hack - Kafka producers/consumers emit some JMX metric
        # values that are actually strings, like "event type". OpenTSDB
        # generally doesn't like these, so we ought to drop them. This is done
        # here in a very stupid way.
        try:
            value_int = int(value)
        except:
            return

        full_metric_name = mbean_properties["name"]
        metric_parts = full_metric_name.split("-")
        # When we split here, the first part is always the consumer group name
        consumer_group = metric_parts[0]
        # example: "flumeS3-MaxLag"
        if len(metric_parts) == 2:
            metric_type = metric_parts[1]
        # example: "flumeS3-raw_event-BytesPerSec"
        elif len(metric_parts) == 3:
            topic = metric_parts[1]
            tags += " topic=%s" % topic
            metric_type = metric_parts[2]
        else:
            return

        metric = ".".join([mbean_domain.lower(), mbean_properties["type"], metric_type, metric])
        self.emit(metric, timestamp, value, tags)

def main():
    return KafkaConsumerJmxMonitor.start_monitors()

if __name__ == "__main__":
    sys.exit(main())
