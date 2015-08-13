#!/usr/bin/python
import re
import sys

from collectors.lib.jmx_monitor import JmxMonitor

MONITORED_MBEANS = ["kafka.producer", ""]


class KafkaProducerJmxMonitor(JmxMonitor):
    USER = "flume"
    PROCESS_NAME = "org.apache.flume.node.Application"
    METRIC_PREFIX = "flume"

    def __init__(self, pid, cmd):
        super(KafkaProducerJmxMonitor, self).__init__(pid, cmd, MONITORED_MBEANS,
                                              KafkaProducerJmxMonitor.PROCESS_NAME)

    # Override
    def process_metric(self, timestamp, metric, tags, value, mbean_domain, mbean_properties):
        # Somewhat of a hack - Kafka producers emit some JMX metric values that are actually strings, like "event type".
        # OpenTSDB generally doesn't like these, so we ought to drop them. This is done here in a very stupid way.
        try:
            value_int = int(value)
        except:
            return

        full_metric_name = mbean_properties["name"].lstrip("-")
        # example: "host_kafkaprod-1b-east-f3cd4a21-purringtiger.us-east-1.optimizely-port_9092-ProducerRequestRateAndTimeMs"
        if full_metric_name.startswith("host"):
            # We don't care about the 'host' portion.
            metric_parts = full_metric_name.split("_")[1:]
            kafka_host = rchop(metric_parts[0], ".us-east-1.optimizely-port")
            tags += " kafka_host=%s" % kafka_host
            port_and_metric_type = metric_parts[1].split("-")
            kafka_port = port_and_metric_type[0]
            tags += " kafka_port=%s" % kafka_port
            metric_type = port_and_metric_type[1]
        # example: "raw_event-BytesPerSec" or "AllBrokersProducerRequestRateAndTimeMs"
        else:
            topic_and_metric = full_metric_name.split("-")
            if len(topic_and_metric) == 2:
                topic = topic_and_metric[0]
                tags += " topic=%s" % topic
                metric_type = topic_and_metric[1]
            else:
                metric_type = topic_and_metric[0]

        metric = ".".join([mbean_domain.lower(), mbean_properties["type"], metric_type, metric])
        self.emit(metric, timestamp, value, tags)

def rchop(st, suffix):
    if st.endswith(suffix):
        return st[:-len(suffix)]
    return st

def main():
    return KafkaProducerJmxMonitor.start_monitors()

if __name__ == "__main__":
    sys.exit(main())
