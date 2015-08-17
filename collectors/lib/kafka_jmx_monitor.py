#!/usr/bin/python

from collectors.lib import utils
from collectors.lib.jmx_monitor import JmxMonitor


class KafkaJmxMonitor(JmxMonitor):

    def __init__(self, pid, cmd, monitored_mbeans, process_name):
        super(KafkaJmxMonitor, self).__init__(pid,  cmd, monitored_mbeans, process_name)


    def _valid_metric_value(self, value):
            # Hack - Kafka producers emit some JMX metric values that are strings, like "event type".
            # OpenTSDB generally doesn't like these, so we ought to drop them. This is done here in a very stupid way.
            try:
                value_float = float(value)
            except:
                return False
            else:
                return True

    def _process_kafka_consumer_metric(self, timestamp, metric, tags, value, mbean_domain, mbean_properties):
        if not self._valid_metric_value(value):
            return

        full_metric_name = mbean_properties["name"]
        metric_parts = full_metric_name.split("-")
        # When we split here, the first part is always the consumer group name
        consumer_group = metric_parts[0]
        tags += " consumer_group=%s" % consumer_group
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

    def _process_kafka_producer_metric(self, timestamp, metric, tags, value, mbean_domain, mbean_properties):
        if not self._valid_metric_value(value):
            return

        full_metric_name = mbean_properties["name"].lstrip("-")
        # ex: "host_kafkaprod-1b-east-f3cd4a21-purringtiger.us-east-1.optimizely-port_9092-ProducerRequestRateAndTimeMs"
        if full_metric_name.startswith("host"):
            metric_type, tags = self._process_host_producer_metric(full_metric_name, tags)
        # ex: "raw_event-BytesPerSec" or "AllBrokersProducerRequestRateAndTimeMs"
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

    def _process_host_producer_metric(self, full_metric_name, tags):
        # ex: "host_kafkaprod-1b-east-f3cd4a21-purringtiger.us-east-1.optimizely-port_9092-ProducerRequestRateAndTimeMs"
        # We don't care about the 'host' portion in front.
        metric_parts = full_metric_name.split("_")[1:]
        kafka_host = rchop(metric_parts[0], ".us-east-1.optimizely-port")
        tags += " kafka_host=%s" % kafka_host
        port_and_metric_type = metric_parts[1].split("-")
        kafka_port = port_and_metric_type[0]
        tags += " kafka_port=%s" % kafka_port
        metric_type = port_and_metric_type[1]
        return metric_type, tags

    # Override
    def process_metric(self, timestamp, metric, tags, value, mbean_domain, mbean_properties):
        if not mbean_domain.startswith("kafka") and not mbean_domain == "java.lang":
            utils.err("Unexpected mbean domain = %r" % mbean_domain)
            return

        if mbean_domain == "java.lang":
            jmx_service = mbean_properties.pop("type", "jvm")
        # Kafka producer metrics
        elif mbean_domain == "kafka.producer":
            self._process_kafka_producer_metric(timestamp, metric, tags, value, mbean_domain, mbean_properties)
            return
        # Kafka consumer metrics
        elif mbean_domain == "kafka.consumer":
            self._process_kafka_consumer_metric(timestamp, metric, tags, value, mbean_domain, mbean_properties)
            return
        # Kafka broker metrics
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


def rchop(st, suffix):
    if st.endswith(suffix):
        return st[:-len(suffix)]
    return st
