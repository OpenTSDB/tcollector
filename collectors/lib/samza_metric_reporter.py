import json
import re
import sys

from kafka import KafkaConsumer

from collectors.lib import utils


class SamzaMetricReporter:
    """
    Samza jobs periodically publish metrics to a Kafka topic. A metric reporter
    consumes from this Kafka topic, extracts only the important metrics and prints
    to STDOUT in OpenTSDB consumable format.
    """

    SAMZA_CONSUMER_LAG_METRIC_NAME = 'samza.consumer.lag'
    CONSUMER_LAG_PATTERN = re.compile(r'kafka-(.+)-(\d+)-messages-behind-high-watermark')

    def __init__(self, consumer_group_id, kafka_bootstrap_servers, kafka_metrics_topic='samza_metrics'):
        utils.drop_privileges()
        self.consumer = KafkaConsumer(kafka_metrics_topic,
                                      group_id=consumer_group_id,
                                      bootstrap_servers=kafka_bootstrap_servers)
        self.methods_to_run = [self.report_consumer_lag, self.report_jvm_and_container_metrics]

    def run(self):
        for message in self.consumer:
            # A sample message from Kafka:
            #
            # {
            #   "metrics": {
            #     "org.apache.samza.system.SystemConsumersMetrics": { ... },
            #     "org.apache.samza.metrics.JvmMetrics": { ... },
            #     "org.apache.samza.container.SamzaContainerMetrics": { ... },
            #     "org.apache.samza.system.chooser.RoundRobinChooserMetrics": { ... },
            #     "org.apache.samza.system.kafka.KafkaSystemConsumerMetrics": { ... },
            #     "org.apache.samza.system.kafka.KafkaSystemProducerMetrics": { ... },
            #     "org.apache.samza.system.SystemProducersMetrics": { ... }
            #   },
            #   "header": {
            #     "reset-time": 1456286517161,
            #     "job-id": "1",
            #     "time": 1456350331573,
            #     "host": "hbasestaging1-1b-east-511669ab-holisticmink",
            #     "container-name": "samza-container-0",
            #     "source": "samza-container-0",
            #     "job-name": "samza-nogoalids",
            #     "samza-version": "0.9.1",
            #     "version": "0.0.1"
            #   }
            # }

            message_json = json.loads(message.value)
            metrics_raw = message_json['metrics']
            header_raw = message_json['header']

            # reporting logic is specific to the type of metric reported
            for method in self.methods_to_run:
                method(metrics_raw, header_raw)

    def report_jvm_and_container_metrics(self, metrics_raw, header_raw):

        metrics = {}
        for m in ['org.apache.samza.metrics.JvmMetrics', 'org.apache.samza.container.SamzaContainerMetrics']:
            if m in metrics_raw:
                metrics[m] = metrics_raw[m]

        tags = self.create_standard_tags(header_raw)
        ts = int(header_raw['time'] / 1000)

        for metric_type, metric_map in metrics.iteritems():
            for metric_name, metric_val in metric_map.iteritems():
                self.print_jvm_and_container_metric(self.sanitize(metric_type),
                                                    self.sanitize(metric_name),
                                                    ts,
                                                    metric_val,
                                                    tags)
            sys.stdout.flush()

    def report_consumer_lag(self, metrics_raw, header_raw):

        m = 'org.apache.samza.system.kafka.KafkaSystemConsumerMetrics'

        if m in metrics_raw:
            metric = metrics_raw[m]
            tags = self.create_standard_tags(header_raw)
            ts = int(header_raw['time'] / 1000)

            for metric_name, metric_val in metric.iteritems():

                m = self.CONSUMER_LAG_PATTERN.match(metric_name)
                if m:
                    # Kafka topic can be extracted from the metric name. This is needed when
                    # a Samza job consumes from multiple sources.
                    tags['topic'] = m.group(1)
                    # Partition number is a part of the metric name when reported to Kafka.
                    # Include it as a tag instead so that the metric can be aggregated.
                    tags['partition'] = m.group(2)

                    self.print_consumer_lag(
                        ts,
                        metric_val,
                        tags)
            sys.stdout.flush()


    def print_jvm_and_container_metric(self, metric_type, metric_name, ts, value, tags):
        if self.is_number(value):
            print ("%s.%s %d %s %s" %
                   (metric_type.replace('org.apache.', ''), metric_name, ts, value, self.to_tsdb_tag_str(tags)))

    def print_consumer_lag(self, ts, value, tags):
        if self.is_number(value):
            print ("%s %d %s %s" % (self.SAMZA_CONSUMER_LAG_METRIC_NAME, ts, value, self.to_tsdb_tag_str(tags)))


    def to_tsdb_tag_str(self, tags):
        if tags:
            tags_str = " " + " ".join("%s=%s" % (self.sanitize(name), v)
                                      for name, v in tags.iteritems())
        else:
            tags_str = ""

        return tags_str


    @staticmethod
    def create_standard_tags(header_raw):
        tags = {'job-name': "%s-%s" % (header_raw['job-name'], header_raw['job-id']),
                'container-name': header_raw['container-name'], 'host': header_raw['host']}
        return tags

    @staticmethod
    def sanitize(s):
        return re.sub('[^0-9a-zA-Z-_.]+', '-', str(s))

    @staticmethod
    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            pass

        try:
            import unicodedata
            unicodedata.numeric(s)
            return True
        except (TypeError, ValueError):
            pass
