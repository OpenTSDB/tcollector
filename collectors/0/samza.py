#!/usr/bin/python

import json
import re
import sys

from kafka import KafkaConsumer

from collectors.lib import utils


KAFKA_BOOTSTRAP_SERVERS = [
    'kafkaprod-1b-east-f4cd4a26-cageylamb.us-east-1.optimizely:9092',
    'kafkaprod-1e-east-65c215ce-obscenejerboa.us-east-1.optimizely:9092',
    'kafkaprod-1e-east-66c215cd-brownroebuck.us-east-1.optimizely:9092',
    'kafkaprod-1b-east-f3cd4a21-purringtiger.us-east-1.optimizely:9092',''
    'kafkaprod-1c-east-dfdadf73-sassywombat.us-east-1.optimizely:9092',
    'kafkaprod-1c-east-efac8a43-drabape.us-east-1.optimizely:9092'
]

KAFKA_METRICS_TOPIC = 'samza_metrics'  # Pulls metrics from the "samza_metrics" Kafka topic
KAFKA_METRICS_GROUP_ID = 'tcollector'
SAMZA_CONSUMER_LAG_METRIC_NAME = 'samza.consumer.lag'
CONSUMER_LAG_PATTERN = re.compile(r'kafka.+-(\d+)-messages-behind-high-watermark')

def main(argv):

    utils.drop_privileges()
    consumer = KafkaConsumer(KAFKA_METRICS_TOPIC,
                             group_id=KAFKA_METRICS_GROUP_ID,
                             bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    for message in consumer:

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
        report_jvm_and_container_metrics(metrics_raw, header_raw)
        report_consumer_lag(metrics_raw, header_raw)


def report_jvm_and_container_metrics(metrics_raw, header_raw):

    metrics = {}
    for m in ['org.apache.samza.metrics.JvmMetrics', 'org.apache.samza.container.SamzaContainerMetrics']:
        if m in  metrics_raw:
            metrics[m] = metrics_raw[m]

    tags = create_standard_tags(header_raw)
    ts = int(header_raw['time'] / 1000)

    for metric_type, metric_map in metrics.iteritems():
        for metric_name, metric_val in metric_map.iteritems():
            print_jvm_and_container_metric(sanitize(metric_type),
                        sanitize(metric_name),
                        ts,
                        metric_val,
                        tags)
        sys.stdout.flush()


def report_consumer_lag(metrics_raw, header_raw):

    m = 'org.apache.samza.system.kafka.KafkaSystemConsumerMetrics'

    if m in  metrics_raw:
        metric = metrics_raw[m]
        tags = create_standard_tags(header_raw)
        ts = int(header_raw['time'] / 1000)

        for metric_name, metric_val in metric.iteritems():

            m = CONSUMER_LAG_PATTERN.match(metric_name)
            if m:
                # Partition number is a part of the metric name when reported to Kafka.
                # Include it as a tag instead so that the metric can be aggregated.
                tags['partition'] = m.group(1)

                print_consumer_lag(
                            ts,
                            metric_val,
                            tags)
        sys.stdout.flush()


def create_standard_tags(header_raw):
    tags = {}
    tags['job-name'] = "%s-%s" % (header_raw['job-name'], header_raw['job-id'])
    tags['container-name'] = header_raw['container-name']
    tags['host'] = header_raw['host']
    return tags


def print_jvm_and_container_metric(metric_type, metric_name, ts, value, tags):
    if (is_valid_value(value)):
        print ("%s.%s %d %s %s" %
               (metric_type.replace('org.apache.', ''), metric_name, ts, value, to_tsdb_tag_str(tags)))


def print_consumer_lag(ts, value, tags):
    if (is_valid_value(value)):
        print ("%s %d %s %s" % (SAMZA_CONSUMER_LAG_METRIC_NAME, ts, value, to_tsdb_tag_str(tags)))


def is_valid_value(value):
    return unicode(str(value), 'utf-8').isnumeric()

def to_tsdb_tag_str(tags):
    if tags:
          tags_str = " " + " ".join("%s=%s" % (sanitize(name), v)
                                for name, v in tags.iteritems())
    else:
      tags_str = ""

    return tags_str



def sanitize(s):
    return re.sub('[^0-9a-zA-Z-_.]+', '-', str(s))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
