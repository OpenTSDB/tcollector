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

METRICS_TO_REPORT = [
    'org.apache.samza.metrics.JvmMetrics',
    'org.apache.samza.container.SamzaContainerMetrics'
]

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

        metrics = {}
        for m in METRICS_TO_REPORT:
            if m in  metrics_raw:
                metrics[m] = metrics_raw[m]

        tags = {}
        tags['job-name'] = "%s-%s" % (header_raw['job-name'], header_raw['job-id'])
        tags['container-name'] = header_raw['container-name']
        tags['host'] = header_raw['host']

        ts = int(header_raw['time'] / 1000)

        for metric_type, metric_map in metrics.iteritems():
            for metric_name, metric_val in metric_map.iteritems():
                print_metric(sanitize(metric_type),
                            sanitize(metric_name),
                            ts,
                            metric_val,
                            **tags)
            sys.stdout.flush()


def sanitize(s):
    return re.sub('[^0-9a-zA-Z-_.]+', '-', str(s))


def print_metric(metric_type, metric_name, ts, value, **tags):

    if (unicode(str(value), 'utf-8').isnumeric()):
        if tags:
          tags = " " + " ".join("%s=%s" % (sanitize(name), v)
                                for name, v in tags.iteritems())
        else:
          tags = ""
        print ("%s.%s %d %s %s" % (metric_type.replace('org.apache.', ''), metric_name, ts, value, tags))


if __name__ == "__main__":
    sys.exit(main(sys.argv))