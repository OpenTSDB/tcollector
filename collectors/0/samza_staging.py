#!/usr/bin/python

import json
import re
import sys

from kafka import KafkaConsumer

from collectors.lib import utils

ENV = 'staging'

def main(argv):

    utils.drop_privileges()

    # Pulls metrics from the "samza_metrics" Kafka topic
    consumer = KafkaConsumer('samza_metrics',
                           group_id='samza_metrics',
                           bootstrap_servers=['kafkastaging-1b-east-fd8d3028-resonantdromedary.us-east-1.optimizely:9092',
                                              'kafkastaging-1e-east-0a3cafa9-enchantedrhinoceros.us-east-1.optimizely'])

    for message in consumer:

        metric_json = json.loads(message.value)
        metric_header = metric_json['header']
        ts = int(metric_header['time'] / 1000)

        metric_header['env'] = ENV
        # Only 5 tags allowed.
        metric_header['metric-source'] = "%s.%s.%s.%s" % \
                                         (sanitize(metric_header['container-name']),
                                          sanitize(metric_header['source']),
                                          sanitize(metric_header['host']),
                                          sanitize(metric_header['reset-time']))
        metric_header.pop("time", None)
        metric_header.pop("samza-version", None)
        metric_header.pop("version", None)
        metric_header.pop("job-id", None)
        metric_header.pop("host", None)
        metric_header.pop("reset-time", None)
        metric_header.pop("host", None)
        metric_header.pop("source", None)
        metric_header.pop("container-name", None)

        for metric_type, metric_map in metric_json['metrics'].iteritems():
            for metric_name, metric_val in metric_map.iteritems():
                print_metric(sanitize(metric_type),
                            sanitize(metric_name),
                            ts,
                            metric_val,
                            **metric_header)
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
        print ("%s.%s %d %s %s" % (metric_type, metric_name, ts, value, tags))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
