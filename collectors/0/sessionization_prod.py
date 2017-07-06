#!/usr/bin/python
import sys

from collectors.lib.sessionization_metric_reporter import SessionizationMetricReporter

CONSUMER_GROUP_ID = "tcollector_sessionization_prod"

KAFKA_BOOTSTRAP_SERVERS = [
    'kafkaprod-1:9092',
    'kafkaprod-2:9092',
    'kafkaprod-3:9092',
    'kafkaprod-4:9092',
    'kafkaprod-5:9092',
    'kafkaprod-6:9092'
]


def main():

    # FIXME: Add prod reporter

    reporter = SessionizationMetricReporter(CONSUMER_GROUP_ID, KAFKA_BOOTSTRAP_SERVERS)
    reporter.run()

if __name__ == "__main__":
    sys.exit(main())
