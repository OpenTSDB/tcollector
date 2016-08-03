#!/usr/bin/python
import sys

from collectors.lib.sessionization_metric_reporter import SessionizationMetricReporter

CONSUMER_GROUP_ID = "tcollector_sessionization_staging"

KAFKA_BOOTSTRAP_SERVERS = [
    "kafkaStaging-1:9092",
    "kafkaStaging-2:9092",
    "kafkaStaging-3:9092"
]


def main():

    # FIXME: Add prod reporter

    reporter = SessionizationMetricReporter(CONSUMER_GROUP_ID, KAFKA_BOOTSTRAP_SERVERS)
    reporter.run()

if __name__ == "__main__":
    sys.exit(main())
