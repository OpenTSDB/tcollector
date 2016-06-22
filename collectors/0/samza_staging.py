#!/usr/bin/python
import sys

from collectors.lib.samza_metric_reporter import SamzaMetricReporter

CONSUMER_GROUP_ID = "tcollector_samza_staging"

KAFKA_BOOTSTRAP_SERVERS = [
    "kafkaStaging-1:9092",
    "kafkaStaging-2:9092"
]


def main():

    reporter = SamzaMetricReporter(CONSUMER_GROUP_ID, KAFKA_BOOTSTRAP_SERVERS)
    reporter.run()

if __name__ == "__main__":
    sys.exit(main())
