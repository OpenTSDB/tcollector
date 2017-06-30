#!/usr/bin/python
import sys

from collectors.lib.enrich_events_metric_reporter import EnrichEventsMetricReporter

CONSUMER_GROUP_ID = "tcollector_enrich_events_staging"

KAFKA_BOOTSTRAP_SERVERS = [
    "kafkaStaging-1:9092",
    "kafkaStaging-2:9092",
    "kafkaStaging-3:9092"
]

def main():
    reporter = EnrichEventsMetricReporter(CONSUMER_GROUP_ID, KAFKA_BOOTSTRAP_SERVERS)
    reporter.run()

if __name__ == "__main__":
    sys.exit(main())
