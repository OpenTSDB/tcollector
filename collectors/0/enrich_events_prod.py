#!/usr/bin/python
import sys

from collectors.lib.enrich_events_metric_reporter import EnrichEventsMetricReporter

CONSUMER_GROUP_ID = "tcollector_enrich_events_prod"

KAFKA_BOOTSTRAP_SERVERS = [
    'kafkaprod-1:9092',
    'kafkaprod-2:9092',
    'kafkaprod-3:9092',
    'kafkaprod-4:9092',
    'kafkaprod-5:9092',
    'kafkaprod-6:9092'
]

def main():
    reporter = EnrichEventsMetricReporter(CONSUMER_GROUP_ID, KAFKA_BOOTSTRAP_SERVERS)
    reporter.run()

if __name__ == "__main__":
    sys.exit(main())
