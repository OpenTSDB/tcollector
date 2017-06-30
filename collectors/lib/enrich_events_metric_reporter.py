import sys

from samza_metric_reporter import SamzaMetricReporter

class EnrichEventsMetricReporter(SamzaMetricReporter):

    """
    This reporter only prints enrich_events related metrics. All
    generic Samza metrics, such as number of messages processed,
    are reported by SamzaMetricReporter.
    """

    def __init__(self, consumer_group_id, kafka_bootstrap_servers, kafka_metrics_topic='samza_metrics'):
        SamzaMetricReporter.__init__(self, consumer_group_id, kafka_bootstrap_servers, kafka_metrics_topic)
        self.methods_to_run = [self.report_enrich_events_metrics]

    def report_enrich_events_metrics(self, metrics_raw, header_raw):

        m = 'com.optimizely.preprocessing.samza.enrichevent.Metrics'

        if m in metrics_raw:
            metric = metrics_raw[m]
            tags = self.create_standard_tags(header_raw)
            ts = int(header_raw['time'] / 1000)
            tags['source'] = self.sanitize(header_raw['source'])

            for metric_name, metric_val in metric.iteritems():
                self.print_metrics(
                    metric_name,
                    ts,
                    metric_val,
                    tags)

            sys.stdout.flush()

    def print_metrics(self, metric_name, ts, value, tags):
        if self.is_number(value):
            print ("%s.%s %d %s %s" % ("enrichevents.metrics", metric_name, ts, value, self.to_tsdb_tag_str(tags)))
