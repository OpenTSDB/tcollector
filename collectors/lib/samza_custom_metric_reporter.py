import sys

from samza_metric_reporter import SamzaMetricReporter

class SamzaCustomMetricReporter(SamzaMetricReporter):

    """
    This reporter only prints custom samza jobs related metrics. All
    generic Samza metrics, such as number of messages processed,
    are reported by SamzaMetricReporter.
    """

    def __init__(self, consumer_group_id, kafka_bootstrap_servers, kafka_metrics_topic='samza_metrics'):
        SamzaMetricReporter.__init__(self, consumer_group_id, kafka_bootstrap_servers, kafka_metrics_topic)
        self.methods_to_run = [self.report_samza_custom_metrics]

    def report_samza_custom_metrics(self, metrics_raw, header_raw):
        class_to_metric_name_map = {
            'com.optimizely.sessionization.samza.SessionizationTask' : 'sessionization.metrics',
            'com.optimizely.preprocessing.samza.enrichevent.Metrics' : 'enrichevents.metrics'
        }

        for m in class_to_metric_name_map:
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
                        tags,
                        class_to_metric_name_map[m])

                sys.stdout.flush()

    def print_metrics(self, metric_name, ts, value, tags, metric_name_string):
        if self.is_number(value):
            print ("%s.%s %d %s %s" % (metric_name_string, metric_name, ts, value, self.to_tsdb_tag_str(tags)))
