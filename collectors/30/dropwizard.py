#!/usr/bin/env python
import requests
import sys
import time

from collectors.lib import utils
from collectors.lib.optimizely_utils import format_tsd_key


URL = 'http://127.0.0.1:8081/metrics'
METRIC_PREFIX = 'dropwizard.'
METRIC_TYPES = {'counters', 'gauges', 'histograms', 'meters', 'timers'}


def request_json(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def try_parse_value(value):
    if isinstance(value, (int, long, float)):
        return value

    if not isinstance(value, str):
        return None

    try:
        return long(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return None


def try_report_metric(time_, metric_type, metric_name, value, value_type=None):
    value = try_parse_value(value)
    if value is None:
        return

    name = METRIC_PREFIX + metric_type + '.' + metric_name
    if value_type:
        name += '.' + value_type

    print format_tsd_key(name, value, time_)


def main():
    utils.drop_privileges()

    time_ = int(time.time())

    try:
        json = request_json(URL)
    except requests.RequestException as e:
        print >> sys.stderr, 'Failed to query metrics. {}'.format(e)
        return

    for metric_type in METRIC_TYPES:
        metrics = json.get(metric_type)
        if not metrics:
            continue

        for metric, values in metrics.iteritems():
            use_value_type = len(values) > 1
            for value_type, value in values.iteritems():
                if use_value_type:
                    try_report_metric(time_, metric_type, metric, value, value_type)
                else:
                    try_report_metric(time_, metric_type, metric, value)


if __name__ == '__main__':
    main()
