!/usr/bin/env python
import requests
import sys
import time

from collectors.lib import utils
from collectors.lib.optimizely_utils import format_tsd_key


URL = 'http://127.0.0.1:8081/metrics'
METRIC_PREFIX = 'springboot.'


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


def try_report_metric(time_, metric_name, value):
    value = try_parse_value(value)
    if value is None:
        return

    name = METRIC_PREFIX + metric_name
    print format_tsd_key(name, value, time_)


def main():
    """Springboot's metrics end-point returns a simple flat map of metric name -> value."""
    utils.drop_privileges()
    time_ = int(time.time())

    try:
        json = request_json(URL)
    except requests.RequestException as e:
        print >> sys.stderr, 'Failed to query metrics. {}'.format(e)
        return
    for metric, value in json.iteritems():
        try_report_metric(time_, metric, value)


if __name__ == '__main__':
    main()
