import unittest
import time

from .jmx_monitor import JmxMonitor
from .jmx_monitor import MAX_METRIC_AGE_IN_SECONDS

class TestJmxMonitor(unittest.TestCase):

    def test_group_metrics(self):
        metric, tag = JmxMonitor.group_metrics("TestingNumOps")
        self.assertTupleEqual((metric, tag.strip()), ("numOps", "op=Testing"))

        metric, tag = JmxMonitor.group_metrics("TestingMinTime")
        self.assertTupleEqual((metric, tag.strip()), ("minTime", "op=Testing"))

        metric, tag = JmxMonitor.group_metrics("TestingMaxTime")
        self.assertTupleEqual((metric, tag.strip()), ("maxTime", "op=Testing"))

        metric, tag = JmxMonitor.group_metrics("TestingAvgTime")
        self.assertTupleEqual((metric, tag.strip()), ("avgTime", "op=Testing"))

        # negative case
        metric, tag = JmxMonitor.group_metrics("TestingSomeOtherMetric")
        self.assertTupleEqual((metric, tag.strip()), ("TestingSomeOtherMetric", ""))

    def test_sanitize_timestamp(self):
        monitor = MockJmxMonitor()

        # timestamp is older than the previous timestamp, so we expect a ValueError
        with self.assertRaises(ValueError):
            monitor.sanitize_timestamp(0)

        # timestamp is
        with self.assertRaises(ValueError):
            monitor.sanitize_timestamp(time.time() - MAX_METRIC_AGE_IN_SECONDS)

        # should return the current time successfully
        expected_time = time.time()
        timestamp = monitor.sanitize_timestamp(expected_time)
        self.assertEqual(timestamp, int(expected_time))


class MockJmxMonitor(JmxMonitor):
    def __init__(self):
        self._prev_timestamp = 1

    def process_metric(self, timestamp, metric, value, mbean):
        pass

if __name__ == '__main__':
    unittest.main()
