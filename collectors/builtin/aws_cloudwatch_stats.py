#!/usr/bin/python
import sys
import time
import datetime
import re
import exceptions
import threading
import Queue
from time import mktime
from collectors.lib import utils
from collectors.lib.collectorbase import CollectorBase

try:
    import boto.ec2
    from boto.ec2.cloudwatch import CloudWatchConnection
    from boto.ec2.cloudwatch import regions
except ImportError:
    exit(13)

STATISTICS = frozenset([
    'Minimum',
    'Maximum',
    'Average',
    'Sum',
    'SampleCount'
])

sendQueue = Queue.Queue()


def cloudwatch_connect_to_region(region):
    access_key, secret_access_key = get_accesskey_secretkey()
    try:
        conn = boto.ec2.cloudwatch.connect_to_region(region, aws_access_key_id=access_key,
                                                     aws_secret_access_key=secret_access_key)
    except:
        print "Unexpected error:", sys.exc_info()[0]
    else:
        return conn


def cloudwatch_list_metrics(conn):
    return conn.list_metrics()


def format_timestamp(ts):
    st = time.strptime(ts, "%Y-%m-%d %H:%M:%S")
    dt = datetime.datetime.fromtimestamp(mktime(st))
    return dt.strftime("%s")


def build_tag_list(metric_name, region, dimensions):
    tags = "region=" + str(region)

    for tagk, tagv in dimensions.iteritems():
        tagkey = str(tagk)
        tagval = str(tagv[0])
        tags += " %s=%s" % (tagkey, tagval)

    if metric_name == 'networkout':
        metric_name = 'network'
        tags += " %s=%s" % ('direction', 'out')

    if metric_name == 'networkin':
        metric_name = 'network'
        tags += " %s=%s" % ('direction', 'in')

    return metric_name.strip().lower(), tags.strip().lower()


def ec2_connect_to_region(region):
    access_key, secret_access_key = get_accesskey_secretkey()
    return boto.ec2.connect_to_region(region, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)


def ec2_list_regions():
    ec2_regions = []
    for i in boto.ec2.cloudwatch.regions():
        ec2_regions.append(str(i.name))
    return ec2_regions


def get_accesskey_secretkey():
    return ('<access_key_id>', '<secret_access_key>')


class AwsCloudWatchStats(CollectorBase):
    def __init__(self, config, logger):
        super(AwsCloudWatchStats, self).__init__(config, logger)
        if self.get_config('interval', None) < 60:
            sys.stderr.write(
                "AWS Cloudwatch Stats is an heavy collector and should not be run more than once per minute.\n")
            raise

    def __call__(self):
        try:
            utils.drop_privileges()
            self.validate_config()
            regions = ec2_list_regions()
            for reg in regions:
                for statistic in STATISTICS:
                    t = threading.Thread(target=self.handle_region, kwargs={"region": reg, "statistic": statistic})
                    t.start()
            while threading.activeCount() > 1:
                time.sleep(1)
        except exceptions.KeyboardInterrupt:
            return 0
        except:
            raise

        ret_metrics = []
        if not sendQueue.empty():
            self.send_metrics(ret_metrics)
        return ret_metrics

    def handle_region(self, region, statistic):
        try:
            self.log_info("starting region " + region + "," + statistic + "\n")
            region_conn = cloudwatch_connect_to_region(region)
            metrics = cloudwatch_list_metrics(region_conn)
            interval = self.get_config('interval', None)
            for metric in metrics:
                self.cloudwatch_query_metric(region, metric, statistic, interval)
        except boto.exception.BotoServerError, e:
            self.log_info("finished region " + region + "," + statistic + "\n")
            pass
        except exceptions.KeyboardInterrupt:
            return 0
        except:
            self.log_error("failed region " + region + "," + statistic + "\n")
            raise
        else:
            self.log_info("finished region " + region + "," + statistic + "\n")

    def send_metrics(self, ret_metrics):
        self.log_info("Processing sendQueue \n")
        datapoints = {}
        try:
            while not sendQueue.empty():
                item = sendQueue.get()
                timestamp = item['timestamp']
                output = item['output']
                if not timestamp in datapoints:
                    datapoints[timestamp] = []
                datapoints[timestamp].append(output)
                sendQueue.task_done()
            sys.stderr.write("Queue Emptied, sorting output")
            for outputs in sorted(datapoints.iteritems(), key=lambda x: x[1]):
                for output in outputs:
                    for t in output:
                        ret_metrics.append(t)
        except exceptions.KeyboardInterrupt:
            return 0

    def validate_config(self):
        access_key, secret_access_key = get_accesskey_secretkey()
        if access_key == '<access_key_id>' or secret_access_key == '<secret_access_key>':
            self.log_error("Cloudwatch Collector is not configured\n")
            raise

    def cloudwatch_query_metric(self, region, metric, statistic, interval):
        end = datetime.datetime.utcnow()
        start = end - datetime.timedelta(seconds=interval)
        dimensions = metric.dimensions
        if len(dimensions) > 0:
            metric_name, tags = build_tag_list(metric.name.lower(), region, dimensions)
            datapoints = metric.query(start, end, statistic)
            if len(datapoints) > 0:
                for datapoint in datapoints:
                    timestamp = format_timestamp(str(datapoint['Timestamp']))
                    value = int(datapoint[statistic])
                    metric_full = " %s.%s.%s" % (metric.namespace.lower().replace('/', '.'), metric_name, statistic.lower())
                    output = "%s.%s.%s %s %s %s" % (
                    metric.namespace.lower().replace('/', '.'), metric_name, statistic.lower(), str(timestamp), str(value),
                    tags)
                    if self.validate_line_parses(output):
                        sendQueue.put({'timestamp': timestamp, 'output': output})

    # Uses the same code as tcollector here
    def validate_line_parses(self, line):
        parsed = re.match('^([-_./a-zA-Z0-9]+)\s+'  # Metric name.
                          '(\d+)\s+'  # Timestamp.
                          '(\S+?)'  # Value (int or float).
                          '((?:\s+[-_./a-zA-Z0-9]+=[-_./a-zA-Z0-9]+)*)$',  # Tags
                          line)
        if parsed is None:
            self.log_error("invalid data: %s \n", line)
            return False
        metric, timestamp, value, tags = parsed.groups()
        return True

if __name__ == "__main__":
    aws_stats = AwsCloudWatchStats(None, None)
    aws_stats()
