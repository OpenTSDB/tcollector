#!/usr/bin/env python
import os
import sys
import time
import datetime
import re
import json
from collections import OrderedDict
import threading
from time import mktime
from collectors.lib import utils
from collectors.etc import aws_cloudwatch_conf

try:
    import boto.ec2
    from boto.ec2.cloudwatch import CloudWatchConnection
    from boto.ec2.cloudwatch import regions
except ImportError:
    exit(13)

try:
    from queue import Queue
except ImportError:
    from Queue import Queue

ILLEGAL_CHARS_REGEX = re.compile('[^a-zA-Z0-9\- _./]')

path = os.path.dirname(os.path.realpath(__file__))
COLLECTION_INTERVAL = int(path.split('/')[-1])

if COLLECTION_INTERVAL == 0:
  sys.stderr.write("AWS Cloudwatch Stats is not a long running collector\n")
  sys.exit(13)

if COLLECTION_INTERVAL < 60:
  sys.stderr.write("AWS Cloudwatch Stats is an heavy collector and should not be run more than once per minute.\n")
  sys.exit(13)

STATISTICS = frozenset([
                        'Minimum',
                        'Maximum',
                        'Average',
                        'Sum',
                        'SampleCount'
                       ])

sendQueue = Queue()

def validate_config():
    access_key, secret_access_key = aws_cloudwatch_conf.get_accesskey_secretkey()
    if access_key == '<access_key_id>' or secret_access_key == '<secret_access_key>':
      sys.stderr.write("Cloudwatch Collector is not configured\n")
      sys.exit(13)
    if not aws_cloudwatch_conf.enabled:
      sys.stderr.write("Cloudwatch Collector is not enabled\n")
      sys.exit(13)

def cloudwatch_connect_to_region(region):
    access_key, secret_access_key = aws_cloudwatch_conf.get_accesskey_secretkey()
    try:
        conn =  boto.ec2.cloudwatch.connect_to_region(region, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
    except:
        print("Unexpected error:", sys.exc_info()[0])
    else:
        return conn

def cloudwatch_list_metrics(conn):
    return conn.list_metrics()

def cloudwatch_query_metric(region, metric, statistic):
    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(seconds=COLLECTION_INTERVAL)
    dimensions = metric.dimensions
    if len(dimensions) > 0:
        metric_name, tags = build_tag_list(metric.name.lower(), region, dimensions)
        datapoints = metric.query(start, end, statistic)
        if len(datapoints) > 0:
            for datapoint in datapoints:
                timestamp = format_timestamp(str(datapoint['Timestamp']))
                value =  int(datapoint[statistic])
                metric_full = " %s.%s.%s" % (metric.namespace.lower().replace('/','.'), metric_name, statistic.lower())
                output = "%s.%s.%s %s %s %s" % (metric.namespace.lower().replace('/','.'), metric_name, statistic.lower(), str(timestamp), str(value), tags)
                if validate_line_parses(output):
                    sendQueue.put({'timestamp': timestamp, 'output': output})

def format_timestamp(ts):
    st = time.strptime(ts, "%Y-%m-%d %H:%M:%S")
    dt = datetime.datetime.fromtimestamp(mktime(st))
    return dt.strftime("%s")

def build_tag_list(metric_name, region, dimensions):
    tags = "region=" + str(region)

    for tagk,tagv in dimensions.items():
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
    access_key, secret_access_key = aws_cloudwatch_conf.get_accesskey_secretkey()
    return boto.ec2.connect_to_region(region, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)

def ec2_list_regions():
    ec2_regions = []
    for i in boto.ec2.cloudwatch.regions():
        ec2_regions.append(str(i.name))
    return ec2_regions

def handle_region(region, statistic):
    try:
#        sys.stderr.write("starting region " + region + "," + statistic + "\n")
        region_conn = cloudwatch_connect_to_region(region)
        metrics = cloudwatch_list_metrics(region_conn)
        for metric in metrics:
            cloudwatch_query_metric(region, metric, statistic)
    except boto.exception.BotoServerError as e:
 #       sys.stderr.write("finished region " + region + "," + statistic + "\n")
        pass
    except KeyboardInterrupt:
        return 0
    except:
        sys.stderr.write("failed region " + region + "," + statistic + "\n")
        raise
#    else:
#        sys.stderr.write("finished region " + region + "," + statistic + "\n")
        return 1

def send_metrics():
    sys.stderr.write("Processing sendQueue \n")
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
        for outputs in sorted(datapoints.items(), key=lambda x: x[1]):
            for output in outputs:
                for t in output:
                    print(t)
    except KeyboardInterrupt:
        return 0

# Uses the same code as tcollector here
def validate_line_parses(line):
    parsed = re.match('^([-_./a-zA-Z0-9]+)\s+' # Metric name.
          '(\d+)\s+'               # Timestamp.
          '(\S+?)'                 # Value (int or float).
          '((?:\s+[-_./a-zA-Z0-9]+=[-_./a-zA-Z0-9]+)*)$', # Tags
          line)
    if parsed is None:
        sys.stderr.write("invalid data: %s \n" % (line))
        return False
    metric, timestamp, value, tags = parsed.groups()
    return True

def main():
    try:
        utils.drop_privileges()
        validate_config()
        regions = ec2_list_regions()
        for reg in regions:
            for statistic in STATISTICS:
                t = threading.Thread(target=handle_region, kwargs={"region":reg, "statistic":statistic})
                t.start()
        while threading.activeCount() > 1:
            time.sleep(1)
    except KeyboardInterrupt:
        return 0
    except:
        raise
    if not sendQueue.empty():
        send_metrics()

if __name__ == "__main__":
    sys.exit(main())
