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
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    utils.err("Could not import boto3\n")
    exit(13)

try:
    # noinspection PyCompatibility
    from queue import Queue
except ImportError:
    # noinspection PyUnresolvedReferences
    from Queue import Queue

ILLEGAL_CHARS_REGEX = re.compile('[^a-zA-Z0-9\- _./]')

path = os.path.dirname(os.path.realpath(__file__))
COLLECTION_INTERVAL = int(path.split('/')[-1])

if COLLECTION_INTERVAL == 0:
    utils.err("AWS Cloudwatch Stats is not a long running collector\n")
    sys.exit(13)

if COLLECTION_INTERVAL < 60:
    utils.err("AWS Cloudwatch Stats is an heavy collector and should not be run more than once per minute.\n")
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
    aws_profile = aws_cloudwatch_conf.get_aws_profile()
    access_key, secret_access_key = aws_cloudwatch_conf.get_accesskey_secretkey()
    if (access_key == '<access_key_id>' or secret_access_key == '<secret_access_key>') and aws_profile is None:
        utils.err("Cloudwatch Collector is not configured\n")
        sys.exit(13)
    if not aws_cloudwatch_conf.enabled:
        utils.err("Cloudwatch Collector is not enabled\n")
        sys.exit(13)


def cloudwatch_init_boto_client(region):
    client_name = 'cloudwatch'
    aws_profile = aws_cloudwatch_conf.get_aws_profile()
    access_key, secret_access_key = aws_cloudwatch_conf.get_accesskey_secretkey()
    if (((access_key is not None) and (access_key != '<access_key_id>')) and (
            #sys.stderr.write("using access keys\n")
                (secret_access_key is not None) and (secret_access_key != '<secret_access_key>'))):
        boto_client = boto3.client(
            client_name,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key,
            region_name=region
        )
    elif aws_profile is not None:
        #sys.stderr.write("Using aws_profile: %s\n" % (aws_profile))
        session = boto3.session.Session(profile_name=aws_profile)
        boto_client = session.client(client_name, region_name=region)
    else:
        #sys.stderr.write("connecting with no auth info, good luck")
        boto_client = boto3.client(client_name, region_name=region)

    return boto_client


def cloudwatch_list_metrics(cloudwatch):
    try:
        metrics = []
        # List metrics through the pagination interface
        paginator = cloudwatch.get_paginator('list_metrics')
        for response in paginator.paginate():
            metrics.append(response['Metrics'])
        return metrics[0]
    except ClientError as exc:
        if exc.response['Error']['Code'] == 'InvalidClientTokenId':
            utils.err('Invalid token while retrieving metrics')
            sys.exit(1)
        else:
            utils.err('Client Error while retrieving metric list: [%s] %s' % (
                exc.response['Error']['Code'], exc.response['Error']['Message']))
            return []
    except:
        print("Unexpected error: %s %s %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]))
        return []


def cloudwatch_query_metric(cloudwatch, region, metric):
    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(seconds=COLLECTION_INTERVAL)
    global STATISTICS
    # TODO: statistics no longer need to be one at at time so refactor that
    response = cloudwatch.get_metric_statistics(
        Namespace=metric["Namespace"],
        MetricName=metric["MetricName"],
        Dimensions=metric["Dimensions"],
        StartTime=start,
        EndTime=end,
        Period=300,
        Statistics=list(STATISTICS),
        Unit='Count'
    )

    for datapoint in response['Datapoints']:
        for statistic in STATISTICS:
            timestamp = format_timestamp(str(datapoint['Timestamp']))
            value = int(datapoint[statistic])
            metric_name, tags = build_tag_list(metric['MetricName'].lower(), region, metric['Dimensions'])
            namespace = metric["Namespace"].lower().replace('/', '.')
            output = "%s.%s.%s %s %s %s" % (
                namespace, metric_name, statistic.lower(), str(timestamp),
                str(value),
                tags)
            #sys.stderr.write('output: %s\n' % (output))
            if validate_line_parses(output):
                sendQueue.put({'timestamp': timestamp, 'output': output})
            else:
                utils.err("Invalid Line: %s" % output)


def format_timestamp(ts):
    st = time.strptime(ts, "%Y-%m-%d %H:%M:%S%z")
    dt = datetime.datetime.fromtimestamp(mktime(st))
    return dt.strftime("%s")


def build_tag_list(metric_name, region, dimensions):
    tags = "region=" + str(region)

    for dimension in dimensions:
        tagkey = str(dimension["Name"])
        tagval = str(dimension["Value"])
        tags += " %s=%s" % (tagkey, tagval)

    if metric_name == 'networkout':
        metric_name = 'network'
        tags += " %s=%s" % ('direction', 'out')

    if metric_name == 'networkin':
        metric_name = 'network'
        tags += " %s=%s" % ('direction', 'in')

    return metric_name.strip().lower(), tags.strip().lower()


def handle_region(region):
    try:
        #sys.stderr.write("handling region %s\n" % (region))
        cloudwatch = cloudwatch_init_boto_client(region)
        metrics = cloudwatch_list_metrics(cloudwatch)
        if metrics is not None:
            for metric in metrics:
                #sys.stderr.write("handling metric %s %s:%s\n" % (region, metric["MetricName"], metric["Namespace"]))
                cloudwatch_query_metric(cloudwatch, region, metric)
        else:
            utils.err('No metrics retrieved for %s' % (region))
    except ClientError as exc:
        utils.err('Client Error while handling region %s: [%s] %s' % (
            region, exc.response['Error']['Code'], exc.response['Error']['Message']))
        pass
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        utils.err("failed region %s: %s\n" % (region, exc))
        raise


def send_metrics():
    #sys.stderr.write("Processing sendQueue \n")
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
        #sys.stderr.write("Queue Emptied, sorting output\n")
        for outputs in sorted(datapoints.items(), key=lambda x: x[1]):
            for output in outputs:
                for t in output:
                    print(t)
    except KeyboardInterrupt:
        return 0


# Uses the same code as tcollector here
def validate_line_parses(line):
    parsed = re.match('^([-_./a-zA-Z0-9]+)\s+'  # Metric name.
                      '(\d+\.?\d+)\s+'  # Timestamp.
                      '(\S+?)'  # Value (int or float).
                      '((?:\s+[-_./a-zA-Z0-9]+=[-_./a-zA-Z0-9]+)*)$',  # Tags
                      line)
    if parsed is None:
        utils.err("invalid data: %s \n" % (line))
        return False
    return True


def main():
    try:
        utils.drop_privileges()
        validate_config()
        regions = boto3.session.Session().get_available_regions('cloudwatch')

        for reg in regions:
            t = threading.Thread(target=handle_region, kwargs={"region": reg})
            t.start()
        while threading.activeCount() > 1:
            #sys.stderr.write('sleeping while threads are running\n')
            time.sleep(1)
    except KeyboardInterrupt:
        return 0
    except:
        raise
    if not sendQueue.empty():
        send_metrics()


if __name__ == "__main__":
    sys.exit(main())
