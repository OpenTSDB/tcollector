#!/usr/bin/python
import os
import sys
import time
import datetime
import json
from time import mktime
from collectors.etc import awsconf

try:
    import boto.ec2
    from boto.ec2.cloudwatch import CloudWatchConnection
    from boto.ec2.cloudwatch import regions
except ImportError:
    exit(13)

COLLECTION_INTERVAL = 300

STATISTICS = frozenset([
                        'Minimum',
                        'Maximum',
                        'Average',
                        'Sum',
                        'SampleCount'
                       ])

NAMESPACES = frozenset([
                        'AWS/AutoScaling',
                        'AWS/Billing',
                        'AWS/CloudFront',
                        'AWS/CloudSearch',
                        'AWS/Events',
                        'AWS/DynamoDB',
                        'AWS/ECS',
                        'AWS/ElastiCache',
                        'AWS/EBS',
                        'AWS/EC2',
                        'AWS/ELB',
                        'AWS/ElasticMapReduce',
                        'AWS/ES',
                        'AWS/Kinesis',
                        'AWS/Lambda',
                        'AWS/ML',
                        'AWS/OpsWorks',
                        'AWS/Redshift',
                        'AWS/RDS',
                        'AWS/Route53',
                        'AWS/SNS',
                        'AWS/SQS',
                        'AWS/S3',
                        'AWS/SWF',
                        'AWS/StorageGateway',
                        'AWS/WAF',
                        'AWS/WorkSpaces'
                       ])

def cloudwatch_connect_to_region(region):
    access_key, secret_access_key = awsconf.get_accesskey_secretkey()
    try:
        conn =  boto.ec2.cloudwatch.connect_to_region(region, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
    except:
        print "Unexpected error:", sys.exc_info()[0]
    else:
        return conn

def cloudwatch_list_metrics(conn):
    return conn.list_metrics()

def cloudwatch_query_metric(metric, statistic):
    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(seconds=COLLECTION_INTERVAL)
    tags = metric.dimensions
    datapoints = metric.query(start, end, statistic)
    if len(datapoints) > 0:
        for datapoint in datapoints:
            timestamp = str(datapoint['Timestamp'])
            st = time.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            dt = datetime.datetime.fromtimestamp(mktime(st))
            timestamp = dt.strftime("%s")
            value = datapoint[statistic]
            taglist = ""

            for tagk,tagv in tags.iteritems():
                tagkey = str(tagk).lower()
                tagval = str(tagv[0]).lower()
                taglist = "%s %s=%s" % (taglist, tagkey, tagval)

            metric_name = metric.name.lower()

            if metric_name == 'networkout':
                metric_name = 'network'
                taglist = "%s %s=%s" % (taglist, 'direction', 'out')

            if metric_name == 'networkin':
                metric_name = 'network'
                taglist = "%s %s=%s" % (taglist, 'direction', 'in')

            print "%s.%s.%s %s %s %s" % (metric.namespace.lower().replace('/','.'), metric_name, statistic.lower(), str(timestamp), str(value), taglist)

def ec2_connect_to_region(region):
    access_key, secret_access_key = awsconf.get_accesskey_secretkey()
    return boto.ec2.connect_to_region(region, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)

def ec2_list_regions():
    ec2_regions = []
    for i in boto.ec2.cloudwatch.regions():
        ec2_regions.append(str(i.name))
    return ec2_regions

def main():
    while True:
        regions = ec2_list_regions()
        for reg in regions:
            region_conn = cloudwatch_connect_to_region(reg)
            metrics = cloudwatch_list_metrics(region_conn)
            for metric in metrics:
                for statistic in STATISTICS:
                    cloudwatch_query_metric(metric, statistic)
        time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    sys.exit(main())
