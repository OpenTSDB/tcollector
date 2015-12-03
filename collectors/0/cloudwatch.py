#!/usr/bin/python

# this must go first; helps with python 3.x compatibility
from __future__ import print_function

# this must go second
from gevent import monkey; monkey.patch_all()

import datetime
import itertools
import os
import sys
import time

import boto.utils
import boto.ec2
import boto.ec2.elb
import boto.ec2.cloudwatch
from boto.ec2.cloudwatch import regions
from boto.ec2.cloudwatch import CloudWatchConnection
from boto.ec2.connection import EC2Connection
from boto.ec2.elb import ELBConnection
import gevent
from gevent.pool import Pool

REGION_LIST = os.environ.get('CLOUDWATCH_REGION_LIST', 'us-east-1').split(',')

ELB_METRICS = {
        "RequestCount": "Sum",
        "HTTPCode_ELB_5XX": "Sum",
        "HTTPCode_ELB_4XX": "Sum",
        "HTTPCode_Backend_2XX": "Sum",
        "HTTPCode_Backend_3XX": "Sum",
        "HTTPCode_Backend_4XX": "Sum",
        "HTTPCode_Backend_5XX": "Sum"
    }

AZS = ['us-east-1b', 'us-east-1c', 'us-east-1d', 'us-east-1e']

def emit(name, value, tags=None, ts=None):
    # format is
    # [metric] [timestamp] [value] [tags]

    if ts:
        epoch = int(ts.strftime("%s"))
    else:
        epoch = int(time.time())

    sys.stdout.write("put %s %d %s" % (name, epoch, value))

    # now write the tags
    if tags:
        for key, value in tags.iteritems():
            sys.stdout.write(" %s=%s" % (key, value))

    sys.stdout.write("\n")

def emit_metric_for_loadbalancer(cw, metric, lb_name, region_name, az):
    # 1 minute periods come at an extra charge
    # 5 minute period available for free
    results = cw.get_metric_statistics(
        300, # period = 5 min
        datetime.datetime.now() - datetime.timedelta(seconds=600), # 10 mins ago
        datetime.datetime.now() - datetime.timedelta(seconds=300), # 5 mins ago
        metric['name'],
        'AWS/ELB',
        metric['stat'],
        dimensions={'LoadBalancerName': lb_name,
                    'AvailabilityZone': az})

    for result in results:
        name = 'aws.ec2.elb.%s' % (metric['name'])
        value = result[metric['stat']]
        tags = {'elb_name': lb_name,
                'region': region_name,
                'placement': az}
        emit(name, value, tags=tags, ts=result['Timestamp'])


# work in progress
def emit_metric_for_billing(cw, item_name):
    results = cw.get_metric_statistics(
        3600, # period = 1 hour
        datetime.datetime.now() - datetime.timedelta(hours=1),
        datetime.datetime.now(),
        'EstimatedCharges',
        'AWS/Billing',
        "Maximum",
        dimensions={'ServiceName': item_name, 'Currency': 'USD'})
    print(results, file=sys.stderr)


def emit_elb_metrics(pool, cw, region_name):
    try:
        elb = boto.ec2.elb.connect_to_region(region_name=region_name)
        lbs = [lb.name for lb in elb.get_all_load_balancers()]
        for name in lbs:
            for az in AZS:
                for metric_name, stat in ELB_METRICS.iteritems():
                    metric = {'name': metric_name, 'stat': stat}
                    pool.spawn(emit_metric_for_loadbalancer, cw, metric, name, region_name, az)
    except Exception as e:
        print(e, file=sys.stderr)


def emit_billing_metrics(pool, cw):
    item_list = ['AmazonCloudFront', 'AmazonDynamoDB', 'AmazonEC2', 'AmazonRDS', 'AmazonS3', 'AmazonSNS', 'AWSDataTransfer']
    for item_name in item_list:
        pool.spawn(emit_metric_for_billing, cw, item_name)

def main():
    pool = Pool(10)
    for region_name in REGION_LIST:
        cw = boto.ec2.cloudwatch.connect_to_region(region_name=region_name)
        # emit_billing_metrics(pool, cw)
        emit_elb_metrics(pool, cw, region_name)

    pool.join()

if __name__ == '__main__':
    main()
