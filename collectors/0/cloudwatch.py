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
        "HealthyHostCount": "Minimum",
        "UnHealthyHostCount": "Maximum",
        "HTTPCode_ELB_5XX": "Sum",
        "HTTPCode_ELB_4XX": "Sum",
        "HTTPCode_Backend_2XX": "Sum",
        "HTTPCode_Backend_3XX": "Sum",
        "HTTPCode_Backend_4XX": "Sum",
        "HTTPCode_Backend_5XX": "Sum",
        "Latency": "Average",
    }


EC2_METRICS = [
    {
        'name': "CPUUtilization",
        'unit': "Percent",
        'stat': "Average"
    },
    {
        'name': "DiskReadBytes",
        'unit': "Bytes",
        'stat': "Average"
    },
    {
        'name': "DiskReadOps",
        'unit': "Count",
        'stat': "Average"
    },
    {
        'name': "DiskWriteBytes",
        'unit': "Bytes",
        'stat': "Average"
    },
    {
        'name': "DiskWriteOps",
        'unit': "Count",
        'stat': "Average"
    },
    {
        'name': "NetworkIn",
        'unit': "Bytes",
        'stat': "Average"
    },
    {
        'name': "NetworkOut",
        'unit': "Bytes",
        'stat': "Average"
    }
]


def emit(name, value, tags=None, ts=None):
    # format is
    # [metric] [timestamp] [value] [tags]

    if ts:
        epoch = int(ts.strftime("%s"))
    else:
        epoch = int(time.time())

    sys.stdout.write("%s %d %s" % (name, epoch, value))

    # now write the tags
    if tags:
        for key, value in tags.iteritems():
            sys.stdout.write(" %s=%s" % (key, value))

    sys.stdout.write("\n")


def emit_metric_for_instance(cw, metric, instance, region_name):
    # 1 minute periods come at an extra charge
    # 5 minute period available for free
    results = cw.get_metric_statistics(
        300, # period = 5 min
        datetime.datetime.now() - datetime.timedelta(seconds=600), # 10 mins ago
        datetime.datetime.now(),
        metric['name'],
        'AWS/EC2',
        metric['stat'],
        dimensions={'InstanceId': instance.id},
        unit=metric['unit'])

    for result in results:
        name = 'aws.ec2.instance.%s' % (metric['name'])
        value = result[metric['stat']]
        tags = {'instance-id': instance.id,
                'placement': instance.placement,
                'instance-type': instance.instance_type,
                'region': region_name}
        if 'Name' in instance.tags:
            tags['host'] = instance.tags['Name']
        emit(name, value, tags=tags, ts=result['Timestamp'])


def emit_metric_for_loadbalancer(cw, metric, lb_name, region_name):
    # 1 minute periods come at an extra charge
    # 5 minute period available for free
    results = cw.get_metric_statistics(
        300, # period = 5 min
        datetime.datetime.now() - datetime.timedelta(seconds=600), # 10 mins ago
        datetime.datetime.now(),
        metric['name'],
        'AWS/ELB',
        metric['stat'],
        dimensions={'LoadBalancerName': lb_name})

    for result in results:
        name = 'aws.ec2.elb.%s' % (metric['name'])
        value = result[metric['stat']]
        tags = {'elb_name': lb_name,
                'region': region_name}
        emit(name, value, tags=tags, ts=result['Timestamp'])


# work in progress
def emit_metric_for_billing(cw, item_name):
    results = cw.get_metric_statistics(
        3600, # period = 1 hour
        datetime.datetime.now() - datetime.timedelta(hours=2),
        datetime.datetime.now(),
        'EstimatedCharges',
        'AWS/Billing',
        "Maximum",
        dimensions={'ServiceName': item_name, 'Currency': 'USD'})
    print(results, file=sys.stderr)


def emit_ec2_metrics(pool, cw, region_name):
    try:
        ec2 = boto.ec2.connect_to_region(region_name=region_name)
        chain = itertools.chain.from_iterable
        all_instances = chain([res.instances for res in ec2.get_all_instances()])
        for instance in all_instances:
            for metric in EC2_METRICS:
                pool.spawn(emit_metric_for_instance, cw, metric, instance, region_name)
    except Exception as e:
        print(e, file=sys.stderr)


def emit_elb_metrics(pool, cw, region_name):
    try:
        elb = boto.ec2.elb.connect_to_region(region_name=region_name)
        lbs = [lb.name for lb in elb.get_all_load_balancers()]
        for name in lbs:
            for metric_name in ELB_METRICS:
                metric = {'name': metric_name, 'stat': ELB_METRICS[metric_name]}
                pool.spawn(emit_metric_for_loadbalancer, cw, metric, name, region_name)
    except Exception as e:
        print(e, file=sys.stderr)


def emit_billing_metrics(pool, cw):
    item_list = ['AmazonCloudFront', 'AmazonDynamoDB', 'AmazonEC2', 'AmazonRDS', 'AmazonS3', 'AmazonSNS', 'AWSDataTransfer']
    for item_name in item_list:
        pool.spawn(emit_metric_for_billing, cw, item_name)


def main():

    while True:
        pool = Pool(10)
        for region_name in REGION_LIST:
            cw = boto.ec2.cloudwatch.connect_to_region(region_name=region_name)
            # emit_billing_metrics(pool, cw)
            emit_ec2_metrics(pool, cw, region_name)
            emit_elb_metrics(pool, cw, region_name)

        pool.join()

        print("Sleeping...", file=sys.stderr)
        gevent.sleep(300)  # sleep for 5 minutes

if __name__ == '__main__':
    main()

