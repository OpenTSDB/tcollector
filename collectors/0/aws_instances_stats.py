#!/usr/bin/python

import os
import sys
import time
import datetime
from time import mktime
from collectors.etc import awsconf

try:
    import boto.ec2
    from boto.ec2.cloudwatch import CloudWatchConnection
    from boto.ec2.connection import EC2Connection
    from boto.ec2.cloudwatch import regions
except ImportError:
    boto = None

SCAN_INTERVAL = 300
COLLECTION_INTERVAL = 15

STATISTICS = frozenset([
                        'Minimum', 
                        'Maximum', 
                        'Average',
                       ])

def err(e):
    print >> sys.stderr, e

def cloudwatch_connect():
    access_key, secret_access_key = awsconf.get_accesskey_secretkey()
    return CloudWatchConnection(aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)

def cloudwatch_connect_to_region(region):
    access_key, secret_access_key = awsconf.get_accesskey_secretkey()
    return boto.ec2.cloudwatch.connect_to_region(region, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)

def cloudwatch_list_metrics(conn):
    cloudwatch_metrics = []
    for i in conn.list_metrics():
        m = str(i).split(":")[1]
        if m in cloudwatch_metrics:
            continue
        else:
            cloudwatch_metrics.append(m)
    return cloudwatch_metrics

def ec2_connect_to_region(region):
    access_key, secret_access_key = awsconf.get_accesskey_secretkey()
    return boto.ec2.connect_to_region(region, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)

def ec2_list_regions():
    ec2_regions = []
    for i in regions():
        ec2_regions.append(str(i.name))
    return ec2_regions
 
def ec2_list_instances(conn):
    instances = {}
    instances_regions = {}
    reservations = conn.get_all_reservations()
    for r in reservations:
        for inst in r.instances:
            if str(inst.state) == "running":
                instances[str(inst.id)] = str(inst.placement)
                instances_regions[str(inst.id)] = str(inst.region.name)
    return (instances,instances_regions)

def print_stat(metric, ts, value, tags=""):
    if value is not None:
        print "aws.%s %s %s %s" % (metric.lower(), ts, value, tags)

def main():
    instances = {}
    instances_regions = {}
    cloudwatch_conn = {}
    last_scan = time.time()

    # List all regions
    regions = ec2_list_regions()

    for reg in regions:
        # Request for us-gov-west-1 AWS Region (esp. for U.S. Govt Agencies) throws invalid permission  
        if reg != "us-gov-west-1":
            ec2_conn = ec2_connect_to_region(reg)
            inst, inst_reg = ec2_list_instances(ec2_conn)
            instances.update(inst)
            instances_regions.update(inst_reg)

    # Connect to each region's cloudwatch to collect metrics of that region
    for reg in regions:
        # Request for us-gov-west-1 AWS Region (esp. for U.S. Govt Agencies) throws invalid permission
        if reg != "us-gov-west-1":
            cloudwatch_conn[reg] = cloudwatch_connect_to_region(reg) 
   
    # Create CloudWatchConnection
    try:
        cw_conn = cloudwatch_connect()
    except:
        err("Can't connect to CloudWatch with these credentials")
        return 13   # Ask tcollector not to respawn us
    
    cw_metrics = cloudwatch_list_metrics(cw_conn)

    while True:
        ts = time.time()

        # We haven't looked for any new ec2 instance recently, let's do that
        if ts - last_scan > SCAN_INTERVAL:
            for reg in regions:
                if reg != "us-gov-west-1":
                    ec2_conn = ec2_connect_to_region(reg)
                    inst, inst_reg = ec2_list_instances(ec2_conn)
                    instances.update(inst)
                    instances_regions.update(inst_reg)
                    last_scan = ts

        if not instances:
            return 13   # Ask tcollector not to respawn us
        
        # Start Time
        start = datetime.datetime.utcnow() - datetime.timedelta(seconds=120)

        # End Time
        end = datetime.datetime.utcnow()

        # Iterate for each instance and collect datapoints
        for instance,placement in instances.iteritems():
            for metric in cw_metrics:
                for statistic in STATISTICS: 
                    stats = cloudwatch_conn[instances_regions[instance]].get_metric_statistics(60, start, end, metric, 'AWS/EC2', statistic, dimensions={'InstanceId':[instance]})
                    if stats:
                        timestamp = str(stats[0]['Timestamp'])
                        st = time.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                        dt = datetime.datetime.fromtimestamp(mktime(st))
                        timestamp = dt.strftime("%s")
                        value = stats[0][statistic]
                        tags = "instance-id=%s statistic=%s placement=%s" % (instance, statistic, placement)
                        print_stat(metric, timestamp, value, tags)
                       
        time.sleep(COLLECTION_INTERVAL)
                                  
if __name__ == "__main__":
    sys.exit(main()) 
