#!/usr/bin/env python

def get_config():

    config = {
        'collection_interval': 1,   # Seconds, how often to collect metric data
        'collection_filter': 'G2203NHGD5ANYhm'    # Filter to choose disks, .* will take all disks
    }

    return config
