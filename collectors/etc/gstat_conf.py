#!/usr/bin/env python

def get_config():

    config = {
        'collection_interval': 15,   # Seconds, how often to collect metric data
        'collection_filter': '.*'    # Filter to choose disks, .* will take all disks
    }

    return config
