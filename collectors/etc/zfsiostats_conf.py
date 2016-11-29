#!/usr/bin/env python

def get_config():

    config = {
        'collection_interval': 15,             # Seconds, how often to collect metric data
        'report_capacity_every_x_times': 20,   # Avoid reporting capacity info too frequently, 0 disables capacity reporting
        'report_disks_in_vdevs': False         # Avoid reporting statistics for disks which are part of vdevs
    }

    return config
