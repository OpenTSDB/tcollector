#!/usr/bin/env python

def get_config():

    config = {
        'collection_interval': 15,     # Seconds, how often to collect metric data
        'interfaces': ['ix0','ix1'],   # Interfaces to monitor
        'report_packets': False,       # Report packets/s in addition to bytes/s
        'merge_err_in_out': True       # Merge err and drp counters into (in+out) counters
    }

    return config
