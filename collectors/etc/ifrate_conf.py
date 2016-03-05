#!/usr/bin/env python

def get_config():

    config = {
        'collection_interval': 15,     # Seconds, how often to collect metric data
        'interfaces': ['ix0','ix1']    # Interfaces to monitor
    }

    return config
