#!/usr/bin/env python

def get_config():

    config = {
        'collection_interval': 15,    # Seconds, how often to collect metric data
        'collect_every_cpu': True     # True will collect statistics for every CPU, False for the "ALL" CPU
    }

    return config
