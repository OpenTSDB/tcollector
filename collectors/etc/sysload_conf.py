#!/usr/bin/env python

def get_config():

    config = {
        'collection_interval': 15,    # Seconds, how often to collect metric data
        'collect_every_cpu': True     # 1 will collect statistics for every CPU, 0 for the "ALL" CPU
    }

    return config
