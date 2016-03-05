#!/usr/bin/env python

def get_config():

    config = {
        'collection_interval': 15,           # Seconds, how often to collect metric data
	'interfaces': ['mlxen0','mlxen1']    # Interfaces to monitor
    }

    return config
