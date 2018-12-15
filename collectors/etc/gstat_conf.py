#!/usr/bin/env python


def get_config():

    config = {"collection_interval": 15, "collection_filter": ".*"}  # Seconds, how often to collect metric data  # Filter to choose disks, .* will take all disks

    return config
