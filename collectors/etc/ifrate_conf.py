#!/usr/bin/env python


def get_config():

    config = {"collection_interval": 15, "interfaces": ["ix0", "ix1"], "report_packets": False, "merge_err_in_out": True}  # Seconds, how often to collect metric data  # Interfaces to monitor  # Report packets/s in addition to bytes/s  # Merge err and drp counters into (in+out) counters

    return config
