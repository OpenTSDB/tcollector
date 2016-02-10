#!/usr/bin/env python

 def get_config():

     config = {
     	'prefix': 'jvm',                   # the full metric name will be prefix.gc.g1.*
     	'log_dir': '/var/log/gc',          # GC log director, e.g. /var/logs/g1gc/
     	'log_name_pattern': '*gc*.log',    # python glob pattern that will be used to find latest gc log
        'collection_interval': 10          # Seconds, how often to collect metric data
    }

    return config
