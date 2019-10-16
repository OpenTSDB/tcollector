#!/usr/bin/env python
def get_settings():
    return {
        'log_dir': '/var/log/impalad/lineage/',
        'log_prefix': 'impala_lineage_log',
        'refresh_interval': 180
    }
