#!/usr/bin/env python

def enabled():
    return False


def get_settings():
    """Prometheus Exporter Targets Connection Details"""
    return {
        'targets': [{
            'target_service': 'hazelcast',
            'target_instance': 'hazelcast01.consul',
            'target_host': 'localhost',
            'target_port': 8080,
            'collection_interval': 15
        }
        ],
        'collection_interval': 60,  # seconds, How often to collect metric data
        'default_timeout': 10.0,  # seconds
        'include_service_tags': True
    }
