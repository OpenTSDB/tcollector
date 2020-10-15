#!/usr/bin/env python

def enabled():
  return True

def get_settings():
  """Prometheus Exporter Targets Connection Details"""
  return {
    'targets': [{
        'target_name': 'hazelcast',
        'target_host': 'localhost',
        'target_port': 8080,
      },
      {
        'target_host': 'localhost',
        'target_port': 8080,
      },
      {
        'target_service': 'hazelcast',
        'target_instance': 'hazelcast01.consul',
        'target_host': 'localhost',
        'target_port': 8080,
        'collection_interval': 5
      }
    ],
    'collection_interval': 15,      # seconds, How often to collect metric data
    'default_timeout': 10.0,         # seconds
    'include_service_tags': False
  }
