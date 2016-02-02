#!/usr/bin/env python

def enabled():
  return True

def get_settings():
  """Flume Connection Details"""
  return {
    'flume_host': "localhost",     # Flume Host to Connect to
    'flume_port': 41414,            # Flume Port to connect to
    'default_timeout': 10.0         # seconds
  }
