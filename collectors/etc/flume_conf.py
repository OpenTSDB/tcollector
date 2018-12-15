#!/usr/bin/env python


def enabled():
    return False


def get_settings():
    """Flume Connection Details"""
    return {"flume_host": "localhost", "flume_port": 34545, "collection_interval": 15, "default_timeout": 10.0}  # Flume Host to Connect to  # Flume Port to connect to  # seconds, How often to collect metric data  # seconds
