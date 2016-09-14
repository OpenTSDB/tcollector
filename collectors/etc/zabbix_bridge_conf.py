#!/usr/bin/env python

def get_settings():
  """MySQL replication credentials."""
  # A user with "GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT"
  return {
    # DB credentials (see pymysql connection info).
    'mysql': {
        'host': '127.0.0.1',
        'port': 3306,
        'user': '',
        'passwd': '',
        'db': 'zabbix'
    },
    'slaveid': 3,                       # Slave identifier, it should be unique.
    'disallow': '[^a-zA-Z0-9\-_\.]',    # Regex of characters to replace with _.
    'internal_metric_interval': 30,     # Internal metric interval drift and error counts.
    'dbrefresh': 10,                    # Number of key misses before DB reload from file occurs
    'sqlitedb': '/tmp/zabbix_bridge.db' # SQLite DB to cache items from Zabbix DB.
  }
