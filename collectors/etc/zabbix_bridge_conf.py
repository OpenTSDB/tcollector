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
    'slaveid': 3,                    # Slave identifier, it should be unique.
    'disallow': '[^a-zA-Z0-9\-_\.]', # Regex of characters to replace with _.
    'gethostmap_interval': 300       # How often to reload itemid, hostmap from DB.
  }
