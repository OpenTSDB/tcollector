#!/usr/bin/env python
import time
from kazoo.client import KazooClient

METRIC_PREFIX = 'zookeeper'
TIME = int(time.time())

def is_numeric(obj):
    """ Check to see if a variable is a int or float.
    Also check if value is >= 0. """
    if isinstance(obj, int) or isinstance(obj, float):
        return True
    return False


def parse_stats(string):
    """ Given a ZK result set, parse into
    key values. """
    metric_dict = {}
    try:
        for line in string.split("\n"):
            # Remove duplicate whitespaces
            trimed = ' '.join(line.split()).split(' ')
            if len(trimed) == 2:
                try:
                    metric = trimed[0]
                    value = int(trimed[1])
                    metric_dict[metric] = value
                except Exception:
                    continue
    except Exception, e:
        pass
    return metric_dict


def get_zk_stats(conn):
    """ Retrieve ZK stats. """
    monitor = None
    try:
        result = conn.command('mntr')
        monitor = parse_stats(result)
    except Exception:
        pass
    return monitor


def get_connection():
    """ Grab a ZK connection. """
    try:
        zk = KazooClient(timeout=5)
        zk.start()
    except Exception, e:
        print 'Could not connect to ZK'
        raise e
    return zk


def format_tsd_key(metric_key, metric_value, tags={}):
    """ Formats a key for OpenTSDB """
    expanded_tags = ''.join([' %s=%s' % (key, value) for key, value in tags.iteritems()])
    output = '{} {} {}{}'.format(metric_key, TIME, metric_value, expanded_tags)
    return output


def main():
    conn = get_connection()
    stats = get_zk_stats(conn)
    for metric_string, metric_value in stats.iteritems():
        print format_tsd_key('.'.join([METRIC_PREFIX,  metric_string]), metric_value)


if __name__ == '__main__':
    main()
