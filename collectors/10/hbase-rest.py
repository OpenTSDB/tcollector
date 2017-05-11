#!/usr/bin/env python
#
# Written by Jeremy Carroll <jeremy@pinterest.com>.
#
import time
import sys
import urllib2
import base64
import simplejson as json

# CONST
TIME = int(time.time())
URL = 'http://127.0.0.1:8080/status/cluster'
TIMEOUT=5
METRIC_PREFIX='hbase.regionserver.dynamic'
#_ Optimization. Add more specific keys to OpenTSDB to help lookup speed
DOUBLE_WRITE=True

def get_json(uri):
    """ Request URL, load JSON, exit if error. """
    request = urllib2.Request(uri, headers={'Accept': 'application/json'})
    r = urllib2.urlopen(request, timeout=TIMEOUT)
    json_response = json.load(r)
    if 'LiveNodes' in json_response:
        return json_response['LiveNodes']
    raise Exception, 'LiveNodes does not exist in JSON response'


def is_numeric(obj):
    """ Check to see if a variable is a int or float.
    Also check if value is >= 0. """
    if isinstance(obj, int) or isinstance(obj, float):
        if isinstance(obj, bool):
            return False
        if int(obj) >= 0:
            return True
    return False


def format_tsd_key(metric_key, metric_value, tags=[]):
    """ Formats a key for OpenTSDB """
    expanded_tags = ''.join([' %s=%s' % (key, value) for key, value in tags.iteritems()])
    output = '{} {} {}{}'.format(metric_key, TIME, metric_value, expanded_tags)
    return output


def main():
    metrics = []

    # Collect statistics
    try:
        json = get_json(URL)
    except Exception, e:
        print 'Exception processing JSON: {}'.format(e)
        sys.exit(1)
    for regionserver in json:
        tags = {}
        if 'Node' in regionserver:
            regionserver = regionserver['Node']
        if not 'name' in regionserver:
            sys.exit(1)
        rs = regionserver['name'].split(':')[0]
        tags['server'] = rs
        for stats in regionserver['Region']:
            region = None
            table = None
            b64_decode = base64.b64decode(stats['name'])
            try:
                if '.META.' in b64_decode:
                    tags['table'] = 'META'
                elif '-ROOT-' in b64_decode:
                    tags['table'] = 'ROOT'
                else:
                    tags['table'] = b64_decode.split(',')[0]
                    tags['region'] = b64_decode.split('.')[-2]
                table = tags['table']
                region = tags['region']
            except Exception:
                pass
            for metric_string, metric_value in stats.iteritems():
                if is_numeric(metric_value):
                    metrics.append(format_tsd_key('.'.join([METRIC_PREFIX, metric_string]), metric_value, tags=tags))
                    if table and region and DOUBLE_WRITE:
                        #_ Double write a more specific metric for speed optimization
                        metrics.append(format_tsd_key('.'.join([METRIC_PREFIX, table, region, metric_string]), metric_value, tags={'server': tags['server']}))
    for tsd in metrics:
        print tsd


if __name__ == '__main__':
    main()
