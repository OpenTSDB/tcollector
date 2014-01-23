#!/usr/bin/env python2.7
#
# Copyright (C) 2013  The tcollector Authors.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.
#
# Written by Jeremy Carroll <jeremy@pinterest.com>.
#
import collections
import time
import re
import sys
import urllib2
import json

# MBean Data Structure
Bean = collections.namedtuple('Bean', 'prefix uri filters deep')
# CONST
TIME = int(time.time())
URL = 'http://127.0.0.1:60030/jmx?qry='
TIMEOUT=5
# Prefixes
METRIC_PREFIX = 'hbase.rs'
TABLE_PREFIX = 'tbl\.'
CF_PREFIX = 'cf\.'
BLOCK_TYPE_PREFIX = 'bt\.'
REGION_PREFIX = 'region\.'
# Regular Expressions for use in Filters
FILTER_TAGS = {
    'table': '{}([a-zA-Z_0-9\-]+).'.format(TABLE_PREFIX),
    'columnfamily': '{}([a-zA-Z_0-9]+).'.format(CF_PREFIX),
    'blocktype': '{}([a-zA-Z_0-9]+).'.format(BLOCK_TYPE_PREFIX),
    'region': '{}([a-zA-Z_0-9]+).'.format(REGION_PREFIX)
}
# Special Case - dict() starts with Prefix
FILTER_OPERATIONS = {
    'avgTime': '([a-zA-Z_0-9.]+)[_.]?AvgTime$',
    'maxTime': '([a-zA-Z_0-9.]+)[_.]?MaxTime$',
    'minTime': '([a-zA-Z_0-9.]+)[_.]?MinTime$',
    'numOps': '([a-zA-Z_0-9.]+)[_.]?NumOps$'
}
# Key Value MBeans to Query
MBEANS = [
    Bean(prefix='rpc', uri=URL + 'hadoop:service=HBase,name=RPCStatistics-60020', filters=['extract_tags', 'extract_operations'], deep=False),
    Bean(prefix='dynamic', uri=URL + 'hadoop:service=RegionServer,name=RegionServerDynamicStatistics', filters=['extract_tags', 'extract_operations'], deep=False),
    Bean(prefix='stats', uri=URL + 'hadoop:service=RegionServer,name=RegionServerStatistics', filters=['extract_tags', 'extract_operations'], deep=False),
    Bean(prefix='replication', uri=URL + 'hadoop:service=Replication,name=ReplicationSink', filters=[], deep=False),
    Bean(prefix='replication.peer', uri=URL + 'hadoop:service=Replication,name=ReplicationSource*', filters=['extract_replication_peers'], deep=False),
    Bean(prefix='threads', uri=URL + 'java.lang:type=Threading', filters=[], deep=False),
    Bean(prefix='gc', uri=URL + 'java.lang:type=GarbageCollector,name=*', filters=['extract_garbage_collectors'], deep=False),
    Bean(prefix='os', uri=URL + 'java.lang:type=OperatingSystem', filters=[], deep=False),
    Bean(prefix='memory', uri=URL + 'java.lang:type=Memory', filters=[], deep=True)
#    Bean(prefix='optimizely', uri=URL + 'hadoop:service=Optimizely,name=OptlyStats', filters=['extract_optly_metrics'], deep=True) TODO
]

def get_json(uri):
    """ Request URL, load JSON, exit if error. """
    json_response = None
    r = urllib2.urlopen(uri, timeout=TIMEOUT)
    json_response = json.load(r)
    if not 'beans' in json_response:
        raise 'Could not find beans in JSON response'
    return json_response['beans']


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


def regex_replace(string, regex):
    """ Search string for regex. If match
    found, then remove whole match
    from string. Return tuple of truncated
    string + first group matched. """
    temp_string = string
    group = None
    match = re.search(regex, string)
    if match:
        temp_string = string.replace(match.group(0), '')
        group = match.group(1)
    return (temp_string, group)


def extract_tags(string, tags, bean):
    """ Recursively go through tag regular
    expressions. When a match is found,
    remove match from string and append
    groups to tags dict(). Continue
    until all regexes have been applied. """
    metric_string = string
    metric_tags = tags
    metric_bean = bean
    # Hack for special region .META.
    # If found, replace .META. with META
    if '.META.' in metric_string:
        metric_string = metric_string.replace('.META.', 'META')
    for tag, regex in FILTER_TAGS.iteritems():
        new_tag = {}
        (metric_string, new_tag[tag]) = regex_replace(metric_string, regex)
        if new_tag[tag]:
            metric_tags.update(new_tag)
    return (metric_string, metric_tags)


def extract_replication_peers(string, tags, bean):
    """ Extract replication stats for each
    peer configured. """
    metric_string = string
    metric_tags = tags
    metric_bean = bean
    replication = {}
    try:
        replication['peer'] = metric_bean.split(' ')[-1]
        metric_tags.update(replication)
    except Exception:
        pass
    return (metric_string, metric_tags)


def extract_garbage_collectors(string, tags, bean):
    """ Iterate over each Bean. 
    Look at the name, and create metrics
    for each Garbage Collector. """
    metric_string = string
    metric_tags = tags
    metric_bean = bean
    collector = {}
    try:
        collector['name'] = metric_bean.split('=')[-1]
        metric_tags.update(collector)
    except Exception:
        pass
    return (metric_string, metric_tags)


def extract_operations(string, tags, bean):
    """ Recursively go through tag regular
    expressions. When a match is found,
    remove match from string. Then prepend
    the 'prefix' to the key. Add a tag with
    'op' = regex.group(1).
    Ex: 'GetFlushAvgTime' => 'avgTime op=GetFlush'
    """
    metric_string = string
    metric_tags = tags
    metric_bean = bean
    # Resursively search all regexes in the FILTER_OPERATIONS dict()
    for tag, regex in FILTER_OPERATIONS.iteritems():
        (metric_string, tag_string) = regex_replace(metric_string, regex)
        if tag_string:
            found_tags = {}
            # Prepend 'tag' to metric string if match found.
            # Ex: 'FlushNumOps' => 'numOps op=flush'
            metric_string = tag + metric_string
            # Sometimes the regex leaves a dangling _ or . If
            # found remove them. Ex: deleted_
            if tag_string.endswith('_') or tag_string.endswith('.'):
                found_tags['op'] = tag_string[:-1]
            else:
                found_tags['op'] = tag_string
            metric_tags.update(found_tags)
    return (metric_string, metric_tags)


def deep_extract(string, values, bean):
    for value_key, value_value in values.iteritems():
        metric_tags = {'type': value_key}
        yield string, metric_tags, value_value

def extract(string, extractors, bean):
    """ Call methods for each extractor specified
    for this mbean. Return a tuple of (string, dict)
    comprising of the metric key and tag dictionary.
    """
    metric_string = string
    metric_tags = {}
    metric_bean = bean
    thismodule = sys.modules[__name__]
    for method in extractors:
        call = getattr(thismodule, method)
        (metric_string, new_tags) = call(metric_string, metric_tags, metric_bean)
        if new_tags:
            metric_tags.update(new_tags)
    return (metric_string, metric_tags)


def main():
    metrics = []

    # Collect statistics
    for bean in MBEANS:
        try:
            json = get_json(bean.uri)
        except Exception:
            continue
        for results in json:
            bean_name = results['name']
            for metric_key, metric_value in results.iteritems():
                if bean.deep and type(metric_value) is dict:
                    for metric_string, metric_tags, metric_value in deep_extract(metric_key, metric_value, bean_name):
                        metrics.append(format_tsd_key('.'.join([METRIC_PREFIX, bean.prefix, metric_string]), metric_value, tags=metric_tags))
                elif is_numeric(metric_value):
                    (metric_string, metric_tags) = extract(metric_key, bean.filters, bean_name)
                    metrics.append(format_tsd_key('.'.join([METRIC_PREFIX, bean.prefix, metric_string]), metric_value, tags=metric_tags))

    # Print metrics to StdOut
    for tsd in metrics:
        print tsd


if __name__ == '__main__':
    main()
