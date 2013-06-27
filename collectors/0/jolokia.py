#!/usr/bin/env python
#
# Script to use with tcollector and OpenTSDB
# to grab metrics from a java process using
# jolokia.
#
# Author: stuart-warren
# Additional work: mikebryant
#
import json
import urllib2
import time
from copy import copy
import yaml
import sys
from fnmatch import fnmatch
import os

USER_AGENT = 'DSI/Scripts'
CONFDIR = os.path.dirname(sys.argv[0]) + "/../etc"
#CONFDIR = os.path.dirname(sys.argv[0]) + "/."
'''
## Example valid base config - etc/jolokia.yaml

# fetch url every (in seconds)
interval: 20
# URL to jolokia/read/
url: 'http://localhost:8778/jolokia/read/'
# tags added ta ALL metrics. 'host' not actually needed but an example
common_tags:
- host: 'yourhostname'
# mbeans to monitor, see jolokia docs
# - mbean - added to url
# - metric - metric name prefix for this mbean
# - tags - tags manually added to metrics from this mbean
# - not_tags - auto tags removed from this metric
monitor:
- mbean: 'java.lang:type=*'
  metric: 'java.lang'
  not_tags:
  - ''
  - 'type'
- mbean: 'java.lang:name=CMS%20Old%20Gen,type=MemoryPool'
  metric: 'java.lang.memorypool.cmsoldgen'
  not_tags: 
  - ''
  - 'type'
- mbean: 'java.lang:name=CMS%20Perm%20Gen,type=MemoryPool'
  metric: 'java.lang.memorypool.cmspermgen'
  not_tags: 
  - ''
  - 'type'

## Example valid app config - etc/jolokia.appname.yaml

mbean: 'org.apache.cassandra.metrics:*'
metric: 'cassandra.metrics'
not_tags:
- ''
- 'type'
- 'name'

'''


# Log error message
def err(msg):
    print >>sys.stderr, msg


def check_jolokia(url):
    url = url.rpartition('read/')[0]
    req = urllib2.Request(url)
    opener = urllib2.build_opener()
    try:
        ff = opener.open(req)
    except urllib2.URLError, req:
        err("error: unable to connect to jolokia")
        sys.exit(13)
    except:
        err("error: unknown issue connecting to jolokia")
        sys.exit(13)

    try:
        # Get data
        data = json.load(ff)
        data['timestamp']
    except:
        err("error: unable to load json data from jolokia")
        sys.exit(13)


# Take a dict of attributes and print out numerical metrics
# Recurse if necessary
def print_metrics(d, metric_prefix, timestamp, tags, not_tags=[]):
    for k, v in d.iteritems():
        # Tack on the name of the attribute
        attribute, more_tags = parse_attribute(k.lower(), not_tags)
        metric_name = '.'.join([metric_prefix, attribute])
        my_tags = tags + more_tags
        # If numerical
        if (type(v) is int) or (type(v) is float) or (type(v) is long):
            print "%s %d %s %s" % (metric_name, timestamp, str(v), ' '.join(my_tags))
        # If a bool, True=1, False=0
        elif type(v) is bool:
            print "%s %d %s %s" % (metric_name, timestamp, str(int(v)), ' '.join(my_tags))
        # Or a dict of more attributes, call ourselves again
        elif type(v) is dict:
            print_metrics(v, metric_name, timestamp, my_tags, not_tags)
        else:
            #lists, strings, etc
            #print '# ', type(v), metric_name, str(v)
            pass


# Parse and order attribute text
# eg from:
#     org.apache.cassandra.metrics:name=CurrentlyBlockedTasks,path=request,scope=RequestResponseStage,type=ThreadPools
# to: cassandra.metrics.threadpools.currentlyblockedtasks.count, [path=request, scope=requestresponsestage]
def parse_attribute(attr, not_tags=[]):
    pruned = {}
    parts = attr.split(',')
    for p in parts:
        # Take anything to the right of a =
        tag_name, _, attrname = p.rpartition('=')
        tag_name = tag_name.split(':')[-1]
        # Swap out bad chars
        attrname = attrname.replace('.', '_').replace('/', '')
        pruned[tag_name] = attrname

    attr_list = []
    for t in not_tags:
        if t in pruned:
            attr_list.append(pruned[t])

    return '.'.join(attr_list), ["%s=%s" % (k, v) for k, v in pruned.items() if k not in not_tags]


def main():

    # Load in main config and then append
    # other config files
    try:
        CONFIG = yaml.load(file(CONFDIR + "/jolokia.yaml"))
        for conffile in os.listdir(CONFDIR):
            if fnmatch(conffile, 'jolokia.*.yaml'):
                CONFIG['monitor'].append(yaml.load(file(CONFDIR + '/' + conffile)))
    except:
        err("error: could not load one of the config files")
        sys.exit(1)
    # print CONFDIR
    # print json.dumps(CONFIG)

    url = CONFIG['url']
    # Make sure jolokia is actually running
    check_jolokia(url)

    # LOOP!!
    while True:

        tags = []
        # Add common tags to array
        if 'common_tags' in CONFIG:
            for t in CONFIG['common_tags']:
                tags.append(t.keys()[0] + '=' + t.values()[0])

        # For each mbean we are monitoring
        for f in CONFIG['monitor']:
            # Create local copy of tags so we can add more specific ones
            f_tags = copy(tags)
            f_not_tags = []

            metric_prefix = f['metric'].lower()
            if 'tags' in f:
                for ft in f['tags']:
                    # Add extra optional specific tags
                    f_tags.append(ft.keys()[0].lower() + '=' + ft.values()[0].lower())

            if 'not_tags' in f:
                f_not_tags = f['not_tags']

            # Create url to mbean stats
            req = urllib2.Request(url + f['mbean'], None, {'user-agent': USER_AGENT})
            opener = urllib2.build_opener()
            try:
                ff = opener.open(req)
            except urllib2.URLError, req:
                err("error: unable to connect to url")
                break
            except:
                err("error: unknown issue connecting to url")
                break

            try:
                # Get data
                data = json.load(ff)
                timestamp = data['timestamp']
            except:
                err("error: unable to load json data from url")
                break

            # Debug
            #print json.dumps(data, indent=2)

            print_metrics(data['value'], metric_prefix, timestamp, f_tags, f_not_tags)
        #sys.exit(0)
        time.sleep(CONFIG['interval'])
    # End while True

if __name__ == "__main__":
    main()

