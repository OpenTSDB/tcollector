#!/usr/bin/env python
import time
import simplejson as json
import requests
import re
import sys
 
 
# Constants
METRIC_PREFIX = 'optimizely.api.'
TIME = int(time.time())
URL_FORMAT = 'http://127.0.0.1:8080/v1/{}/stats'

KEYS = {
    'opCount': "^(.*)Count$",
    'opTime': "^(.*)Time$",
    'opDuration': "^(.*)Duration$"
}

PATHS = [
    "results"
]
 
def get_json(url):
    """ Request URL, load JSON, exit if error. """
    url_json = None
    try:
        r = requests.get(url)
    except Exception, e:
        print 'Unable to query url {} - {}'.format(url, e)
        sys.exit(13)
    if r.status_code == 200:
        try:
            url_json = r.json()
        except Exception, e:
            print 'Could not load JSON for {}'.format(url)
            raise e
    else:
        print 'Did not receive 200 response for {}'.format(url)
    return url_json

 
def format_tsd_key(metric_key, metric_value, tags={}):
    """ Formats a key for OpenTSDB """
    expanded_tags = ''.join([' %s=%s' % (key, value) for key, value in tags.iteritems()])
    output = '{} {} {} {}'.format(metric_key, TIME, metric_value, expanded_tags)
    return output
 
 
def main():
    # Collect statistics
    for path in PATHS:
        json = get_json(URL_FORMAT.format(path))

        for item, value in json.iteritems():
            for key, tag_expression in KEYS.iteritems():
                matches = re.match(tag_expression, item)
                if matches:
                    tag = matches.group(1)
                    print format_tsd_key(METRIC_PREFIX + path + "." + key, value, {'type': tag})
 
 
if __name__ == '__main__':
    main()
