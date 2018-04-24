import re
import requests
import sys

ILLEGAL_CHARS_REGEX = re.compile('[^a-zA-Z0-9\-_./]')


def format_tsd_key(metric_key, metric_value, time_, tags={}):
    def sanitize(s):
        return ILLEGAL_CHARS_REGEX.sub('_', str(s))

    expanded_tags = ''.join([' {}={}'.format(sanitize(key), sanitize(value)) for key, value in tags.iteritems()])
    output = '{} {} {} {}'.format(sanitize(metric_key), time_, metric_value, expanded_tags)
    return output


def get_json(url):
    """ Request URL, load JSON, exit if error. """
    url_json = None
    try:
        r = requests.get(url)
    except Exception, e:
        print 'Unable to query url {} - {}'.format(url, e)
        sys.exit(0)
    if r.status_code == 200:
        try:
            url_json = r.json()
        except Exception, e:
            print 'Could not load JSON for {}'.format(url)
            raise e
    else:
        print 'Did not receive 200 response for {}'.format(url)
    return url_json
