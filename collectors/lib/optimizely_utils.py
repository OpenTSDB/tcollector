import re


ILLEGAL_CHARS_REGEX = re.compile('[^a-zA-Z0-9\-_./]')


def format_tsd_key(metric_key, metric_value, time_, tags={}):
    def sanitize(s):
        return ILLEGAL_CHARS_REGEX.sub('_', str(s))

    expanded_tags = ''.join([' {}={}'.format(sanitize(key), sanitize(value)) for key, value in tags.iteritems()])
    output = '{} {} {} {}'.format(sanitize(metric_key), time_, metric_value, expanded_tags)
    return output
