import time
import requests
from collectors.lib.collectorbase import CollectorBase


# reference by http://opentsdb.net/docs/build/html/api_http/stats/index.html
STATS_HTTP_API = "/api/stats"


class Opentsdb(CollectorBase):
    def __init__(self, config, logger, readq):
        super(Opentsdb, self).__init__(config, logger, readq)
        self.port = self.get_config('port', 4242)
        self.host = self.get_config('host', "localhost")
        self.http_prefix = 'http://%s:%s' % (self.host, self.port)

    def __call__(self):
        try:
            stats = self.request(STATS_HTTP_API)
            if stats:
                for metric in stats:
                    self.printmetric(metric['metric'], metric['timestamp'], metric['value'], **metric['tags'])
        except Exception as e:
            self._readq.nput("opentsdb.state %s %s" % (int(time.time()), '1'))
            self.log_error("opentsdb collector except exception , abort %s" % e)

    def request(self,uri):
        resp = requests.get('%s%s' % (self.http_prefix, uri))
        if resp.status_code != 200:
            raise HTTPError('%s%s' % (self.http_prefix, uri))

        return resp.json()

    def printmetric(self,metric, ts, value, **tags):
        if tags:
            tags = " " + " ".join("%s=%s" % (name, value) for name, value in tags.iteritems())
        else:
            tags = ""
        self._readq.nput("%s %d %s %s" % (metric, ts, value, tags))
