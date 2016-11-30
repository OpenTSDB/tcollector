import time
import requests
from collectors.lib.collectorbase import CollectorBase

# reference: http://flume.apache.org/FlumeUserGuide.html#json-reporting
# restart flume
# $ bin/flume-ng agent --conf-file example.conf --name a1 -Dflume.monitoring.type=http -Dflume.monitoring.port=34545

# request : http://<hostname>:<port>/metrics
#
# response:
# {
#   "CHANNEL.fileChannel":{"EventPutSuccessCount":"468085",
#                          "Type":"CHANNEL",
#                          "StopTime":"0",
#                          "EventPutAttemptCount":"468086",
#                          "ChannelSize":"233428",
#                          "StartTime":"1344882233070",
#                          "EventTakeSuccessCount":"458200",
#                          "ChannelCapacity":"600000",
#                          "EventTakeAttemptCount":"458288"},
#   "CHANNEL.memChannel":{"EventPutSuccessCount":"22948908",
#                         "Type":"CHANNEL",
#                         "StopTime":"0",
#                         "EventPutAttemptCount":"22948908",
#                         "ChannelSize":"5",
#                         "StartTime":"1344882209413",
#                         "EventTakeSuccessCount":"22948900",
#                         "ChannelCapacity":"100",
#                         "EventTakeAttemptCount":"22948908"}
# }

REST_API = "/metrics"
EXCLUDE = [ 'StartTime', 'StopTime', 'Type' ]

class Flume(CollectorBase):
  def __init__(self, config, logger, readq):
    super(Flume, self).__init__(config, logger, readq)

    self.port = self.get_config('port', 8080)
    self.host = self.get_config('host', "localhost")
    self.http_prefix = 'http://%s:%s' % (self.host, self.port)

  def __call__(self):
    try:
      stats = self.request(REST_API)
      ts = time.time()
      for metric in stats:
        (component, name) = metric.split(".")
        tags = {component.lower(): name}
        for key,value in stats[metric].items():
          if key not in EXCLUDE:
            self.printmetric(key.lower(), ts, value, **tags)
    except Exception as e:
      self.log_exception('exception collecting flume cluster metric form : %s \n %s' % ('%s%s' % (self.http_prefix, REST_API), e))

  def request(self,uri):
    resp = requests.get('%s%s' % (self.http_prefix, uri))
    if resp.status_code != 200:
      raise HTTPError(resp)

    return resp.json()

  def printmetric(self,metric, ts, value, **tags):
    if tags:
      tags = " " + " ".join("%s=%s" % (name, value) for name, value in tags.iteritems())
      self.log_info(tags)
    else:
      tags = ""
    self._readq.nput("flume.%s %d %s %s" % (metric, ts, value, tags))

class HTTPError(RuntimeError):
  def __init__(self, resp):
    RuntimeError.__init__(self, str(resp))
    self.resp = resp