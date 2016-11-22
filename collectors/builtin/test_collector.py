import time
from collectors.lib.collectorbase import CollectorBase
from collectors.lib.inc_processor import IncPorcessor


class TestCollector(CollectorBase):

    def __init__(self, config, logger, readq):
        super(TestCollector, self).__init__(config, logger, readq)
        self.currval1 = 8
        self.currval2 = 10
        self.sampling_count = 0
        self.inc_proc = IncPorcessor(None)

    def __call__(self):
        ts = time.time()
        self.currval1 = self.currval1 + 2
        self.currval2 = self.currval2 + self.sampling_count * 2
        self.sampling_count = self.sampling_count + 1
        self._readq.nput('metric1 %d %d t1=10 t2=a metric_type=counter' % (ts, self.currval1))
        self._readq.nput('metric2 %d %d t1=20 t2=b metric_type=counter' % (ts, self.currval2))
        self._readq.nput(self.inc_proc.process('metric3', ts, self.currval2))
