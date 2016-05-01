import time
import random
from collectors.lib.collectorbase import CollectorBase


class TestCollector(CollectorBase):

    def __init__(self, config, logger):
        super(TestCollector, self).__init__(config, logger)

    def __call__(self):
        ts = time.time()
        return ['metric1 %d %d t1=10 t2=a' % (ts, random.randint(10, 10000)),
                'metric2 %d %d t1=20 t2=b' % (ts, random.randint(1000, 10000000)),
                'metric3 %d %d t1=30 t2=c' % (ts, random.randint(1, 1000))]
