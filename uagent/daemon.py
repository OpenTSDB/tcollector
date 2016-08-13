#!/usr/bin/python2.7

import time
import uagent

while True:
    uagent.main()
    time.sleep(3600)
    # in case the update agent itself was upgraded
    reload(uagent)
