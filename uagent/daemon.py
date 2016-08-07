#!/usr/bin/python3.5

import importlib
import time
import uagent

while True:
    uagent.main()
    time.sleep(3600)
    # in case the agent was upgraded
    importlib.reload(uagent)
