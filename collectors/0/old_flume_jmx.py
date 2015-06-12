#!/usr/bin/python

import sys
from flume_jmx import FlumeJmxMonitor

class OldFlumeJmxMonitor(FlumeJmxMonitor):
    """ Necessary because the 'old' flume runs using the ``optimizely`` user.
    """
    USER = "optimizely"

def main():
    return OldFlumeJmxMonitor.start_monitors()

if __name__ == "__main__":
    sys.exit(main())
