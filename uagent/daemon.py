#!/usr/bin/python2.7

import time
import logging
import signal
from uagent import UAgent
import sys
from os import path
sys.path.append(path.abspath(path.join(path.dirname(__file__), path.pardir)))
import common_utils

LOG_FILE = '/var/log/cloudwiz-uagent.log'
LOG = logging.getLogger('uagent')
common_utils.setup_logging(LOG, LOG_FILE, 64 * 1024 * 1024, 1)


def shutdown_signal(signum, frame):
    LOG.info("shutting down, got signal %d", signum)
    exit(0)


def main():
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, shutdown_signal)

    while True:
        LOG.info("uagnt daemon starting...")
        try:
            uagent = UAgent(LOG)
            exit_code = uagent.run()
            LOG.info("uagnt daemon finished. exit code %d. sleep for 1 hour and recheck.", exit_code)
            time.sleep(3600)
            # in case the update agent itself was upgraded
            LOG.info("reloading uagent package...")
            reload(uagent)
            LOG.info("reloaded uagent package.")
        except:
            LOG.exception("failed to run one iteration of uagent. ignore error and retry after 5 minutes")
            time.sleep(300)


if __name__ == "__main__":
    main()