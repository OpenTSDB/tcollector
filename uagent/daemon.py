#!/usr/bin/python2.7

import time
import logging
import signal
from uagent import UAgent
import sys
from optparse import OptionParser
from os import path
sys.path.append(path.abspath(path.join(path.dirname(__file__), path.pardir)))
import common_utils

DEFAULT_LOG_FILE = '/var/log/cloudwiz-uagent.log'
LOG = logging.getLogger('uagent')


def shutdown_signal(signum, frame):
    LOG.info("shutting down, got signal %d", signum)
    exit(0)


def main(argv):
    options, args = parse_cmdline(argv)
    common_utils.setup_logging(LOG, options.logfile, 64 * 1024 * 1024, 1)
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
        except SystemExit:
            LOG.info("shutting down, exit")
            exit(0)
        except:
            LOG.exception("failed to run one iteration of uagent. ignore error and retry after 5 minutes")
            time.sleep(300)


def parse_cmdline(argv):
    parser = OptionParser(description='manages upgrade agent options')
    parser.add_option('--logfile', dest='logfile', type='str',
                      default=DEFAULT_LOG_FILE,
                      help='Filename where logs are written to.')
    (options, args) = parser.parse_args(args=argv[1:])
    return options, args

if __name__ == "__main__":
    main(sys.argv)
