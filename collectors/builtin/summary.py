import os
import json
import platform
import time
from time import localtime, strftime
from collectors.lib import utils
from collectors.lib.collectorbase import CollectorBase

SERVICE_RUNNING_TIME = [
    'hadoop',
    'hbase',
    'kafka',
    'mysql',
    'spark',
    'storm',
    'yarn',
    'zookeeper'
]


class Summary(CollectorBase):
    def __init__(self, config, logger, readq):
        super(Summary, self).__init__(config, logger, readq)
        ## would be send when the collector start
        runner_config = utils.load_runner_conf()
        version = runner_config.get('base', 'version')
        commit = runner_config.get('base', 'commit')
        token = runner_config.get('base', 'token')
        self.running_time = 0
        self.interval = self.get_config('interval')

        try:
            ip = get_ip()
        except Exception:
            self.log_error("can't get ip adress")

        services = json.loads(
                        os.popen(
                            os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../collector_mgr.py")+' json'
                        ).read())

        utils.summary_sender("collector.service", {}, {"type": "service"}, services)

        summary = {
            "version": version,
            "commitId": commit,
            "token": token,
            "start_time": strftime("%Y-%m-%d %H:%M:%S", localtime()),
            "os_version": platform.platform()
        }

        if ip and ip is not None:
            summary["ip"] = ip
            utils.summary_sender_info("collector.ip", {"value": ip})

        utils.summary_sender_info("collector.os", {"value": platform.platform()})
        utils.summary_sender("collector.summary", {}, {"type": "service"}, [summary])

    def __call__(self):
        self.running_time = self.running_time + int(self.interval)
        self._readq.nput("collector.state %s %s" % (int(time.time()), '0'))
        self._readq.nput("collector.runningTime %s %s" % (int(time.time()), self.running_time))


def get_ip():
    ips = os.popen("/sbin/ip -o -4 addr list| awk '{print $4}' | cut -d/ -f1").read().splitlines()
    if not ips:
        ips = os.popen(
            "ifconfig | grep -v 'eth0:'| grep -A 1 'eth0' | tail -1 | cut -d ':' -f 2 | cut -d ' ' -f 1").read().splitlines()

    try:
        ips.remove("127.0.0.1")
    except ValueError:
        pass
    ## we need one of the ip adress
    return (ips or [None])[0]
