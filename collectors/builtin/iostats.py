import time
import re
import sys
import os
from collectors.lib.collectorbase import CollectorBase

class Iostats(CollectorBase):
    def __init__(self, config, logger, readq):
        super(Iostats, self).__init__(config, logger, readq)
        self.header_re = re.compile(r'([%\\/\-_a-zA-Z0-9]+)[\s+]?')
        self.item_re = re.compile(r'^([\-a-zA-Z0-9\/]+)')
        self.value_re = re.compile(r'\d+\.\d+')

    def _parse_linux2(self, output):
        recentStats = output.split('Device:')[2].split('\n')
        header = recentStats[0]
        headerNames = re.findall(self.header_re, header)
        device = None

        ioStats = {}

        for statsIndex in range(1, len(recentStats)):
            row = recentStats[statsIndex]

            if not row:
                # Ignore blank lines.
                continue

            deviceMatch = self.item_re.match(row)

            if deviceMatch is not None:
                # Sometimes device names span two lines.
                device = deviceMatch.groups()[0]
            else:
                continue

            values = re.findall(self.value_re, row)

            if not values:
                # Sometimes values are on the next line so we encounter
                # instances of [].
                continue

            ioStats[device] = {}

            for headerIndex in range(len(headerNames)):
                headerName = headerNames[headerIndex]
                self._readq.nput("iostat.%s %d %s device=%s" % (
                headerName.replace("%", "percentage_"), int(time.time()), values[headerIndex], device))

    def _parse_darwin(self, output):
        lines = [l.split() for l in output.split("\n") if len(l) > 0]
        disks = lines[0]
        lastline = lines[-1]
        io = {}
        for idx, disk in enumerate(disks):
            kb_t, tps, mb_s = map(float, lastline[(3 * idx):(3 * idx) + 3])  # 3 cols at a time
            self._readq.nput("iostat.%s %d %s device=%s" % ('bytes_per_s', int(time.time()), mb_s * 2 ** 20, disk))

        return io

    def __call__(self):
        try:
            if is_linux():
                stdout = os.popen("iostat -d 1 2 -x -k").read()
                #                 Linux 2.6.32-343-ec2 (ip-10-35-95-10)   12/11/2012      _x86_64_        (2 CPU)
                #
                # Device:         rrqm/s   wrqm/s     r/s     w/s    rkB/s    wkB/s avgrq-sz avgqu-sz   await  svctm  %util
                # sda1              0.00    17.61    0.26   32.63     4.23   201.04    12.48     0.16    4.81   0.53   1.73
                # sdb               0.00     2.68    0.19    3.84     5.79    26.07    15.82     0.02    4.93   0.22   0.09
                # sdg               0.00     0.13    2.29    3.84   100.53    30.61    42.78     0.05    8.41   0.88   0.54
                # sdf               0.00     0.13    2.30    3.84   100.54    30.61    42.78     0.06    9.12   0.90   0.55
                # md0               0.00     0.00    0.05    3.37     1.41    30.01    18.35     0.00    0.00   0.00   0.00
                #
                # Device:         rrqm/s   wrqm/s     r/s     w/s    rkB/s    wkB/s avgrq-sz avgqu-sz   await  svctm  %util
                # sda1              0.00     0.00    0.00   10.89     0.00    43.56     8.00     0.03    2.73   2.73   2.97
                # sdb               0.00     0.00    0.00    2.97     0.00    11.88     8.00     0.00    0.00   0.00   0.00
                # sdg               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00   0.00   0.00
                # sdf               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00   0.00   0.00
                # md0               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00   0.00   0.00
                self._parse_linux2(stdout)

            elif sys.platform == 'darwin':
                stdout = os.popen("iostat -d -c 2 -w 1").read()
                #          disk0           disk1          <-- number of disks
                #    KB/t tps  MB/s     KB/t tps  MB/s
                #   21.11  23  0.47    20.01   0  0.00
                #    6.67   3  0.02     0.00   0  0.00    <-- line of interest
                self._parse_darwin(stdout)

            else:
                self._readq.nput("iostats.state %s %s" % (int(time.time()), '1'))
                # there is sunos or freebsd can't be test so . I haven't done . if we need  please add it once
                return False

            self._readq.nput("iostats.state %s %s" % (int(time.time()), '0'))
        except Exception as e:
            self._readq.nput("iostats.state %s %s" % (int(time.time()), '1'))
            self.log_exception('exception collecting iostats metrics %s' % e)

def is_linux(name=None):
    name = name or sys.platform
    return 'linux' in name
