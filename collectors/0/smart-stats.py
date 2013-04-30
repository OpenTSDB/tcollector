#! /usr/bin/python

# This file is part of tcollector.
# Copyright (C) 2013  The tcollector Authors.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.
#
"""SMART disk stats for TSDB"""

import glob
import os
import signal
import subprocess
import sys
import time

ARCCONF = "/usr/local/bin/arcconf"
ARCCONF_ARGS = "GETVERSION 1"
NO_CONTROLLER = "Controllers found: 0"
BROKEN_DRIVER_VERSIONS = ("1.1-5",)

SMART_CTL = "smartctl"
SLEEP_BETWEEN_POLLS = 60
COMMAND_TIMEOUT = 10

# Common smart attributes, add more to this list if you start seeing
# numbers instead of attribute names in TSD results.
ATTRIBUTE_MAP = {
  "1": "raw_read_error_rate",
  "2": "throughput_performance",
  "3": "spin_up_time",
  "4": "start_stop_count",
  "5": "reallocated_sector_ct",
  "7": "seek_error_rate",
  "8": "seek_time_performance",
  "9": "power_on_hours",
  "10": "spin_retry_count",
  "11": "recalibration_retries",
  "12": "power_cycle_count",
  "13": "soft_read_error_rate",
  "175": "program_fail_count_chip",
  "176": "erase_fail_count_chip",
  "177": "wear_leveling_count",
  "178": "used_rsvd_blk_cnt_chip",
  "179": "used_rsvd_blk_cnt_tot",
  "180": "unused_rsvd_blk_cnt_tot",
  "181": "program_fail_cnt_total",
  "182": "erase_fail_count_total",
  "183": "runtime_bad_block",
  "184": "end_to_end_error",
  "187": "reported_uncorrect",
  "188": "command_timeout",
  "189": "high_fly_writes",
  "190": "airflow_temperature_celsius",
  "191": "g_sense_error_rate",
  "192": "power-off_retract_count",
  "193": "load_cycle_count",
  "194": "temperature_celsius",
  "195": "hardware_ecc_recovered",
  "196": "reallocated_event_count",
  "197": "current_pending_sector",
  "198": "offline_uncorrectable",
  "199": "udma_crc_error_count",
  "200": "write_error_rate",
  "233": "media_wearout_indicator",
  "240": "transfer_error_rate",
  "241": "total_lba_writes",
  "242": "total_lba_read",
  }


class Alarm(RuntimeError):
  pass


def alarm_handler(signum, frame):
  print >>sys.stderr, ("Program took too long to run, "
                       "consider increasing its timeout.")
  raise Alarm()


def smart_is_broken():
  if not os.path.exists(ARCCONF):
    # If arcconf isn't installed, we assume SMART is safe.
    return False
  signal.alarm(COMMAND_TIMEOUT)
  arcconf = subprocess.Popen("%s %s" % (ARCCONF, ARCCONF_ARGS),
                             shell=True,
                             stdout=subprocess.PIPE)
  arcconf_output = arcconf.communicate()[0]
  signal.alarm(0)
  if arcconf.returncode != 0:
    if arcconf_output and arcconf_output.startswith(NO_CONTROLLER):
      # No controller => no problem.
      return False
    if arcconf.returncode == 127:
      # arcconf doesn't even work on this system, so assume we're safe
      return False
    print >>sys.stderr, ("arcconf unexpected error %s" % arcconf.returncode)
    return True
  for line in arcconf_output.split("\n"):
    fields = [x for x in line.split(" ") if x]
    if fields[0] == "Driver" and fields[2] in BROKEN_DRIVER_VERSIONS:
      print >>sys.stderr, ("arcconf indicates broken driver version %s"
                           % fields[2])
      return True
  return False


def process_output(drive, smart_output):
  """Print formatted SMART output for the drive"""
  ts = int(time.time())
  smart_output = smart_output.split("\n")
  # Set data_marker to 0, so we skip stuff until we see a line
  # beginning with ID# in the output.  Start processing rows after
  # that point.
  data_marker = False
  is_seagate = False

  for line in smart_output:
    if data_marker:
      fields = line.split()
      if len(fields) < 2:
        continue
      field = fields[0]
      if len(fields) > 2 and field in ATTRIBUTE_MAP:
        metric = ATTRIBUTE_MAP[field]
        value = fields[9].split()[0]
        print ("smart.%s %d %s disk=%s" % (metric, ts, value, drive))
        if is_seagate and metric in ("seek_error_rate", "raw_read_error_rate"):
          # It appears that some Seagate drives (and possibly some Western
          # Digital ones too) use the first 16 bits to store error counts,
          # and the low 32 bits to store operation counts, out of these 48
          # bit values.  So try to be helpful and extract these here.
          value = int(value)
          print ("smart.%s %d %d disk=%s"
                 % (metric.replace("error_rate", "count"), ts,
                    value & 0xFFFFFFFF, drive))
          print ("smart.%s %d %d disk=%s"
                 % (metric.replace("error_rate", "errors"), ts,
                    (value & 0xFFFF00000000) >> 32, drive))
    elif line.startswith("ID#"):
      data_marker = True
    elif line.startswith("Device Model:"):
      model = line.split(None, 2)[2]
      # Rough approximation to detect Seagate drives.
      is_seagate = model.startswith("ST")


def main():
  """main loop for SMART collector"""

  # Get the list of block devices.
  drives = [dev[5:] for dev in glob.glob("/dev/[hs]d[a-z]")]

  # to make sure we are done with smartctl in COMMAND_TIMEOUT seconds
  signal.signal(signal.SIGALRM, alarm_handler)

  if smart_is_broken():
    sys.exit(13)

  while True:
    for drive in drives:
      signal.alarm(COMMAND_TIMEOUT)
      smart_ctl = subprocess.Popen(SMART_CTL + " -i -A /dev/" + drive,
                                   shell=True, stdout=subprocess.PIPE)
      smart_output = smart_ctl.communicate()[0]
      signal.alarm(0)
      if smart_ctl.returncode != 0:
        if smart_ctl.returncode == 127:
          sys.exit(13)
        else:
          print >>sys.stderr, "Command exited with: %d" % smart_ctl.returncode
      process_output(drive, smart_output)

    sys.stdout.flush()
    time.sleep(SLEEP_BETWEEN_POLLS)


if __name__ == "__main__":
  main()
