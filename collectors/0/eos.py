#!/usr/bin/python
# Copyright (c) 2013 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

import optparse
import os
import sys
import re
import time

try:
  import Tac

  import EntityManager
  import LazyMount
  import Plugins
except ImportError, e:
  print >> sys.stderr, e
  sys.exit(13)

from CliPlugin import IntfCli


class FakeMode(object):
  def __init__(self, sysdbRoot):
    self.sysdbRoot = sysdbRoot


class Interface(object):
  """An interface of the system that supports counters."""

  def __init__(self, intf):
    """Ctor.

    Args:
      intf: An instance of IntfCli.Intf (or generally of one of its subclasses).
    """
    self.intf_ = intf

  def printCounters(self):

    def _printCounter():
      print ("eos.interface.%s %d %d iface=%s%s"
             % (counter, ts, value, self.intf_.name, tag))

    ts = int(time.time())
    counters = self.intf_.counter()
    statistics = counters.statistics
    for counter in statistics.attributes:
      value = getattr(statistics, counter)
      m = re.match("(in|out)([A-Z])(.*)", counter)
      if m:
        tag, first, rest = m.groups()
        counter = first.lower() + rest
        tag = " direction=" + tag
      else:
        tag = ""
      _printCounter()

    # priority-flow-control counters
    statistics = getattr(counters, "ethStatistics", None)
    if not statistics:
      return  # Only physical interfaces have those.
    for counter in statistics.attributes:
      value = getattr(statistics, counter)
      if value is None:
        continue  # No ethStatistics for management interfaces.
      m = re.match("(in|out)([A-Z0-9])(.*)", counter)
      if not m:
        continue
      direction, first, rest = m.groups()
      counter = first.lower() + rest
      tag = " direction=" + direction
      if counter.endswith("OctetFrames"):
        tag += " size=" + counter[:-11]  # Drop the "OctetFrames"
        counter = "frameBySize"
      if counter == "pfcClassFrames":
        counter = "pfcFramesByClass"
        values = value.count
        base_tags = tag
        for priority, value in values.iteritems():
          tag = base_tags + " priority=%d" % priority
          _printCounter()
      else:
        _printCounter()


class Interfaces(object):
  """An iterable list of interfaces that support counters in the system."""
  # XXX Should be a Tac.Notifiee to react to interface creation / deletion

  def __init__(self, entityManager):
    self.em_ = entityManager
    self.interfaces_ = None
    self.loadInterfaces_()

  def __len__(self):
    return len(self.interfaces_)

  def __iter__(self):
    return iter(self.interfaces_)

  def loadInterfaces_(self):
    mode = FakeMode(self.em_.root())
    self.interfaces_ = [Interface(intf)
                        for intf in IntfCli.counterSupportedIntfs(mode)]


def getopt(args):
  parser = optparse.OptionParser()
  parser.add_option("-s", "--sysname", action="store", help="Sysdb system name",
                    default=os.environ.get("SYSNAME", "ar"))
  opts, args = parser.parse_args()
  if args:
    parser.error("Extraneous arguments: %r" % args)
  return opts


def main(args):
  opts = getopt(args)
  em = EntityManager.Sysdb(opts.sysname)
  if "setEntityManager" in dir(LazyMount):
     # Used to be required until at least 4.10.2
     LazyMount.setEntityManager(em)
  mg = em.mountGroup()
  Plugins.loadPlugins("CliPlugin", context=em)
  mg.close(blocking=True)
  interfaces = Interfaces(em)
  while True:
    for intf in interfaces:
      intf.printCounters()
    time.sleep(10)

if __name__ == "__main__":
  sys.exit(main(sys.argv))
