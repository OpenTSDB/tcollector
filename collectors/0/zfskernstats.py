#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2012  The tcollector Authors.
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

import errno
import re
import sys
import time

'''
ZFS kernel memory statistics for TSDB

This plugin tracks kernel memory for both:

- the SPL and its allocated slabs backing ZFS memory
  zfs.mem.slab
- the ARC and its various values
  zfs.mem.arc
'''

# /proc/spl/slab has several fields.  we only care about the sizes
# and the allocation sizes for the slabs
# /proc/spl/kstat/zfs/arcstats is a table.  we only care about the data column

def main():
    """zfsstat main loop"""
    interval = 15
    typere = re.compile("(^.*)_[0-9]+$")

    try:
        f_slab = open("/proc/spl/kmem/slab", "r")
        f_arcstats = open("/proc/spl/kstat/zfs/arcstats", "r")
    except IOError, e:
        if e.errno == errno.ENOENT:
            # it makes no sense to run this collector here
            sys.exit(13) # we signal tcollector to not run us
        raise

    while True:
        f_slab.seek(0)
        f_arcstats.seek(0)
        ts = int(time.time())

        for n, line in enumerate(f_slab):
            if n < 2:
                continue
            line = line.split()
            name, _, size, alloc, _, objsize = line[0:6]
            size, alloc, objsize = int(size), int(alloc), int(objsize)
            typ = typere.match(name)
            if typ:
                typ = typ.group(1)
            else:
                typ = name
            print ("zfs.mem.slab.size %d %d type=%s objsize=%d" %
                  (ts, size, typ, objsize)
            )
            print ("zfs.mem.slab.alloc %d %d type=%s objsize=%d" %
                  (ts, alloc, typ, objsize)
            )

        for n, line in enumerate(f_arcstats):
            if n < 2:
                continue
            line = line.split()
            name, _, data = line
            data = int(data)
            print ("zfs.mem.arc.%s %d %d" %
                  (name, ts, data)
            )

        sys.stdout.flush()
        time.sleep(interval)

if __name__ == "__main__":
    main()

