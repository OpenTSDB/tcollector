#!/usr/bin/env python
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

"""mountstats.py  Tcollector
#
# This script pull NFS mountstats data, dedupes it by mount point and puts it into the following namespaces:
#
#  proc.mountstats.<rpccall>.<metric>  nfshost=<nfsserver> nfsvol=<nfsvolume>
#   # Note that if subdirectories of nfsvol are mounted, but the 'events' line of /proc/self/mountstats is 
#   identical, then the metrics will be deduped, and the first alphabetic volume name will be used
#  proc.mountstats.bytes.<metric> 1464196613 41494104 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
#   # Taken from the 'bytes:'  line in /proc/self/mountstats
#   # each <metric> represents one field on the line
#
# See https://utcc.utoronto.ca/~cks/space/blog/linux/NFSMountstatsIndex
#   and https://utcc.utoronto.ca/~cks/space/blog/linux/NFSMountstatsNFSOps
#  for a great example of the data available in /proc/self/mountstats
#
# Example output:
# proc.mountstats.getattr.totaltime 1464196613 2670792 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.getattr.ops 1464196613 1570976 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.getattr.timeouts 1464196613 0 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.getattr.qtime 1464196613 14216 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.getattr.txbytes 1464196613 244313360 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.getattr.rttime 1464196613 1683992 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.getattr.rxbytes 1464196613 263929348 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.getattr.txs 1464196613 1570976 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.access.totaltime 1464196613 2670792 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.access.ops 1464196613 1570976 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.access.timeouts 1464196613 0 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.access.qtime 1464196613 14216 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.access.txbytes 1464196613 244313360 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.access.rttime 1464196613 1683992 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.access.rxbytes 1464196613 263929348 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.access.txs 1464196613 1570976 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.read.totaltime 1464196613 2670792 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.read.ops 1464196613 1570976 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.read.timeouts 1464196613 0 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.read.qtime 1464196613 14216 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.read.txbytes 1464196613 244313360 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.read.rttime 1464196613 1683992 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.read.rxbytes 1464196613 263929348 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.read.txs 1464196613 1570976 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.write.totaltime 1464196613 2670792 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.write.ops 1464196613 1570976 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.write.timeouts 1464196613 0 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.write.qtime 1464196613 14216 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.write.txbytes 1464196613 244313360 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.write.rttime 1464196613 1683992 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.write.rxbytes 1464196613 263929348 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.write.txs 1464196613 1570976 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.other.totaltime 1464196613 2670792 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.other.ops 1464196613 1570976 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.other.timeouts 1464196613 0 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.other.qtime 1464196613 14216 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.other.txbytes 1464196613 244313360 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.other.rttime 1464196613 1683992 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.other.rxbytes 1464196613 263929348 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.other.txs 1464196613 1570976 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.bytes.normalread 1464196613 41494104 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.bytes.normalwrite 1464196613 10145341022 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.bytes.directread 1464196613 0 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.bytes.directwrite 1464196613 0 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.bytes.serverread 1464196613 8413526 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.bytes.serverwrite 1464196613 10145494716 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.bytes.readpages 1464196613 2157 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
# proc.mountstats.bytes.writepages 1464196613 2477054 nfshost=fls1.sys.lab1.syseng.tmcs nfsvol=/vol/vol0
"""

import os
import socket
import sys
import time

PY3 = sys.version_info[0] > 2
if PY3:
    from hashlib import md5

    def md5_digest(line):
        return md5(line.encode("utf8")).digest()
else:
    import md5 # pylint: disable=import-error

    def md5_digest(line):
        return md5.new(line).digest()

COLLECTION_INTERVAL = 10  # seconds

# BYTES_FIELDS is individual fields in the 'bytes:  '   line
BYTES_FIELDS = ['normalread', 'normalwrite', 'directread', 'directwrite', 'serverread', 'serverwrite', 'readpages', 'writepages']
# KEY_METRICS contains the RPC call metrics we want specific data for
KEY_METRICS = ['GETATTR', 'ACCESS', 'READ', 'WRITE']
# OTHER_METRICS contains the other RPC call we will aggregate as 'OTHER'
OTHER_METRICS = ['SETATTR', 'LOOKUP', 'READLINK', 'CREATE', 'MKDIR', 'SYMLINK', 'MKNOD', 'REMOVE', 'RMDIR', 'RENAME', 'LINK', 'READDIR', 'READDIRPLUS', 'FSSTAT', 'FSINFO', 'PATHCONF', 'COMMIT']
# RPC_FIELDS is the individual metric fields on the RPC metric lines
RPC_FIELDS = ['ops', 'txs', 'timeouts', 'txbytes', 'rxbytes', 'qtime', 'rttime', 'totaltime']

def main():
    """nfsstats main loop."""
    try:
        f_nfsstats = open("/proc/self/mountstats", "r")
    except:
        sys.exit(13)

    while True:
        device = None
        f_nfsstats.seek(0)
        ts = int(time.time())
        rpc_metrics = { }
        for line in f_nfsstats:
            values = line.split(None)
            if len(values) == 0:
                continue

            if len(values) >= 8 and values[0] == 'device':
                if values[7] == 'nfs':
                    dupe = False
                    hostname, mount = values[1].split(':')
                    mountpoint = values[4]
                    mount = mount.rstrip("/")
                    device = hostname + mount + mountpoint
                    rpc_metrics[device] = { }
                    rpc_metrics[device]['other'] = dict((x,0) for x in RPC_FIELDS)
                    rpc_metrics[device]['nfshost'] = hostname
                    rpc_metrics[device]['nfsvol'] = mount
                    rpc_metrics[device]['mounts'] = [ mount ]
                    for metric in KEY_METRICS:
                        rpc_metrics[device][metric] = dict((x,0) for x in RPC_FIELDS)

            if device == None:
                continue

            if dupe == True:
                continue

            field = values[0].rstrip(":")

            # Use events as a deduping key for multiple mounts of the same volume
            # ( If multiple subdirectories of the same volume are mounted to different places they
            #   will show up in mountstats, but will have duplicate data. )
            if field == "events":
                m = md5_digest(line)
                rpc_metrics[device]['digest'] = m
                if m in rpc_metrics:
                    # metrics already counted, mark as dupe ignore
                    dupe = True
                    first_device=rpc_metrics[m]
                    rpc_metrics[first_device]['mounts'].append(mount)
                    rpc_metrics[device]['dupe'] = True
                else:
                    rpc_metrics[m] = device

            if field == "bytes":
                rpc_metrics[device]['bytes'] = dict((BYTES_FIELDS[i], values[i+1]) for i in range(0, len(BYTES_FIELDS)))

            if field in KEY_METRICS:
                for i in range(1, len(RPC_FIELDS) + 1):
                    metric = field
                    rpc_metrics[device][metric][RPC_FIELDS[i-1]] += int(values[i])

            if field in OTHER_METRICS:
                for i in range(1, len(RPC_FIELDS) + 1):
                    rpc_metrics[device]['other'][RPC_FIELDS[i-1]] += int(values[i])


        for device in rpc_metrics:
            # Skip the duplicates
            if 'dupe' in rpc_metrics[device]:
                continue
            # Skip the digest only entries (they wont have a referenct to the digest)
            if 'digest' not in rpc_metrics[device]:
                continue
            nfshost = rpc_metrics[device]['nfshost']
            rpc_metrics[device]['mounts'].sort()
            nfsvol = rpc_metrics[device]['mounts'][0]
            for metric in KEY_METRICS+['other']:
                for field in rpc_metrics[device][metric]:
                    print("proc.mountstats.%s.%s %d %s nfshost=%s nfsvol=%s" % (metric.lower(), field.lower(), ts, rpc_metrics[device][metric][field], nfshost, nfsvol))
            for field in BYTES_FIELDS:
                print("proc.mountstats.bytes.%s %d %s nfshost=%s nfsvol=%s" % (field.lower(), ts, rpc_metrics[device]['bytes'][field], nfshost, nfsvol))

        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)



if __name__ == "__main__":
    main()
