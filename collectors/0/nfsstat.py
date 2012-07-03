#!/usr/bin/python
#
# Copyright (C) 2012  Jari Takkala
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
"""import nfs stats from /proc into TSDB"""

import sys
import time

COLLECTION_INTERVAL = 15  # seconds

nfs_client_proc4_names = [
    # list of ops taken from nfs-utils / nfsstat.c
    "null", "read", "write", "commit", "open", "open_conf", "open_noat",
    "open_dgrd", "close", "setattr", "fsinfo", "renew", "setclntid", "confirm",
    "lock", "lockt", "locku", "access", "getattr", "lookup", "lookup_root",
    "remove", "rename", "link", "symlink", "create", "pathconf", "statfs",
    "readlink", "readdir", "server_caps", "delegreturn", "getacl", "setacl",
    "fs_locations", "rel_lkowner", "secinfo",
    # nfsv4.1 client ops
    "exchange_id", "create_ses", "destroy_ses", "sequence", "get_lease_t",
    "reclaim_comp", "layoutget", "getdevinfo", "layoutcommit", "layoutreturn",
    "getdevlist",
]


def main():
    """nfsstat main loop"""

    try:
        f_nfs = open("/proc/net/rpc/nfs", "r")
    except IOError, e:
        print >>sys.stderr, "Failed to open input file: %s" % (e,)
        return 13  # Ask tcollector to not re-start us immediately.

    while True:
        f_nfs.seek(0)
        ts = int(time.time())
        for line in f_nfs:
            fields = line.split()
            if fields[0] == "proc4":
                # NFSv4
                # first entry should equal total count of subsequent entries
                assert int(fields[1]) == len(fields[2:]), "Reported number of entries does not equal list length"
                for idx, val in enumerate(fields[2:]):
                    try:
                        print "nfs.client.v4.rpc %d %s op=%s" % (ts, int(val), nfs_client_proc4_names[idx])
                    except IndexError:
                        print >> sys.stderr, "%s: Warning: name lookup failed at position %d" % (sys.argv[0], idx)
            elif fields[0] == "rpc":
                # RPC
                calls = fields[1]
                retrans = fields[2]
                authrefrsh = fields[3]
                print "rpc.client.stats %d %s type=%s" % (ts, int(calls), "calls")
                print "rpc.client.stats %d %s type=%s" % (ts, int(retrans), "retrans")
                print "rpc.client.stats %d %s type=%s" % (ts, int(authrefrsh), "authrefrsh")

        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    main()
