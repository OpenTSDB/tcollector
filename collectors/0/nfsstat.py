#!/usr/bin/python
#
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
"""Imports NFS stats from /proc."""

import sys
import time

from collectors.lib import utils

COLLECTION_INTERVAL = 15  # seconds

nfs_client_proc_names = {
    "proc4": (
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
    ),
    "proc3": (
        "null", "getattr", "setattr", "lookup", "access", "readlink",
        "read", "write", "create", "mkdir", "symlink", "mknod",
        "remove", "rmdir", "rename", "link", "readdir", "readdirplus",
        "fsstat", "fsinfo", "pathconf", "commit",
    ),
}


def main():
    """nfsstat main loop"""

    try:
        f_nfs = open("/proc/net/rpc/nfs")
    except IOError, e:
        print >>sys.stderr, "Failed to open input file: %s" % (e,)
        return 13  # Ask tcollector to not re-start us immediately.

    utils.drop_privileges()
    while True:
        f_nfs.seek(0)
        ts = int(time.time())
        for line in f_nfs:
            fields = line.split()
            if fields[0] in nfs_client_proc_names.keys():
                # NFSv4
                # first entry should equal total count of subsequent entries
                assert int(fields[1]) == len(fields[2:]), (
                    "reported count (%d) does not equal list length (%d)"
                    % (int(fields[1]), len(fields[2:])))
                for idx, val in enumerate(fields[2:]):
                    try:
                        print ("nfs.client.rpc %d %s op=%s version=%s"
                               % (ts, int(val), nfs_client_proc_names[fields[0]][idx], fields[0][4:]))
                    except IndexError:
                        print >> sys.stderr, ("Warning: name lookup failed"
                                              " at position %d" % idx)
            elif fields[0] == "rpc":
                # RPC
                calls = int(fields[1])
                retrans = int(fields[2])
                authrefrsh = int(fields[3])
                print "nfs.client.rpc.stats %d %d type=calls" % (ts, calls)
                print "nfs.client.rpc.stats %d %d type=retrans" % (ts, retrans)
                print ("nfs.client.rpc.stats %d %d type=authrefrsh"
                       % (ts, authrefrsh))

        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    sys.exit(main())
