#!/usr/bin/python
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

"""Common utility functions shared for Python collectors"""

import os
import stat
import pwd
import errno
import sys
from Queue import Queue

# If we're running as root and this user exists, we'll drop privileges.
USER = "cwiz-user"


class RevertibleLowPrivilegeUser(object):
    def __init__(self, low_privelege_user, logger):
        self.low_privilege_user = low_privelege_user
        self.logger = logger

    def __enter__(self):
        if os.geteuid() != 0:
            return
        try:
            ent = pwd.getpwnam(self.low_privilege_user)
        except KeyError:
            return

        self.logger.info("set to lower-privilege user %s", self.low_privilege_user)
        os.setegid(ent.pw_gid)
        os.seteuid(ent.pw_uid)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.info("revert. set current euser %s back to %s", os.geteuid(), os.getuid())
        os.seteuid(os.getuid())


def lower_privileges(logger, user=USER):
    return RevertibleLowPrivilegeUser(user, logger)


# deprecated. use "with lower_privileges()" instead
def drop_privileges(user=USER):
    """Drops privileges if running as root."""
    try:
        ent = pwd.getpwnam(user)
    except KeyError:
        return

    if os.getuid() != 0:
        return

    os.setgid(ent.pw_gid)
    os.setuid(ent.pw_uid)


def is_sockfile(path):
    """Returns whether or not the given path is a socket file."""
    try:
        s = os.stat(path)
    except OSError, (no, e):
        if no == errno.ENOENT:
            return False
        err("warning: couldn't stat(%r): %s" % (path, e))
        return None
    return s.st_mode & stat.S_IFSOCK == stat.S_IFSOCK


def err(msg):
    print >> sys.stderr, msg


def is_numeric(value):
    return isinstance(value, (int, long, float))


def remove_invalid_characters(str):
    """removes characters unacceptable by opentsdb"""
    replaced = False
    lstr = list(str)
    for i, c in enumerate(lstr):
        if not (('a' <= c <= 'z') or ('A' <= c <= 'Z') or ('0' <= c <= '9') or c == '-' or c == '_' or
                c == '.' or c == '/' or c.isalpha()):
            lstr[i] = '_'
            replaced = True
    if replaced:
        return "".join(lstr)
    else:
        return str


class TestQueue(Queue):
    def nput(self, value):
        print value
