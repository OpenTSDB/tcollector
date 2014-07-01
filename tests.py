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

import os
import sys
from stat import S_ISDIR, S_ISREG, ST_MODE
import unittest

import tcollector


class CollectorsTests(unittest.TestCase):

    def test_collectorsAccessRights(self):
        """Test of collectors access rights, permissions should be 0100775."""

        def check_access_rights(top):
            for f in os.listdir(top):
                pathname = os.path.join(top, f)
                mode = os.stat(pathname).st_mode

                if S_ISDIR(mode):
                    # directory, recurse into it
                    check_access_rights(pathname)
                elif S_ISREG(mode):
                    # file, check permissions
                    self.assertEqual("0100775", oct(os.stat(pathname)[ST_MODE]))
                else:
                    # unknown file type
                    pass

        collectors_path = os.path.dirname(os.path.abspath(__file__)) + \
            "/collectors/0"
        check_access_rights(collectors_path)


class TSDBlacklistingTests(unittest.TestCase):
    """
    Tests of TSD blacklisting logic
    https://github.com/OpenTSDB/tcollector/commit/c191d0d0889860db2ea231cad02e398843031a74
    """

    def setUp(self):
        # Stub out the randomness
        self.random_shuffle = tcollector.random.shuffle
        tcollector.random.shuffle = lambda x: x

    def tearDown(self):
        tcollector.random.shuffle = self.random_shuffle

    def mkSenderThread(self, tsds):
        return tcollector.SenderThread(None, True, tsds, False, {})

    def test_blacklistOneConnection(self):
        tsd = ("localhost", 4242)
        sender = self.mkSenderThread([tsd])
        sender.pick_connection()
        self.assertEqual(tsd, (sender.host, sender.port))
        sender.blacklist_connection()
        sender.pick_connection()
        self.assertEqual(tsd, (sender.host, sender.port))

    def test_blacklistTwoConnections(self):
        tsd1 = ("localhost", 4242)
        tsd2 = ("localhost", 4243)
        sender = self.mkSenderThread([tsd1, tsd2])
        sender.pick_connection()
        self.assertEqual(tsd1, (sender.host, sender.port))
        sender.blacklist_connection()
        sender.pick_connection()
        self.assertEqual(tsd2, (sender.host, sender.port))
        sender.blacklist_connection()
        sender.pick_connection()
        self.assertEqual(tsd1, (sender.host, sender.port))

    def test_doublePickOneConnection(self):
        tsd = ("localhost", 4242)
        sender = self.mkSenderThread([tsd])
        sender.pick_connection()
        self.assertEqual(tsd, (sender.host, sender.port))
        sender.pick_connection()
        self.assertEqual(tsd, (sender.host, sender.port))

    def test_doublePickTwoConnections(self):
        tsd1 = ("localhost", 4242)
        tsd2 = ("localhost", 4243)
        sender = self.mkSenderThread([tsd1, tsd2])
        sender.pick_connection()
        self.assertEqual(tsd1, (sender.host, sender.port))
        sender.pick_connection()
        self.assertEqual(tsd2, (sender.host, sender.port))
        sender.pick_connection()
        self.assertEqual(tsd1, (sender.host, sender.port))

if __name__ == '__main__':
    tcollector.setup_logging()
    sys.exit(unittest.main())
