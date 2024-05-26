#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2014  The tcollector Authors.
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

import signal
import subprocess
import time
import unittest
import os


class ProcstatsTests(unittest.TestCase):
    """Just make sure you can run a collector without it blowing up."""

    def test_start_terminate(self):
        env = os.environ.copy()
        if env.get("PYTHONPATH"):
            env["PYTHONPATH"] += ":."
        else:
            env["PYTHONPATH"] = "."
        p = subprocess.Popen(["collectors/available/linux/long-lived/procstats.py"], env=env,
                             stdout=subprocess.PIPE)
        time.sleep(5)
        p.terminate()
        time.sleep(1)
        if p.poll() is None:
            p.kill()
        self.assertEqual(p.poll(), -signal.SIGTERM)
        self.assertIn(b"proc.", p.stdout.read())


if __name__ == '__main__':
    unittest.main()
