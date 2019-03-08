#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2010-2013  The tcollector Authors.
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
"""Imports Docker stats from the docker-api"""

from __future__ import print_function
import sys

from collectors.etc import docker_engine_conf
from collectors.lib.docker_engine.docker_metrics import DockerMetrics

CONFIG = docker_engine_conf.get_config()
ENABLED = docker_engine_conf.enabled()
METRICS_PATH = CONFIG['metrics_path']


def main():
    if not ENABLED:
        sys.stderr.write("Docker-engine collector is not enabled")
        sys.exit(13)

    """docker_cpu main loop"""
    cli = DockerMetrics(METRICS_PATH)

    for m in cli.get_endpoint():
        print(m.get_metric_lines())


if __name__ == "__main__":
    sys.exit(main())
