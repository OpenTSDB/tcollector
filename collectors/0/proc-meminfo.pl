#!/usr/bin/perl
# This file is part of tcollector.
# Copyright (C) 2010  StumbleUpon, Inc.
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
# simple script to convert /proc/meminfo into a tsdb format and demonstrate
# using Perl scripts with tcollector

# required: flushes STDOUT every newline
$| = 1;

while (1) {
    my $ts = time();
    open FILE, "</proc/meminfo";
    while (<FILE>) {
        next unless /^(\w+):\s+(\d+)/;
        printf "proc.meminfo.%s %d %d\n", lc $1, $ts, $2;
    }
    sleep 15;
}
