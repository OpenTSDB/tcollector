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

package oracleconf;

use strict;
use warnings;
use POSIX;

require Exporter;

our @ISA = qw(Exporter);

our @EXPORT_OK = qw( oracle_config switch_to_dba );

use constant ORATAB_FILE => '/etc/oratab';

# Return mapping of Oracle SIDs to Oracle homes
sub oracle_config {
  my %oracle_config;

  # By default we will try to figure database list from
  # /etc/oratab . But you can define configuration manually
  # here, in which case /etc/oratab will not be checked
  #%oracle_config = (
  #  "SID" => "ORACLE_HOME"
  #); 

  if ((keys %oracle_config == 0) and (-f ORATAB_FILE)) {
    my ($sid,$home,$extra);
    open ORATAB, ORATAB_FILE; 
    while (<ORATAB>) {
      ($sid,$home,$extra) = split(':');
      # OEM agent uses this
      next if $sid =~ /^\*/;
      # ASM uses this
      next if $sid =~ /^\+/;
      $oracle_config{$sid} = $home; 
    }
    close ORATAB;
  }
  return \%oracle_config;
}

# In order to connect as "/" without password we need to
# be user that can do it. Change username if you use something
# different from 'oracle'
sub switch_to_dba {
  if (POSIX::getuid() == 0) {
    my @pwinfo = POSIX::getpwnam('oracle');
    POSIX::setuid($pwinfo[2]); 
  } 
}

1;
