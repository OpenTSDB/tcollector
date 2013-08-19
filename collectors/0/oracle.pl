#!/usr/bin/perl -w
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

use strict;
use DBI;
use DBD::Oracle qw(:ora_session_modes);

# Include configuration module from ../etc
use FindBin qw($Bin);
use lib "$Bin/../etc";
use oracleconf qw(oracle_config switch_to_dba);

# Switch to user with DBA privileges if running as root
switch_to_dba();

# Flush STDOUT immediately
$|=1;

# How many seconds to sleep between queries
use constant SLEEP_SECONDS => 15;

# Used for debugging
use constant DEBUG => 0;

# Statistics to collect from V$SYSSTAT. Format is
# stat_id => [oracle name, metric name, tags, [array of stat_ids to subtract values from this value]]
my %sysstat = (
  2450922463 => ["background checkpoints completed","oracle.checkpoints","",[],
          "Number of checkpoints completed by the background process"],
  161936656  => ["bytes received via SQL*Net from client","oracle.net.received","type=client",[],
          "Total number of bytes received from the client over Oracle Net Services"],
  1983609624 => ["bytes received via SQL*Net from dblink","oracle.net.received","type=dblink",[],
          "Total number of bytes received from a database link over Oracle Net Services"],
  2967415760 => ["bytes sent via SQL*Net to client","oracle.net.sent","type=client",[],
          "Total number of bytes sent to the client from the foreground processes"],
  1622773540 => ["bytes sent via SQL*Net to dblink","oracle.net.sent","type=dblink",[],
          "Total number of bytes sent over a database link"],
  159846526  => ["SQL*Net roundtrips to/from client","oracle.net.roundtrips","type=client",[],
          "Total number of Oracle Net Services messages sent to and received from the client"],
  2420448902 => ["SQL*Net roundtrips to/from dblink","oracle.net.roundtrips","type=dblink",[],
          "Total number of Oracle Net Services messages sent over and received from a database link"],
  3876379665 => ["consistent changes","oracle.block.consistent_changes","",[],
          qq|Number of times a user process has applied rollback entries to perform a consistent read on the block.
             Work loads that produce a great deal of consistent changes can consume a great deal of resources. 
             The value of this statistic should be small in relation to the "consistent gets" statistic.|],
  420374750  => ["consistent gets direct","oracle.block.gets","class=consistent type=direct",[],
          qq|Number of times a consistent read was requested for a block bypassing the buffer cache 
             (for example, direct load operation)|],
  2839918855 => ["consistent gets from cache","oracle.block.gets","class=consistent type=cache",[],
          "Number of times a consistent read was requested for a block from buffer cache"],
  516801181  => ["CR blocks created","oracle.cr_blocks.created","",[],
          qq|Number of CURRENT blocks cloned to create CR (consistent read) blocks. 
             The most common reason for cloning is that the buffer is held in a incompatible mode|],
  3142330676 => ["current blocks converted for CR","oracle.cr_blocks.converted","",[],
          "Number CURRENT blocks converted to CR state"],
  916801489  => ["db block changes","oracle.block.changes","",[],
          qq|Closely related to "consistent changes", this statistic counts the total number of changes 
             that were part of an update or delete operation that were made to all blocks in the SGA. 
             Such changes generate redo log entries and hence become permanent changes to the database 
             if the transaction is committed.This approximates total database work|],
  95128520   => ["db block gets direct","oracle.block.gets","class=current type=direct",[],
          qq|Number of times a CURRENT block was requested bypassing the buffer cache 
             (for example, a direct load operation)|],
  4017839461 => ["db block gets from cache","oracle.block.gets","class=current type=cache",[],
          "Number of times a CURRENT block was requested from the buffer cache"],
  1064154723 => ["db corrupt blocks detected","oracle.corrupt_blocks","",[],
          "Number of corrupt blocks detected. If this is not 0, your hardware has issues"],
  3411924934 => ["free buffer requested","oracle.buffer.requested","",[],
          "Number of times a reusable buffer or a free buffer was requested to create or load a block"],
  1344569897 => ["dirty buffers inspected","oracle.buffer.inspected","type=dirty",[],
          "Number of dirty buffers found by the user process while it is looking for a buffer to reuse"],
  833456521  => ["pinned buffers inspected","oracle.buffer.inspected","type=pinned",[],
          qq|Number of times a user process, when scanning the tail of the replacement list looking 
             for a buffer to reuse, encountered a cold buffer that was pinned or had a waiter that 
             was about to pin it. This occurrence is uncommon, because a cold buffer should not be pinned very often|],
  472183780  => ["enqueue deadlocks","oracle.enqueue.deadlocks","",[],
          "Total number of deadlocks between table or row locks in different sessions"],
  2440542518 => ["enqueue requests","oracle.enqueue.requests","",[],
          "Total number of table or row locks acquired"],
  2425496215 => ["enqueue timeouts","oracle.enqueue.timeouts","",[],
          "Total number of table and row locks (acquired and converted) that timed out before they could complete"],
  2307006529 => ["enqueue waits","oracle.enqueue.waits","",[],
          "Total number of waits that occurred during an enqueue convert or get because the enqueue get was deferred"],
  2453370665 => ["execute count","oracle.executes","",[],
          "Total number of calls (user and recursive) that executed SQL statements"],
  973553265  => ["flashback log write bytes","oracle.flashback.bytes","",[],
          "Total size in bytes of flashback database data written by RVWR to flashback database logs"],
  3123176560 => ["flashback log writes","oracle.flashback.writes","",[],
          "Total number of writes by RVWR to flashback database logs"],
  12081473   => ["index fast full scans (full)","oracle.index_ffs","",[],
          "Number of fast full scans initiated for full segments"],
  3626914479 => ["lob reads","oracle.lob.reads","",[],
          qq|Number of LOB API read operations performed in the session/system. A single LOB API read 
             may correspond to multiple physical/logical disk block reads|],
  2682192071 => ["lob writes","oracle.lob_writes","",[],
          qq|Number of LOB API write operations performed in the session/system. A single LOB API write 
             may correspond to multiple physical/logical disk block writes|],
  2666645286 => ["logons cumulative","oracle.logons.cumulative","",[],
         "Total number of logons since the instance started"],
  3080465522 => ["logons current","oracle.logons.current","",[],
         "Total number of current logons"],
  85052502   => ["opened cursors cumulative","oracle.cursors.opened","",[],
         "Total number of cursors opened since the instance started"],
  2301954928 => ["opened cursors current","oracle.cursors.current","",[],
         "Total number of current open cursors"],
  143509059  => ["parse count (hard)","oracle.parse_count","type=hard",[],
          qq|Total number of parse calls (real parses). A hard parse is a very expensive operation 
             in terms of memory use, because it requires Oracle to allocate a workheap and other 
             memory structures and then build a parse tree|],
  1118776443 => ["parse count (failures)","oracle.parse_count","type=failure",[],
          "Parse failures"],
  63887964   => ["parse count (total)","oracle.parse_count","type=soft",[143509059,1118776443],
          qq|Total number of parse calls (hard, soft, and describe). A soft parse is a check on an 
             object already in the shared pool, to verify that the permissions on the 
             underlying object have not changed|],
  2263124246 => ["physical reads","oracle.block.read","type=private",[4171507801,2589616721],
          qq|Total number of data blocks read from disk. This value can be greater than the value 
             of "physical reads direct" plus "physical reads cache" as reads into process private 
             buffers also included in this statistic|],
  4171507801 => ["physical reads cache","oracle.block.read","type=cache",[],
          "Total number of data blocks read from disk into the buffer cache"],
  2589616721 => ["physical reads direct","oracle.block.read","type=direct",[],
          "Number of reads directly from disk, bypassing the buffer cache"],
  2699895516 => ["physical writes direct","oracle.block.write","type=direct",[],
          "Number of writes directly to disk, bypassing the buffer cache (as in a direct load operation)"],
  163083034  => ["physical writes from cache","oracle.block.write","type=cache",[],
          "Total number of data blocks written to disk from the buffer cache"],
  523531786  => ["physical read bytes","oracle.io.read_bytes","type=application",[],
          "Total size in bytes of all disk reads by application activity (and not other instance activity) only"],
  2572010804 => ["physical read total bytes","oracle.io.read_bytes","type=system",[523531786],
          qq|Total size in bytes of disk reads by all database instance activity including application reads, 
             backup and recovery, and other utilities. The difference between this value and "physical read bytes" 
             gives the total read size in bytes by non-application workload|],
  789768877  => ["physical read IO requests","oracle.io.read_requests","type=application",[],
          qq|Number of read requests for application activity (mainly buffer cache and direct load operation) 
             which read one or more database blocks per request|],
  3343375620 => ["physical read total IO requests","oracle.io.read_requests","type=system",[789768877],
          qq|Number of read requests which read one or more database blocks for all instance activity 
             including application, backup and recovery, and other utilities|],
  3131337131 => ["physical write bytes","oracle.io.write_bytes","type=application",[],
          "Total size in bytes of all disk writes by application activity (and not other instance activity) only"],
  2495644835 => ["physical write total bytes","oracle.io.write_bytes","type=system",[3131337131],
          qq|Total size in bytes of all disk writes for the database instance including application activity, 
             backup and recovery, and other utilities. The difference between this value and "physical write bytes" 
             gives the total write size in bytes by non-application workload|],
  2904164198 => ["physical write IO requests","oracle.io.write_requests","type=application",[],
          qq|Number of write requests for application activity (mainly buffer cache and direct load operation)
             which wrote one or more database blocks per request|],
  1315894329 => ["physical write total IO requests","oracle.io.write_requests","type=system",[2904164198],
          qq|Number of write requests which wrote one or more database blocks for all instance activity
             including application, backup and recovery, and other utilities|],
  1236385760 => ["redo size","oracle.io.write_bytes","type=redo",[],
          "Total amount of redo generated in bytes"],
  1948353376 => ["redo writes","oracle.io.write_requests","type=redo",[],
          "Total number of writes by LGWR to the redo log files"],
  3488821837 => ["redo entries","oracle.redo.entries","",[],
          "Number of times a redo entry is copied into the redo log buffer"],
  1985754937 => ["redo log space requests","oracle.redo.space_requests","",[],
          qq|Number of times the active log file is full and Oracle must wait for disk space to be 
             allocated for the redo log entries. Such space is created by performing a log switch|],
  252430928  => ["redo log space wait time","oracle.redo.space_wait","",[],
          qq|Total elapsed waiting time for "redo log space requests" in 10s of milliseconds|],
  4215815172 => ["redo synch time","oracle.redo.sync_time","",[],
          qq|Elapsed time of all "redo synch writes" calls in 10s of milliseconds|],
  1439995281 => ["redo synch writes","oracle.redo.sync_writes","",[],
          "Number of times the redo is forced to disk, usually for a transaction commit"],
  4148600571 => ["session pga memory","oracle.session.memory","type=pga",[],
          "PGA usage for all sessions"],
  1856888586 => ["session uga memory","oracle.session.memory","typpe=uga",[],
          "UGA usage for all sessions"],
  2533123502 => ["sorts (disk)","oracle.sort.count","type=disk",[],
          "Number of sort operations that required at least one disk write"],
  2091983730 => ["sorts (memory)","oracle.sort.count","type=memory",[],
          "Number of sort operations that were performed completely in memory and did not require any disk writes"],
  3757672740 => ["sorts (rows)","oracle.sort.rows","",[],
          "Total number of rows sorted"],
  3741388076 => ["table scan blocks gotten","oracle.table_scan.blocks","",[],
          qq|During scanning operations, each row is retrieved sequentially by Oracle. This statistic 
             counts the number of blocks encountered during the scan|],
  1400824662 => ["table scan rows gotten","oracle.table_scan.rows","",[],
          "Number of rows that are processed during scanning operations"],
  681815839  => ["table fetch by rowid","oracle.table_fetch_by_rowid","",[],
          "Number of rows that are fetched using a ROWID (usually recovered from an index)"],
  1413702393 => ["table fetch continued row","oracle.table_fetch_chained_row","",[],
          qq|Number of times a chained or migrated row is encountered during a fetch.
             Retrieving rows that span more than one block increases the logical I/O by a factor 
             that corresponds to the number of blocks than need to be accessed|],
  2882015696 => ["user calls","oracle.user.calls","",[],
          "Number of user calls such as login, parse, fetch, or execute"],
  582481098  => ["user commits","oracle.user.commits","",[],
          qq|Number of user commits. When a user commits a transaction, the redo generated that 
             reflects the changes made to database blocks must be written to disk. Commits often 
             represent the closest thing to a user transaction rate|]
); 

# Some stats in v$sysstat are in "10s of milliseconds". This is used to convert
# to seconds
my %stat_multipliers = (
252430928  => 0.01,
4215815172 => 0.01
);


my ($connections,$sid,$home,$instance_count,$report_instance_tag,$connection_info);
my ($start, $elapsed);

# Get configuration from oracleconf.pm
my $oracle_config = oracle_config();

$instance_count = 0;
while (($sid,$home) = each(%$oracle_config)) {
  my %connection_info;
  $connection_info{ORACLE_HOME} = $home;
  $connection_info{DBH} = undef;
  $connection_info{STATEMENTS} = undef;
  $connections->{$sid} = \%connection_info;
  $instance_count++;
}

# Return if there is nothing to collect stats from
exit(13) if !$instance_count;

# If there is only 1 instance on the box, no need to use instance tag
$report_instance_tag = $instance_count > 1;

my $killed = 0;

# Trap signals
$SIG{INT}  = \&kill;
$SIG{TERM} = \&kill;

# Main loop
while (!$killed) {
  $start = time();
  maintain_connections($connections);
  
  while (($sid,$connection_info) = each(%$connections)) {
    next if !$connection_info->{DBH};
    if ($report_instance_tag) {
       collect_stats($connection_info->{STATEMENTS}[0],$connection_info->{STATEMENTS}[1],$sid); 
    } else {
       collect_stats($connection_info->{STATEMENTS}[0],$connection_info->{STATEMENTS}[1],0); 
    }
  }
 
  $elapsed = time() - $start;
  sleep(SLEEP_SECONDS - $elapsed);
}

# Disconnect
while (($sid,$connection_info) = each(%$connections)) {
   ($connection_info->{DBH})->disconnect() if $connection_info->{DBH};
}


# Maintain connections
sub maintain_connections {

  my $connections = shift;

  my $dbh;

  while (($sid,$connection_info) = each(%$connections)) {

    # Create connection if its not created
    if (!$connection_info->{DBH}) {

      $ENV{"ORACLE_SID"} = $sid;
      $ENV{"ORACLE_HOME"} = $connection_info->{ORACLE_HOME};

      $dbh = DBI->connect("DBI:Oracle:","","",
            {ora_session_mode => ORA_SYSDBA,
             RaiseError => 0,
             AutoCommit=>0,
             PrintError=>0});
       if (!$dbh) {
         print STDERR "Can not connect to the instance $sid: $DBI::errstr\n";
         next;
       }
       my @statements = prepare_statements($dbh);
       $connection_info->{DBH} = $dbh;
       $connection_info->{STATEMENTS} = \@statements;
       next;
    } else {
      # Ping connection
      if (!$connection_info->{DBH}->ping()) {
        print STDERR "Lost connection to $sid\n";  
        undef $connection_info->{DBH};
      }
    }
  }

}

# Parse and prepare SQL
sub prepare_statements {

  my $dbh = shift;
  my ($sql,$stat_sth,$wait_sth);

  # Fetch data from v$sysstat
  $sql = q{
  select stat_id, name, value
  from V$SYSSTAT
  };

  $stat_sth = $dbh->prepare($sql);

  # Fetch data from v$system_event
  $sql = q{
  select
    decode(wait_class,
      'Concurrency','concurrency',
      'User I/O','user_io',
      'System I/O','system_io',
      'Administrative','admin',
      'Configuration','config',
      'Other','other',
      'Application','application',
      'Queueing','queueing',
      'Network','network',
      'Commit','commit',
      'unknown') wait_class,
    sum(total_waits) total_waits,
    sum(total_timeouts) total_timeouts,
    sum(time_waited) time_waited
  from V$SYSTEM_EVENT
  where wait_class != 'Idle'
  group by wait_class
  };

  $wait_sth = $dbh->prepare($sql);

  return ($stat_sth,$wait_sth);

}

# Collect stats from particular instance
sub collect_stats {

  my ($stat_sth,$wait_sth,$instance_tag) = @_;

  # Add instance tag if requested
  $instance_tag = $instance_tag ? "instance=$instance_tag" : "" ;

  my ($row,%stat_values,$stat_id,@stats_to_subtract,$stat_to_subtract,$current_time);

  $current_time = time();

  # Process stats
  $stat_sth->execute() or return(0);
  while ($row = $stat_sth->fetchrow_hashref()) {
    next if !$sysstat{$row->{STAT_ID}};
    if (DEBUG && ($sysstat{$row->{STAT_ID}}[0] ne $row->{NAME})) {
      print STDERR "Warning: stat $row->{STAT_ID} name $row->{NAME} doesn't match name in control table\n";
    }
    $stat_values{$row->{STAT_ID}} = $row->{VALUE};
  }

  # Some stats need to be calculated by subtracting other stats from the total. 
  # For example, Oracle provides total number of parses, then number of hard, describe and failure parses.
  # So to calculate soft parses we need to subtract those from the total. This way we can use the same
  # metric for all parses and let OpenTSDB calculate total. 
  foreach $stat_id (keys %stat_values) {
    @stats_to_subtract = @{$sysstat{$stat_id}[3]};
    if (@stats_to_subtract) {
      foreach $stat_to_subtract (@stats_to_subtract) {
        if ($stat_values{$stat_to_subtract}) {
          $stat_values{$stat_id} -= $stat_values{$stat_to_subtract};
          print STDERR "Subtracted <$sysstat{$stat_to_subtract}[0]> from <$sysstat{$stat_id}[0]>\n" if DEBUG;
        } else {
          print STDERR "Tried to subtract unpopulated <$sysstat{$stat_to_subtract}[0]> from <$sysstat{$stat_id}[0]>\n" if DEBUG;
        }
      }
    }
  }

  #Some stats are presented in 10s of milliseconds, need to convert those to seconds
  foreach $stat_id (keys %stat_multipliers) {
    if ($stat_values{$stat_id}) {
      $stat_values{$stat_id} *= $stat_multipliers{$stat_id};
      print STDERR "Multiplying $sysstat{$stat_id}[0] value by $stat_multipliers{$stat_id}\n" if DEBUG;
    }
  }

  # Dump stats
  foreach $stat_id (sort keys %stat_values) {
    print("$sysstat{$stat_id}[1] $current_time $stat_values{$stat_id} $sysstat{$stat_id}[2] $instance_tag\n");
  }

  # Dump waits
  $wait_sth->execute() or return(0);
  while ($row = $wait_sth->fetchrow_hashref()) {
    print("oracle.wait.waits $current_time $row->{TOTAL_WAITS} class=$row->{WAIT_CLASS} $instance_tag\n");
    print("oracle.wait.timeouts $current_time $row->{TOTAL_TIMEOUTS} class=$row->{WAIT_CLASS} $instance_tag\n");
    print("oracle.wait.time_waited $current_time $row->{TIME_WAITED} class=$row->{WAIT_CLASS} $instance_tag\n");
  }
  
  return 1;
}

# Handle KILL signal
sub kill {
  $killed = 1;
}

