#!/bin/bash
# Tcollector to do some TCP analysis for region usage in HBase
# Created by: Geoffrey Anderson <geoff@box.com>
# Created on: 2013-03-26
# Updated by: Geoffrey Anderson <geoff@box.com>
# Updated on: 2013-03-27


####
# Variables you can change
####
# Name of the metric to use for this. There will automatically be tags for "table" and "region"
metric_name='hadoop.hbase.regionserver.regions.requests'
# duration to capture tcpdump data in seconds
sleep_time=11



####
# Additional variables for this script, change if necessary
####
tmp_dir='/tmp'
lock_file="${self}.lockfile"
tcpdump_raw_file="${self}.tcpdump.raw"
tcpdump_temp_file="${self}.tcpdump.out"
result_file="${self}.result.txt"
self="$(basename $0)"
hbase_tables=''



####
# Functions
####

# Quick and easy function to cleanup any files/directories made by this collector
cleanup_collector()
{
	rm -f "${tmp_dir}/${result_file}" "${tmp_dir}/${lock_file}" "${tmp_dir}/${tcpdump_temp_file}" "${tmp_dir}/${tcpdump_raw_file}"

}

# Function to get a list of hbase tables by parsing the output of 'list' from the hbase shell
get_hbase_tables()
{
	start_re='^TABLE'
	end_re='[0-9][0-9]* row.s. in'
	table_line=0
	while read -r line
	do
		if [[ "$line" =~ $start_re ]]
		then
			table_line=1
			continue
		elif [[ "$line" =~ $end_re ]]
		then
			table_line=0
			break
		fi

		if (( $table_line == 1 ))
		then
			if [[ -z "$hbase_tables" ]]
			then
				hbase_tables="$line"
			else
				hbase_tables="${hbase_tables}|${line}"
			fi
		fi
	done < <(echo "list" | hbase shell 2>/dev/null)
}



####
# Script start!
####

# check lock file
if [[ -e "${tmp_dir}/${lock_file}" ]]
then
	echo "${self}: lock file ${lock_file} already exists, aborting"
	exit 1
fi

# Set a trap for if the script is killed before the wait time is over
trap 'rm -f "${tmp_dir}/${lock_file}"; exit' INT TERM EXIT
touch "${tmp_dir}/${lock_file}"

# Populate the hbase_tables variable
get_hbase_tables


# Start on a clock tick interval for this collection
current_time="$(date +%s)"
next_time="$( echo "${current_time} 10" | awk '{ print (int( $1/$2)+1)*$2 }' )"
let wait_time=($next_time-$current_time-1)

if (( $wait_time < 0 ))
then
	wait_time=9
fi

#echo "waiting for $wait_time"
sleep $wait_time

# set trap to be sure tcpdump doesn't run for ever and clean up the temp file too
trap 'rm -f "${tmp_dir}/${lock_file}"; kill $tcpdump_pid; rm -f "${tmp_dir}/${tcpdump_temp_file}"; rm -f "${tmp_dir}/${tcpdump_raw_file}" ;exit' INT TERM EXIT

# run the tcpdump, write to file, and sleep for a bit
tcpdump -nni any -s 0 -l -w "${tmp_dir}/${tcpdump_raw_file}" 'port 60020' 2>/dev/null &

tcpdump_pid="$!"
sleep $sleep_time
kill $tcpdump_pid

# Run the output file through strings and then break new lines on table names
cat "${tmp_dir}/${tcpdump_raw_file}" 2>/dev/null | strings | perl -e 'while (<>) {
	chomp;
	if(/('"$hbase_tables"')/) {
		if (defined $q) {
			print "$q\n";
		}
		$q=$_;
	}
	else {
		$_ =~ s/^[ \t]+//;
		$q.=" $_";
	}
}' > "${tmp_dir}/${tcpdump_temp_file}" 2>/dev/null

# set trap to be sure both remote files are removed
trap 'rm -f "${tmp_dir}/${result_file}" "${tmp_dir}/${lock_file}" "${tmp_dir}/${tcpdump_temp_file}" "${tmp_dir}/${tcpdump_raw_file}" ;exit' INT TERM EXIT

# if the ASCII version of the tcpdump is empty, bail out
if [[ ! -s "${tmp_dir}/${tcpdump_temp_file}" ]]
then
	cleanup_collector
	exit 0
fi

# Using a quick one-liner (heh..:)) aggregate hbase table and region names into a perl hash and the counts for them.
# Write this hash into a file
cat "${tmp_dir}/${tcpdump_temp_file}" | perl -e 'my %cntHash = (); while (<>) { if(/('"$hbase_tables"'),[^,]+,[0-9]+\.([a-z0-9]+)\./) { if(!$cntHash{"$1,$2"}) { $cntHash{"$1,$2"} = 0; } $cntHash{"$1,$2"} += 1; } else { next; } } ; while(my ($region, $cnt) = each(%cntHash)) { print "$cnt $region\n"; }' > "${tmp_dir}/${result_file}"

# Read the file from the above perl work and pluck out hbase table names and region names for use in a collector output.
while read -r cnt table_region
do
	echo "${metric_name} ${current_time} ${cnt} table=${table_region%%,*} region=${table_region##*,} port=60020"
done < "${tmp_dir}/${result_file}"

# clean up files
cleanup_collector

trap - INT TERM EXIT
exit 0
