# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [1.3.4](https://github.com/OpenTSDB/tcollector/issues?q=is%3Aopen+is%3Aissue+milestone%3A1.3.4)


## [1.3.3](https://github.com/OpenTSDB/tcollector/issues?utf8=%E2%9C%93&q=milestone%3A1.3.3)

### Support
- Dropped Support for Python 2.x, <=3.4.x

### Improvements
- A namespace prefix can be added to all metrics. [#434](https://github.com/OpenTSDB/tcollector/pull/434)
- An optional status monitoring API, serving JSON over HTTP [#422](https://github.com/OpenTSDB/tcollector/pull/422)
- Command-line options can be configured using an optional `/etc/tcollector.json` configuration file. [#433](https://github.com/OpenTSDB/tcollector/pull/433)
- Boolean values True, true, False, and false, are now converted to 1,1,0 and 0 respectively. [No PR #, I merged it to master directly...sorry....](https://github.com/OpenTSDB/tcollector)
- collectors/0/nfsstat: correct list of nfs client names [#374](https://github.com/OpenTSDB/tcollector/pull/374)
- Adding two TCP stats to netstat.py [#419](https://github.com/OpenTSDB/tcollector/pull/419)
- Add info on the cpu sets to procstats [#432](https://github.com/OpenTSDB/tcollector/pull/432)
- Ignore kubernetes mounts in dfstat.py [#418](https://github.com/OpenTSDB/tcollector/pull/418)
- Adding ncat usage option to opentsdb.sh  [#420](https://github.com/OpenTSDB/tcollector/pull/420)
- updated HBase ports for modern versions of HBase (0.99+ @ 2014/2015) [#394](https://github.com/OpenTSDB/tcollector/pull/394)
- iostat - Generate write_bytes and read_bytes metrics  enhancement [#326](https://github.com/OpenTSDB/tcollector/pull/326)
- allow dates past Sept 2020 [#405](https://github.com/OpenTSDB/tcollector/pull/405)
- additonial Python compatibility [#396](https://github.com/OpenTSDB/tcollector/pull/396) [#411](https://github.com/OpenTSDB/tcollector/pull/411) [#218](https://github.com/OpenTSDB/tcollector/pull/218)
- Added mysqlclient module [#383](https://github.com/OpenTSDB/tcollector/pull/383)

### Collectors Added
- Collector for MapR Hadoop node metrics [#281](https://github.com/OpenTSDB/tcollector/pull/281)
- New Collectors for Hadoop Yarn Resource Manager and Node Manager JMX API Stats [#400](https://github.com/OpenTSDB/tcollector/pull/400)
- netfilter stats [#354](https://github.com/OpenTSDB/tcollector/pull/354)
- Added postgresql_replication collector. [#323](https://github.com/OpenTSDB/tcollector/pull/323)
- Adding buddying memory fragmentation and slab info stats for tcollector [#318](https://github.com/OpenTSDB/tcollector/pull/318)
- Added mountstats collector [#322](https://github.com/OpenTSDB/tcollector/pull/322)

### Bugfixes
- tcollector daemon hangs and becomes unresponsive. [#378](https://github.com/OpenTSDB/tcollector/pull/378)
- If response code from OpenTSDB is 400, don't retry sending since this means we sent bad data. [#436](https://github.com/OpenTSDB/tcollector/pull/436)
- Small data collection validation refinements [#431](https://github.com/OpenTSDB/tcollector/pull/431)
- fix elasticsearch sending booleans to opentsdb  [#429](https://github.com/OpenTSDB/tcollector/pull/429)
- Fix order of checking if value is bool  [#428](https://github.com/OpenTSDB/tcollector/pull/428)
- add flush to zookeeper collector agent [#427](https://github.com/OpenTSDB/tcollector/pull/427)
- correctly dedup timestamps in milleseconds [#440](https://github.com/OpenTSDB/tcollector/pull/440)
- fix proc status [#425](https://github.com/OpenTSDB/tcollector/pull/425)
- Hbase metric lost due to coding problems In python2 [#439](https://github.com/OpenTSDB/tcollector/pull/439)
- hadoop_http.py - Fixed unicode issue [#437](https://github.com/OpenTSDB/tcollector/pull/437)
- hbase regionserver collector "Error splitting numRegions" [#396](https://github.com/OpenTSDB/tcollector/pull/396)
- added fix for #398 to flush metrics after each emit_metric [#399](https://github.com/OpenTSDB/tcollector/pull/399)
- Fix for multithreaded HAProxy (since HAProxy 1.8) [#404](https://github.com/OpenTSDB/tcollector/pull/404)
- Fix memory leak around timestamp precision adjust [#408](https://github.com/OpenTSDB/tcollector/pull/408)
- Check counter names exist before referencing. [#377](https://github.com/OpenTSDB/tcollector/pull/377) [#376](https://github.com/OpenTSDB/tcollector/pull/376)

## [1.3.1](https://github.com/OpenTSDB/tcollector/issues?utf8=%E2%9C%93&q=milestone%3A1.3.1+)
### Collectors Added
- docker.py - Pulls metrics from a local Docker instance, tries /var/run/docker.sock, then localhost API
- pxc-collector.py - Added Percona XtraDB Cluster Collector [#301](https://github.com/OpenTSDB/tcollector/pull/301)
- mongo3.py - Added MongoDB 3 Collector [#302](https://github.com/OpenTSDB/tcollector/pull/302)

### Bugfixes
- startstop - Fixed issue where host was still required [#291](https://github.com/OpenTSDB/tcollector/pull/291)
- tcollector.py - Fixed default pid location [#299](https://github.com/OpenTSDB/tcollector/pull/299)
- tcollector.py - Few bugs related to new configuration [#305(https://github.com/OpenTSDB/tcollector/pull/305) [#306](https://github.com/OpenTSDB/tcollector/pull/306) [#307](https://github.com/OpenTSDB/tcollector/pull/307)
- tcollector.py - Fixed issues with default cdir and classpath on Debian [#308](https://github.com/OpenTSDB/tcollector/pull/308)
- tcollector.py - Fixed issue with last_datapoint for longer running collectors [#309](https://github.com/OpenTSDB/tcollector/pull/309)

### Improvements
- Improved ZFS Iostat collector [#294](https://github.com/OpenTSDB/tcollector/pull/294)
- Avoid picking new connction with just one host [#295](https://github.com/OpenTSDB/tcollector/pull/295)
- Improved CPI pctusage [#298](https://github.com/OpenTSDB/tcollector/pull/298)
- Fixed CGROUP path for EL7 [#304](https://github.com/OpenTSDB/tcollector/pull/304)


## [1.3.0](https://github.com/OpenTSDB/tcollector/issues?utf8=%E2%9C%93&q=milestone%3A1.3.0)
### Collectors Added
- rtt.py - Pings a configured set of hosts and records the result [#183](https://github.com/OpenTSDB/tcollector/pull/183)
- aws_cloudwatch.py - Collects metrics from AWS Cloudwatch [#246](https://github.com/OpenTSDB/tcollector/pull/246)
- tcp_bridge.py - Listens on a TCP port for metrics to relay to configured hosts [#252](https://github.com/OpenTSDB/tcollector/pull/252)
- ntp.py - Gathers offset of clock from NTP [#265](https://github.com/OpenTSDB/tcollector/pull/265)
- mapr_metrics.py - Gathers MAPR metrics for Hadoop nodes [#196](https://github.com/OpenTSDB/tcollector/pull/196)
- tcollector.py - Gathers metrics on CPU and Memory usage of TCollector and the collectors [#276](https://github.com/OpenTSDB/tcollector/pull/276)

### Bugfixes
- zookeper.py [#221](https://github.com/OpenTSDB/tcollector/pull/221)
- mysql.py [#227](https://github.com/OpenTSDB/tcollector/pull/227) [#178](https://github.com/OpenTSDB/tcollector/pull/178)
- zfsiostats.py [#256](https://github.com/OpenTSDB/tcollector/pull/256)

### Core TCollector Features
- Support for HTTP API [#225](https://github.com/OpenTSDB/tcollector/issues/225)
- Support for MS precision [#230](https://github.com/OpenTSDB/tcollector/issues/230) [#234](https://github.com/OpenTSDB/tcollector/pull/234)
- Support TSD_HOSTS comma delimited host list rather than TSD_HOST/TSD_PORT [#237](https://github.com/OpenTSDB/tcollector/pull/237)
- RPM Packages now have base TCollector package, Collectors Package and an EOS specific package [#244](https://github.com/OpenTSDB/tcollector/pull/244)
- Improved FreeBSD compatiblity [#256](https://github.com/OpenTSDB/tcollector/pull/256) [#257](https://github.com/OpenTSDB/tcollector/pull/257) [#258](https://github.com/OpenTSDB/tcollector/pull/258) [#263](https://github.com/OpenTSDB/tcollector/pull/263)
- TCollector and all collectors now using '/usr/bin/env python' rather than '/usr/bin/python' [#263](https://github.com/OpenTSDB/tcollector/pull/263)
- Elasticsearch Collector now allows multiple+configurable targets [#207](https://github.com/OpenTSDB/tcollector/pull/207)
- TCollector now reads default values from the configuration [#287](https://github.com/OpenTSDB/tcollector/pull/287)

### Packaging Changes
- The RPM no longer starts or enables TCollector on installation
- Enable RPM subpackages. Allows you to create 3 packages
  - A base package with the just the tcollector
  - A collectors subpackage with just collectors
  - An eos subpackage with EOS-only collectors (depends on EosSDK)
- %{rootdir} was hardcoded instead of using the pwd passed to %{_srcrpmdir}.
- The sub-packages were missing a dependency on the main package.

## [1.2.0] - 2015-05
### Initial Baseline
- This is the current release, so the CHANGELOG is from here forward.
