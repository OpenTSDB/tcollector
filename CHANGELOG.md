# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [1.3.1-SNAPSHOT](https://github.com/OpenTSDB/tcollector/issues?utf8=%E2%9C%93&q=milestone%3A1.3.1+)
### Collectors Added
- docker.py - Pulls metrics from a local Docker instance, tries /var/run/docker.sock, then localhost API

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
