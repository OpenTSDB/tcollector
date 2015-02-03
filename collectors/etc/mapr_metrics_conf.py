# Before you enable the mapr_metrics collector, create the metrics: 
# tsdb mkmetric mapr.disks.READBYTES mapr.disks.WRITEOPS mapr.disks.WRITEBYTES mapr.disks.READOPS mapr.cpus.CPUTOTAL mapr.cpus.CPUIDLE mapr.cpus.CPUIOWAIT mapr.network.PKTSOUT mapr.network.BYTESOUT mapr.network.PKTSIN mapr.network.BYTESIN mapr.memory.used mapr.mfs.available mapr.mfs.used
#
enabled = True
webserver = "localhost"
port = 8443
no_ssl = False
username = "metrics"
password = "maprmetrics"
