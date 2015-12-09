#!/usr/bin/env python

def getUserPassword():
	return ("DBUser", "DBPassword", "DBHost")

def getKeyMap():
	"""
	You can use everything that is displayed
	when you call "SHOW STATUS LIKE '%wsrep%'"
	on your DB-host
	"""
	myMap = (
        	"wsrep_last_committed",
	        "wsrep_replicated",
		"wsrep_repl_keys",
	        "wsrep_local_commits",
		"wsrep_received",
	        "wsrep_local_send_queue_avg",
	        "wsrep_local_recv_queue_avg"
	)
	return myMap

def getGaleraFile():
	"""
	Used for ensuring that Percona XtraDB Cluster is installed
	and not a common MySQL-Server
	"""
	return "/usr/lib/libgalera_smm.so"

def getInterval():
	""" Interval in seconds """
	return 1

def getPrefix():
	return "pxc"
