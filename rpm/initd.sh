#!/bin/bash
#
# tcollector        Startup script for the tcollector monitoring agent
#
# chkconfig: - 15 85
# description: tcollector is an agent that collects and reports  \
#              monitoring data for OpenTSDB.
# processname: tcollector
# pidfile: /var/run/tcollector.pid
#
### BEGIN INIT INFO
# Provides: tcollector
# Required-Start: $local_fs $remote_fs $network $named
# Required-Stop: $local_fs $remote_fs $network
# Short-Description: start and stop tcollector monitoring agent
# Description: tcollector is an agent that collects and reports
#  monitoring data for OpenTSDB.
### END INIT INFO

# Source function library.
. /etc/init.d/functions

TSD_HOST=tsd
THIS_HOST=`hostname`
THIS_HOST=${THIS_HOST%%.*}
TCOLLECTOR=${TCOLLECTOR-usr/local/tcollector/tcollector.py}
PIDFILE=${PIDFILE-/var/run/tcollector.pid}
LOGFILE=${LOGFILE-/var/log/tcollector.log}
NUMLOGFILES=3

prog=tcollector
if [ -f /etc/sysconfig/$prog ]; then
  . /etc/sysconfig/$prog
fi

lockfile=${LOCKFILE-/var/lock/subsys/tcollector}

if [ -z "$OPTIONS" ]; then
  OPTIONS="-H $TSD_HOST -t host=$THIS_HOST -P $PIDFILE"
  OPTIONS="$OPTIONS --logfile=$LOGFILE --backup-count=$NUMLOGFILES"
fi

sanity_check() {
  for i in "$PIDFILE" "$LOG"; do
    # If the file doesn't exist, check that we have write access to its parent
    # directory to be able to create it.
    test -e "$i" || i=`dirname "$i"`
    test -w "$i" || {
      echo >&2 "error: Cannot write to $i"
      return 4
    }
  done
}

start() {
  echo -n $"Starting $prog: "
  sanity_check || return $?
  daemon --pidfile=$PIDFILE $TCOLLECTOR $OPTIONS
  RETVAL=$?
  echo
  [ $RETVAL = 0 ] && touch ${lockfile}
  return $RETVAL
}

# When stopping tcollector a delay of ~15 seconds before SIGKILLing the
# process so as to give enough time for tcollector to SIGKILL any errant
# collectors.
stop() {
  echo -n $"Stopping $prog: "
  sanity_check || return $?
  killproc -p $PIDFILE -d 15 $TCOLLECTOR
  RETVAL=$?
  echo
  [ $RETVAL = 0 ] && rm -f ${lockfile} $PIDFILE
}

# See how we were called.
case "$1" in
  start) start;;
  stop) stop;;
  status)
    status -p $PIDFILE $TCOLLECTOR
    RETVAL=$?
    ;;
  restart|force-reload|reload) stop && start;;
  condrestart|try-restart)
    if status -p $PIDFILE $TCOLLECTOR >&/dev/null; then
      stop && start
    fi
    ;;
  *)
    echo $"Usage: $prog {start|stop|status|restart|force-reload|reload|condrestart|try-restart}"
    RETVAL=2
esac

exit $RETVAL
