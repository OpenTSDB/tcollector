%post
if [ "$1" = 1 ]; then
  chkconfig --add tcollector
fi
%preun
if [ "$1" = 0 ]; then
  service tcollector stop
  chkconfig tcollector off
  chkconfig --del tcollector
fi
