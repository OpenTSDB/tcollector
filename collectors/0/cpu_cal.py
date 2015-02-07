import os
import time
import sys 
import multiprocessing
from collectors.lib import utils
host=os.popen('hostname -f').read()

metric1="user.percentage"
metric2="nice.percentage"
metric3="system.percentage"
metric4="idle.percentage"
metric5="iowait.percentage"
metric6= "irq.percentage"
metric7="softirq.percentage"
metric8="steal.percentage"
metric9="guest.percentage"
metric10="guest_nice.percentage"
ts=int(time.time())
COLLECTION_INTERVAL=3     #seconds


def cpuperc():
    
    ticks = os.sysconf(os.sysconf_names['SC_CLK_TCK'])* COLLECTION_INTERVAL
   
    stat_fd = open('/proc/stat')
    stat_buf = stat_fd.readlines()[0].split()
    user, nice, sys, idle, iowait, irq, softirq ,steal ,guest,guest_nice= ( float(stat_buf[1]), float(stat_buf[2]),float(stat_buf[3]), float(stat_buf[4]), float(stat_buf[5]), float(stat_buf[6]), float(stat_buf[7]),float(stat_buf[8]),float(stat_buf[9]),float(stat_buf[10]))

    stat_fd.close()

    time.sleep(COLLECTION_INTERVAL)

    stat_fd = open('/proc/stat')
    stat_buf = stat_fd.readlines()[0].split()
    user_n, nice_n, sys_n, idle_n, iowait_n, irq_n, softirq_n, steal_n, guest_n,guest_nice_n= ( float(stat_buf[1]), float(stat_buf[2]),float(stat_buf[3]), float(stat_buf[4]),float(stat_buf[5]), float(stat_buf[6]),float(stat_buf[7]),float(stat_buf[8]),float(stat_buf[9]),float(stat_buf[10]))

    stat_fd.close()
    
    user_per=((user_n - user)/ ticks*10)
    nice_per=((nice_n - nice) /ticks*10)
    sys_per=((sys_n - sys) /ticks*10)
    idle_per=((idle_n -idle)/ ticks*10)
    iowait_per=((iowait_n - iowait)/ ticks*10)
    irq_per=((irq_n - irq) / ticks*10)
    softirq_per=((softirq_n - softirq) / ticks*10)
    steal_per=((steal_n - steal) / ticks*10)
    guest_per=((guest_n - guest)/ ticks*10)
    guest_nice_per=((guest_nice_n - guest_nice) / ticks*10)
   
    print ("%s %d %f host=%s"  %(metric1,ts,user_per,host))
    print ("%s %d %f host=%s"  %(metric2,ts,nice_per,host))
    print ("%s %d %f host=%s"  %(metric3,ts,sys_per,host))
    print ("%s %d %f host=%s"  %(metric4,ts,idle_per,host))
    print ("%s %d %f host=%s"  %(metric5,ts,iowait_per,host))
    print ("%s %d %f host=%s"  %(metric6,ts,irq_per,host))
    print ("%s %d %f host=%s"  %(metric7,ts,softirq_per,host))
    print ("%s %d %f host=%s"  %(metric8,ts,steal_per,host))
    print ("%s %d %f host=%s"  %(metric9,ts,guest_per,host))
    print ("%s %d %f host=%s"  %(metric10,ts,guest_nice_per,host))
sys.stdout.flush() 
if __name__ == '__main__':
    cpuperc()


