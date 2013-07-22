// This file is part of OpenTSDB.
// Copyright (C) 2010  The tcollector Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.

/** Quick CLI tool to get JMX MBean attributes.  */
package com.stumbleupon.monitoring;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.TabularData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

// Composite Data
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenType;

// Sun specific
import com.sun.tools.attach.AgentInitializationException;
import com.sun.tools.attach.AgentLoadException;
import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;

// Sun private
import sun.management.ConnectorAddressLink;
import sun.jvmstat.monitor.HostIdentifier;
import sun.jvmstat.monitor.MonitoredHost;
import sun.jvmstat.monitor.MonitoredVm;
import sun.jvmstat.monitor.MonitoredVmUtil;
import sun.jvmstat.monitor.VmIdentifier;

final class jmx {

  private static final String LOCAL_CONNECTOR_ADDRESS =
    "com.sun.management.jmxremote.localConnectorAddress";

  private static void usage() {
      System.out.println("Usage:\n"
                         + "  jmx -l                    Lists all reachable VMs.\n"
                         + "  jmx <JVM>                 Lists all MBeans for this JVM (PID or regexp).\n"
                         + "  jmx <JVM> <MBean>         Prints all the attributes of this MBean.\n"
                         + "  jmx <JVM> <MBean> <attr>  Prints the matching attributes of this MBean.\n"
                         + "\n"
                         + "You can pass multiple <MBean> <attr> pairs to match multiple different\n"
                         + "attributes for different MBeans.  For example:\n"
                         + "  jmx --long JConsole Class Count Thread Total Garbage Collection\n"
                         + "  LoadedClassCount	2808	java.lang:type=ClassLoading\n"
                         + "  UnloadedClassCount	0	java.lang:type=ClassLoading\n"
                         + "  TotalLoadedClassCount	2808	java.lang:type=ClassLoading\n"
                         + "  CollectionCount	0	java.lang:type=GarbageCollector,name=ConcurrentMarkSweep\n"
                         + "  CollectionTime	0	java.lang:type=GarbageCollector,name=ConcurrentMarkSweep\n"
                         + "  CollectionCount	1	java.lang:type=GarbageCollector,name=ParNew\n"
                         + "  CollectionTime	19	java.lang:type=GarbageCollector,name=ParNew\n"
                         + "  TotalStartedThreadCount	43	java.lang:type=Threading\n"
                         + "The command above searched for a JVM with `JConsole' in its name, and then searched\n"
                         + "for MBeans with `Class' in the name and `Count' in the attribute (first 3 matches\n"
                         + "in this output), MBeans with `Thread' in the name and `Total' in the attribute (last\n"
                         + "line in the output) and MBeans matching `Garbage' with a `Collection' attribute.\n"
                         + "\n"
                         + "Other flags you can pass:\n"
                         + "  --long                    Print a longer but more explicit output for each value.\n"
                         + "  --timestamp               Print a timestamp at the beginning of each line.\n"
                         + "  --watch N                 Reprint the output every N seconds.\n"
                         + "\n"
                         + "Return value:\n"
                         + "  0: Everything OK.\n"
                         + "  1: Invalid usage or unexpected error.\n"
                         + "  2: No JVM matched.\n"
                         + "  3: No MBean matched.\n"
                         + "  4: No attribute matched for the MBean(s) selected.");
  }

  private static void fatal(final int rv, final String errmsg) {
    System.err.println(errmsg);
    System.exit(rv);
    throw new AssertionError("You should never see this, really.");
  }

  public static void main(final String[] args) throws Exception {
    if (args.length == 0 || "-h".equals(args[0]) || "--help".equals(args[0])) {
      usage();
      System.exit(args.length == 0 ? 1 : 0);
      return;
    }

    int current_arg = 0;
    int watch = 0;
    boolean long_output = false;
    boolean print_timestamps = false;
    while (current_arg < args.length) {
      if ("--watch".equals(args[current_arg])) {
        current_arg++;
        try {
          watch = Integer.parseInt(args[current_arg]);
        } catch (NumberFormatException e) {
          fatal(1, "Invalid value for --watch: " + e.getMessage());
          return;
        }
        if (watch < 1) {
          fatal(1, "Invalid value for --watch: " + watch);
        }
        current_arg++;
      } else if ("--long".equals(args[current_arg])) {
        long_output = true;
        current_arg++;
      } else if ("--timestamp".equals(args[current_arg])) {
        print_timestamps = true;
        current_arg++;
      } else {
        break;
      }
    }

    if (current_arg == args.length) {
      usage();
      fatal(1, "error: Missing argument (-l or JVM specification).");
      return;
    }

    HashMap<Integer, JVM> vms = getJVMs();
    if ("-l".equals(args[current_arg])) {
      printVmList(vms.values());
      return;
    }

    final JVM jvm = selectJVM(args[current_arg++], vms);
    vms = null;
    final JMXConnector connection = JMXConnectorFactory.connect(jvm.jmxUrl());
    try {
      final MBeanServerConnection mbsc = connection.getMBeanServerConnection();
      if (args.length == current_arg) {
        for (final ObjectName mbean : listMBeans(mbsc)) {
          System.out.println(mbean);
        }
        return;
      }

      final TreeMap<ObjectName, Pattern> objects = selectMBeans(args, current_arg, mbsc);
      if (objects.isEmpty()) {
        fatal(3, "No MBean matched your query in " + jvm.name());
        return;
      }
      do {
        boolean found = false;
        for (final Map.Entry<ObjectName, Pattern> entry : objects.entrySet()) {
          final ObjectName object = entry.getKey();
          final MBeanInfo mbean = mbsc.getMBeanInfo(object);
          final Pattern wanted = entry.getValue();
          for (final MBeanAttributeInfo attr : mbean.getAttributes()) {
            if (wanted == null || wanted.matcher(attr.getName()).find()) {
              dumpMBean(long_output, print_timestamps, mbsc, object, attr);
              found = true;
            }
          }
        }
        if (!found) {
          fatal(4, "No attribute of " + objects.keySet()
                + " matched your query in " + jvm.name());
          return;
        }
        System.out.flush();
        Thread.sleep(watch * 1000);
      } while (watch > 0);
    } finally {
      connection.close();
    }
  }

  private static TreeMap<ObjectName, Pattern> selectMBeans(final String[] args,
                                                           final int current_arg,
                                                           final MBeanServerConnection mbsc) throws IOException {
    final TreeMap<ObjectName, Pattern> mbeans = new TreeMap<ObjectName, Pattern>();
    for (int i = current_arg; i < args.length; i += 2) {
      final Pattern object_re = compile_re(args[i]);
      final Pattern attr_re = i + 1 < args.length ? compile_re(args[i + 1]) : null;
      for (final ObjectName o : listMBeans(mbsc)) {
        if (object_re.matcher(o.toString()).find()) {
          mbeans.put(o, attr_re);
        }
      }
    }
    return mbeans;
  }

  private static void dumpMBean(final boolean long_output,
                                final boolean print_timestamps,
                                final MBeanServerConnection mbsc,
                                final ObjectName object,
                                final MBeanAttributeInfo attr) throws Exception {
    final String name = attr.getName();
    Object value  = null;
    try {
      value = mbsc.getAttribute(object, name);
    } catch (Exception e) {
      // Above may raise errors for some attributes like 
      // CollectionUsage
      return;
    }
    if (value instanceof TabularData) {
      final TabularData tab = (TabularData) value;
      int i = 0;
      for (final Object o : tab.keySet()) {
        dumpMBeanValue(long_output, print_timestamps, object, name + "." + i, o);
        i++;
      }
    } else if (value instanceof CompositeDataSupport){
    	CompositeDataSupport cds = (CompositeDataSupport) value;
    	CompositeType ct = cds.getCompositeType();
    	for (final String item: ct.keySet()){
    		dumpMBeanValue(long_output, print_timestamps, object, name + "." + item, cds.get(item));
    	}
    } else {
      dumpMBeanValue(long_output, print_timestamps, object, name, value);
    }
  }

  private static void dumpMBeanValue(final boolean long_output,
                                     final boolean print_timestamps,
                                     final ObjectName object,
                                     final String name,
                                     final Object value) {
    // Ignore non numeric values
    if ((value instanceof String)||
        (value instanceof String[])|| 
        (value instanceof Boolean)) {
      return;
    }
    final StringBuilder buf = new StringBuilder();
    final long timestamp = System.currentTimeMillis() / 1000;
    if (print_timestamps) {
      buf.append(timestamp).append('\t');
    }
    if (value instanceof Object[]) {
      for (final Object o : (Object[]) value) {
        buf.append(o).append('\t');
      }
      if (buf.length() > 0) {
        buf.setLength(buf.length() - 1);
      }
    } else {
      buf.append(name).append('\t').append(value);
    }
    if (long_output) {
      buf.append('\t').append(object);
    }
    buf.append('\n');
    System.out.print(buf);
  }

  private static ArrayList<ObjectName> listMBeans(final MBeanServerConnection mbsc) throws IOException {
    ArrayList<ObjectName> mbeans = new ArrayList<ObjectName>(mbsc.queryNames(null, null));
    Collections.sort(mbeans, new Comparator<ObjectName>() {
      public int compare(final ObjectName a, final ObjectName b) {
        return a.toString().compareTo(b.toString());
      }
    });
    return mbeans;
  }

  private static Pattern compile_re(final String re) {
    try {
      return Pattern.compile(re);
    } catch (PatternSyntaxException e) {
      fatal(1, "Invalid regexp: " + re + ", " + e.getMessage());
      throw new AssertionError("Should never be here");
    }
  }

  private static final String MAGIC_STRING = "this.is.jmx.magic";

  private static JVM selectJVM(final String selector,
                               final HashMap<Integer, JVM> vms) {
    String error = null;
    try {
      final int pid = Integer.parseInt(selector);
      if (pid < 2) {
        throw new IllegalArgumentException("Invalid PID: " + pid);
      }
      final JVM jvm = vms.get(pid);
      if (jvm != null) {
        return jvm;
      }
      error = "Couldn't find a JVM with PID " + pid;
    } catch (NumberFormatException e) {
      /* Ignore. */
    }
    if (error == null) {
      try {
        final Pattern p = compile_re(selector);
        final ArrayList<JVM> matches = new ArrayList<JVM>(2);
        for (final JVM jvm : vms.values()) {
          if (p.matcher(jvm.name()).find()) {
            matches.add(jvm);
          }
        }
        // Exclude ourselves from the matches.
        System.setProperty(MAGIC_STRING,
                           "LOL Java processes can't get their own PID");
        final String me = jmx.class.getName();
        final Iterator<JVM> it = matches.iterator();
        while (it.hasNext()) {
          final JVM jvm = it.next();
          final String name = jvm.name();
          // Ignore other long running jmx clients too.
          if (name.contains("--watch") && name.contains(me)) {
            it.remove();
            continue;
          }
          final VirtualMachine vm = VirtualMachine.attach(String.valueOf(jvm.pid()));
          try {
            if (vm.getSystemProperties().containsKey(MAGIC_STRING)) {
              it.remove();
              continue;
            }
          } finally {
            vm.detach();
          }
        }
        System.clearProperty(MAGIC_STRING);
        if (matches.size() == 0) {
          error = "No JVM matched your regexp " + selector;
        } else if (matches.size() > 1) {
          printVmList(matches);
          error = matches.size() + " JVMs matched your regexp " + selector
            + ", it's too ambiguous, please refine it.";
        } else {
          return matches.get(0);
        }
      } catch (PatternSyntaxException e) {
        error = "Invalid pattern: " + selector + ", " + e.getMessage();
      } catch (Exception e) {
        e.printStackTrace();
        error = "Unexpected Exception: " + e.getMessage();
      }
    }
    fatal(2, error);
    return null;
  }

  private static void printVmList(final Collection<JVM> vms) {
    final ArrayList<JVM> sorted_vms = new ArrayList<JVM>(vms);
    Collections.sort(sorted_vms, new Comparator<JVM>() {
      public int compare(final JVM a, final JVM b) {
        return a.pid() - b.pid();
      }
    });
    for (final JVM jvm : sorted_vms) {
      System.out.println(jvm.pid() + "\t" + jvm.name());
    }
  }

  private static final class JVM {
    final int pid;
    final String name;
    String address;

    public JVM(final int pid, final String name, final String address) {
      if (name.isEmpty()) {
        throw new IllegalArgumentException("empty name");
      }
      this.pid = pid;
      this.name = name;
      this.address = address;
    }

    public int pid() {
      return pid;
    }

    public String name() {
      return name;
    }

    public JMXServiceURL jmxUrl() {
      if (address == null) {
        ensureManagementAgentStarted();
      }
      try {
        return new JMXServiceURL(address);
      } catch (Exception e) {
        throw new RuntimeException("Error", e);
      }
    }

    public void ensureManagementAgentStarted() {
      if (address != null) {  // already started
        return;
      }
      VirtualMachine vm;
      try {
        vm = VirtualMachine.attach(String.valueOf(pid));
      } catch (AttachNotSupportedException e) {
        throw new RuntimeException("Failed to attach to " + this, e);
      } catch (IOException e) {
        throw new RuntimeException("Failed to attach to " + this, e);
      }
      try {
        // java.sun.com/javase/6/docs/technotes/guides/management/agent.html#gdhkz
        // + code mostly stolen from JConsole's code.
        final String home = vm.getSystemProperties().getProperty("java.home");

        // Normally in ${java.home}/jre/lib/management-agent.jar but might
        // be in ${java.home}/lib in build environments.

        String agent = home + File.separator + "jre" + File.separator
          + "lib" + File.separator + "management-agent.jar";
        File f = new File(agent);
        if (!f.exists()) {
          agent = home + File.separator +  "lib" + File.separator
            + "management-agent.jar";
          f = new File(agent);
          if (!f.exists()) {
            throw new RuntimeException("Management agent not found");
          }
        }

        agent = f.getCanonicalPath();
        try {
          vm.loadAgent(agent, "com.sun.management.jmxremote");
        } catch (AgentLoadException e) {
          throw new RuntimeException("Failed to load the agent into " + this, e);
        } catch (AgentInitializationException e) {
          throw new RuntimeException("Failed to initialize the agent into " + this, e);
        }
        address = (String) vm.getAgentProperties().get(LOCAL_CONNECTOR_ADDRESS);
      } catch (IOException e) {
        throw new RuntimeException("Error while loading agent into " + this, e);
      } finally {
        try {
          vm.detach();
        } catch (IOException e) {
          throw new RuntimeException("Failed to detach from " + vm + " = " + this, e);
        }
      }
      if (address == null) {
        throw new RuntimeException("Couldn't start the management agent.");
      }
    }

    public String toString() {
      return "JVM(" + pid + ", \"" + name + "\", "
        + (address == null ? null : '"' + address + '"') + ')';
    }
  }

  /**
   * Returns a map from PID to JVM.
   */
  private static HashMap<Integer, JVM> getJVMs() throws Exception {
    final HashMap<Integer, JVM> vms = new HashMap<Integer, JVM>();
    getMonitoredVMs(vms);
    getAttachableVMs(vms);
    return vms;
  }

  private static void getMonitoredVMs(final HashMap<Integer, JVM> out) throws Exception {
    final MonitoredHost host =
      MonitoredHost.getMonitoredHost(new HostIdentifier((String) null));
    @SuppressWarnings("unchecked")
    final Set<Integer> vms = host.activeVms();
    for (final Integer pid : vms) {
      try {
        final VmIdentifier vmid = new VmIdentifier(pid.toString());
        final MonitoredVm vm = host.getMonitoredVm(vmid);
        out.put(pid, new JVM(pid, MonitoredVmUtil.commandLine(vm),
                             ConnectorAddressLink.importFrom(pid)));
        vm.detach();
      } catch (Exception x) {
        System.err.println("Ignoring exception:");
        x.printStackTrace();
      }
    }
  }

  private static void getAttachableVMs(final HashMap<Integer, JVM> out) {
    for (final VirtualMachineDescriptor vmd : VirtualMachine.list()) {
      int pid;
      try {
        pid = Integer.parseInt(vmd.id());
      } catch (NumberFormatException e) {
        System.err.println("Ignoring invalid vmd.id(): " + vmd.id()
                           + ' ' + e.getMessage());
        continue;
      }
      if (out.containsKey(pid)) {
        continue;
      }
      try {
        final VirtualMachine vm = VirtualMachine.attach(vmd);
        out.put(pid, new JVM(pid, String.valueOf(pid),
                             (String) vm.getAgentProperties().get(LOCAL_CONNECTOR_ADDRESS)));
        vm.detach();
      } catch (AttachNotSupportedException e) {
        System.err.println("VM not attachable: " + vmd.id()
                           + ' ' + e.getMessage());
      } catch (IOException e) {
        System.err.println("Could not attach: " + vmd.id()
                           + ' ' + e.getMessage());
      }
    }
  }

}
