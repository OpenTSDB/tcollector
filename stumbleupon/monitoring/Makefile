# Copyright 2010 StumbleUpon, Inc.
#
# This library is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library.  If not, see <http://www.gnu.org/licenses/>.

all: jmx

top_builddir = build
package = com.stumbleupon.monitoring
spec_title = Monitoring Tools
spec_vendor = StumbleUpon, Inc.
spec_version = 1.0
jmx_JAVA = \
	jmx.java	\

jmx_LIBADD = \
	/usr/lib/jvm/java-6-sun/lib/tools.jar	\

AM_JAVACFLAGS = -Xlint -source 6
JVM_ARGS =
package_dir = $(subst .,/,$(package))
jmx_classes=$(jmx_JAVA:%.java=$(top_builddir)/$(package_dir)/%.class)
jmx_jar = $(top_builddir)/jmx-$(spec_version).jar

jmx: $(jmx_jar)

jmx_get_dep_classpath = `echo $(jmx_LIBADD) | tr ' ' ':'`
$(top_builddir)/.javac-stamp: $(jmx_JAVA)
	@mkdir -p $(top_builddir)
	javac $(AM_JAVACFLAGS) -cp $(jmx_get_dep_classpath) \
	  -d $(top_builddir) $(jmx_JAVA)
	@touch "$@"

classes_with_nested_classes = $(jmx_classes:$(top_builddir)/%.class=%*.class)

pkg_version = \
  `git rev-list --pretty=format:%h HEAD --max-count=1 | sed 1d || echo unknown`
$(top_builddir)/manifest: $(top_builddir)/.javac-stamp ../../.git/HEAD
	{ echo "Specification-Title: $(spec_title)"; \
          echo "Specification-Version: $(spec_version)"; \
          echo "Specification-Vendor: $(spec_vendor)"; \
          echo "Implementation-Title: $(package)"; \
          echo "Implementation-Version: $(pkg_version)"; \
          echo "Implementation-Vendor: $(spec_vendor)"; } >"$@"

$(jmx_jar): $(top_builddir)/manifest $(top_builddir)/.javac-stamp $(jmx_classes)
	cd $(top_builddir) && jar cfm `basename $(jmx_jar)` manifest $(classes_with_nested_classes) \
         || { rv=$$? && rm -f `basename $(jar)` && exit $$rv; }
#                       ^^^^^^^^^^^^^^^^^^^^^^^
# I've seen cases where `jar' exits with an error but leaves a partially built .jar file!

doc: $(top_builddir)/api/index.html

JDK_JAVADOC=http://download.oracle.com/javase/6/docs/api
$(top_builddir)/api/index.html: $(jmx_JAVA) $(BUILT_SOURCES)
	javadoc -d $(top_builddir)/api -classpath $(get_dep_classpath) \
          -link $(JDK_JAVADOC) -link $(jmx_JAVA) $(BUILT_SOURCES)

clean:
	@rm -f $(top_builddir)/.javac-stamp
	rm -f $(top_builddir)/manifest $(BUILT_SOURCES)
	cd $(top_builddir) || exit 0 && rm -f $(classes_with_nested_classes)
	cd $(top_builddir) || exit 0 \
	  && test -d $(package_dir) || exit 0 \
	  && find $(package_dir) -type d -depth -exec rmdir {} ';' \
	  && dir=$(package_dir) && dir=$${dir%/*} \
	  && while test x"$$dir" != x"$${dir%/*}"; do \
	       rmdir "$$dir" && dir=$${dir%/*} || break; \
	     done \
	  && rmdir "$$dir"

distclean: clean
	rm -f $(jar)
	rm -rf $(top_builddir)/api
	test ! -d $(top_builddir) || rmdir $(top_builddir)

.PHONY: all jmx clean distclean doc check
