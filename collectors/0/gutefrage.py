#!/usr/bin/env python
# Gutefrage App stat collector.
# Copyright (C) 2011  Gutefrage.net GmbH.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.
"""gutefrage app stats for TSDB """


"""
...
"""

import MySQLdb
import time
import sys

COLLECTION_INTERVAL = 120  # seconds

def main():

   while True:
       ts = int(time.time())

       try:
           db = MySQLdb.connect(host='127.0.0.1', user='root', passwd="", db="GFN")
         
           cursor = db.cursor(MySQLdb.cursors.DictCursor)

           cursor.execute("SELECT count(id) qCount FROM ask_question WHERE 1")
           counter = cursor.fetchall()
           print ("gf.app.questions %d %s" % (ts, counter[0]['qCount']))
           
           cursor.execute("SELECT count(id) qCount FROM ask_tag WHERE 1")
           counter = cursor.fetchall()
           print ("gf.app.tags %d %s" % (ts, counter[0]['qCount']))

           cursor.execute("SELECT count(id) qCount FROM ask_answer WHERE 1")
           counter = cursor.fetchall()
           print ("gf.app.answers %d %s" % (ts, counter[0]['qCount']))
           
           cursor.execute("SELECT count(id) qCount FROM ask_comment WHERE 1")
           counter = cursor.fetchall()
           print ("gf.app.comments %d %s" % (ts, counter[0]['qCount']))

           cursor.execute("SELECT count(id) qCount FROM ask_video WHERE 1")
           counter = cursor.fetchall()
           print ("gf.app.videos %d %s" % (ts, counter[0]['qCount']))
           
           cursor.execute("SELECT count(id) qCount FROM ask_medium WHERE 1")
           counter = cursor.fetchall()
           print ("gf.app.media %d %s" % (ts, counter[0]['qCount']))

           cursor.execute("SELECT count(id) qCount FROM ask_user WHERE 1")
           counter = cursor.fetchall()
           print ("gf.app.users %d %s" % (ts, counter[0]['qCount']))

           cursor.execute("SELECT count(*) qCount FROM ask_interest WHERE 1")
           counter = cursor.fetchall()
           print ("gf.app.questions.diggs %d %s" % (ts, counter[0]['qCount']))

           cursor.execute("SELECT count(*) qCount FROM ask_messages WHERE 1")
           counter = cursor.fetchall()
           print ("gf.app.messages %d %s" % (ts, counter[0]['qCount']))

           cursor.execute("SELECT count(*) qCount FROM ask_moderation_queue WHERE 1")
           counter = cursor.fetchall()
           print ("gf.app.moderation.queue %d %s" % (ts, counter[0]['qCount']))
           
           cursor.execute("SELECT count(*) qCount FROM ask_relevancy WHERE 1")
           counter = cursor.fetchall()
           print ("gf.app.answers.diggs %d %s" % (ts, counter[0]['qCount']))
           
           cursor.execute("SELECT count(*) qCount FROM ask_universal_queue WHERE 1")
           counter = cursor.fetchall()
           print ("gf.app.universal_queue %d %s" % (ts, counter[0]['qCount']))
           
           cursor.execute("SELECT count(*) qCount FROM ask_user_action WHERE 1")
           counter = cursor.fetchall()
           print ("gf.app.moderation.actions %d %s" % (ts, counter[0]['qCount']))
           
           cursor.execute("SELECT count(*) qCount FROM ask_answer_selection WHERE 1")
           counter = cursor.fetchall()
           print ("gf.app.answers.helpful %d %s" % (ts, counter[0]['qCount']))
           
           cursor.close()
           db.close()
       except MySQLdb.Error, e:
         print >> sys.stderr, "MySQLdb error %d: %s" % (e.args[0],e.args[1])
         time.sleep(COLLECTION_INTERVAL*5)

       sys.stdout.flush()
       time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
   main()
