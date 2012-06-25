#!/usr/bin/env python
# dbutil.py
#
# This file is part of CacheManager.
# 
# CacheManager is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.
# 
# CacheManager is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with CacheManager. If not, see <http://www.gnu.org/licenses/>.
#
# Copyright 2012, RWTH Aachen University. All rights reserved.

"""
database utility for cache-manager databases
"""

import sys
from cachemanager import *
from cmserver import *

__version__ = "$Rev$"
__author__  = "rybach@cs.rwth-aachen.de (David Rybach)"
__copyright__ = "Copyright 2012, RWTH Aachen University"


def main(argv):
    if len(argv) < 3:
        print "usage: %s <db-file> <action>" % argv[0]
        print "action: stat | filestat | dump | convert <file> | clean <max-age> | delete <prefix>"
        return 1
    enableDebug()
    db = FileDatabase()
    if not db.load(argv[1]):
        print "cannot load database from " + argv[1]
    action = argv[2]

    if action == "stat":
        numFiles, numLoc = db.getStat()
        print "numFiles: ", numFiles
        print "numLoc:   ", numLoc
    elif action == "filestat":
        for f in db.files:
            print f, len(db.files[f])
    elif action == "dump":
        for f in db.files:
            for l in db.files[f]:
                print f, l
    elif action == "convert":
        db.write(sys.argv[3])

    elif action == "clean":
        # db.getLocation( db.files.keys()[1] )
        db.removeOldRecords(int(time.time()) - int(sys.argv[3]))
        db.write(sys.argv[1])
    elif action == "delete":
	prefix = sys.argv[3]
	deleted = 0
	scanned = 0
	for f in db.files.keys():
	    scanned += 1
	    if f.startswith(prefix):
		del db.files[f]
		deleted += 1
	print "deleted %d / %d records" % (deleted, scanned)
	db.write(sys.argv[1])
    elif action == "fill":
        nFiles = int(sys.argv[3])
        nLoc = int(sys.argv[4])
        for f in range(nFiles):
            length = random.randint(4, 20)
            name = []
            for l in range(length):
                name.append( "DIR_%d" % random.randint(0,20) )
            filename = "FILE_%d" % random.randint(0,500)
            filepath = "/" + "/".join(name) + "/" + filename
            for l in range(nLoc):
                host = "HOST_%d" % random.randint(0, 150)
                user = "USER_%d" % random.randint(0, 100)
                path = "/var/tmp/%s%s" % (user, filepath)
                loc = Location(path, 1, int(time.time()), host)
                db.addLocation(filepath, loc)
        db.write(sys.argv[1])
    else:
        print "unknwon action: " + action
        return 1
    return 0

if __name__ == "__main__":
    sys.exit( main(sys.argv) )

