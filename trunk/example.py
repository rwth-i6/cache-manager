#!/usr/bin/env python
# example.py
#
# This file is part of CacheManager.
# 
# CacheManager is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
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

"""example uage of CmClient"""

import sys
import os
import cachemanager

if __name__ == "__main__":
    #cachemanager.logging.LogLevel.enableDebug()

    # get a local copy
    lf = cachemanager.cacheFile(sys.argv[0])
    print lf

    # copy a local file to the file server
    user = os.getenv("USER")
    lf = "/var/tmp/%s/test.1" % user
    fp = file(lf, "w")
    fp.write("1")
    cachemanager.copyFile(lf, "/u/%s/test.1" % user)

    # use CmClient
    config = cachemanager.client.ClientConfiguration()
    config.loadDefault()
    client = cachemanager.client.CmClient(config)
    print client.fetch(sys.argv[0])

    # use CmClient to copy several files
    client = cachemanager.client.CmClient(config, False)
    files = [ "client.py", "fetcher.py", "shared.py" ]
    dirname = os.path.dirname(sys.argv[0])
    for f in files:
        print client.fetch(os.path.join(dirname, f))


