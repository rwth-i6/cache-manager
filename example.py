#!/usr/bin/env python
"""example uage of CmClient"""

import sys
import os
import cachemanager

if __name__ == "__main__":
    #cachemanager.cmlogging.LogLevel.enableDebug()

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


