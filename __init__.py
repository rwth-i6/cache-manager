"""Load-balanced file caching on local disks"""

from cmlogging import LogLevel
from client import ClientConfiguration, CmClient

__version__ = "$Rev: 821 $"
__author__  = "rybach@cs.rwth-aachen.de (David Rybach)"
__copyright__ = "Copyright 2012, RWTH Aachen University"

def cacheFile(filename, verbose=True):
    """convenience function to cache a file to the local disk.
    returns the path of the cached file.
    """
    loglevel = LogLevel.level
    if not verbose:
        LogLevel.level = 0
    config = ClientConfiguration()
    config.loadDefault()
    client = CmClient(config)
    cachedFile, ok = client.fetch(filename)
    LogLevel.set(loglevel)
    return cachedFile

def copyFile(source, destination, verbose=True):
    """convenience function to copy a file from the local disk to a file server.
    returns true if the file was copied.
    """
    loglevel = LogLevel.level
    if not verbose:
        LogLevel.level = 0
    config = ClientConfiguration()
    config.loadDefault()
    client = CmClient(config)
    ok = client.copy(source, destination)
    LogLevel.set(loglevel)
    return ok

