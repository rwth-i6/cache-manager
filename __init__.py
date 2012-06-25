# __init__.py
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

"""Load-balanced file caching on local disks"""

from logging import LogLevel
from client import ClientConfiguration, CmClient

__version__ = "$Rev$"
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

