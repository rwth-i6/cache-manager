# settings.py
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
definition of configuration objects and default configuration values
"""

from shared import Configuration
import environment

__version__ = "$Rev$"
__author__  = "rybach@cs.rwth-aachen.de (David Rybach)"
__copyright__ = "Copyright 2012, RWTH Aachen University"


def clientEnvironment():
    """client environment to be used.
    see environment.py
    """
    return environment.ClientEnvironmentI6()


class ClientDefaultConfiguration (Configuration):
    """ default configuration for CacheManager clients """

    """ hostname or IP of machine running the CacheManager server (environment "i6") """
    MASTER_HOST_I6      = "www-i6"

    """ hostname or IP of machine running the CacheManager server (environment "cluster") """
    MASTER_HOST_CLUSTER = "cluster-mn-04"

    """ port number of the CacheManager server """
    MASTER_PORT         = 10322

    """ directory on local hard disks used for caching (environment "cluster") """
    CACHE_DIR_CLUSTER   = "/var/autofs/net/$(HOST)/$(USER)"

    """ directory on local hard disks used for caching (environment "i6") """
    CACHE_DIR_I6        = "/var/tmp/$(USER)"

    """ minimum free space on cache disk (bytes) """
    MIN_FREE            = 100 * 1024 * 1024

    """ maximum disk space used for caches (percent of total space) """
    MAX_USAGE           = 10

    """ minimum age (in terms of atime) of a file to be deleted (seconds) """
    MIN_AGE             = 24 * 60 * 60

    """ time out for the connection to the master (seconds) """
    SOCKET_TIMEOUT      = 2 * 60.0

    """ time to wait for operations on remote files (seconds) """
    STAT_TIMEOUT        = 20

    """ ignore special meaning of bundle archives (*.bundle) """
    IGNORE_BUNDLE       = False

    """ slow down file copies (for regression tests only) """
    SLOW_COPY           = False


class ServerConfiguration (Configuration):
    """ default configuration for CacheManager server"""

    """ TCP port number used """
    PORT                = 10322

    """ queue size of the server socket """
    CONNECTION_QUEUE    = 256

    """ maximum number of parallel transfers from/to file servers """
    MAX_COPY_SERVER     = 20

    """ maximum number of parallel transfers from/to nodes """
    MAX_COPY_NODE       = 1

    """ database persistence file """
    DB_FILE             = "cm-server.db"

    """ interval between database writes (seconds) """
    DB_SAVE_INTERVAL    = 60 * 60

    """ interval between statistics writes (seconds) """
    STAT_INTERVAL       = 60 * 60

    """ interval between database cleanups (seconds) """
    CLEANUP_INTERVAL    = 60 * 60 * 24

    """ timeout for client sockets (seconds) """
    SOCKET_TIMEOUT      = 30 * 60

    """ maximum time a client may spend copying (seconds) """
    MAX_WAIT_COPY       = 15 * 60

    """ time a client has to wait before next copy attempt (seconds) """
    CLIENT_WAIT         = 10

    """ time after a record in the database is deleted (seconds) """
    MAX_AGE             = 60 * 60 * 24 * 14

