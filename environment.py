"""
environment settings for CacheManager client
"""

import socket
import filesystem

__version__ = "$Rev: 822 $"
__author__  = "rybach@cs.rwth-aachen.de (David Rybach)"
__copyright__ = "Copyright 2012, RWTH Aachen University"


class GenericClientEnvironment:
    """environment specific settings.

    selects server, cache directory, and remote file system implementation
    depending on the environment of the client (e.g. the hostname)

    to be customized in derived classes
    """

    def server(self, config):
        """return the hostname of the master"""
        return config.MASTER_HOST

    def cacheDir(self, config):
        """return the local cache directory"""
        return config.CACHE_DIR

    def remoteFileSystem(self, config):
        """return a RemoteFileSystem object to use.

        use filesytem.SshRemoteFileSystem if hard disks on other nodes
        are accessible by SSH connection

        use filesystem.NfsRemoteFileSystem if hard disks on other nodes
        are accessible by a network file system (e.g. NFS) and mounted
        on the compute nodes
        """
        return filesystem.SshRemoteFileSystem(config)


class ClientEnvironmentI6 (GenericClientEnvironment):
    """configuration used at i6

    two environments are defined: "i6" and "cluster".
    selection is based on the hostname of the client

    SSH connections are used for both environments

    this class can used as an example for multi-environment setups
    """

    isCluster = None

    def __init__(self):
        if ClientEnvironmentI6.isCluster is None:
            # check hostname only once
            ClientEnvironmentI6.isCluster = ClientEnvironmentI6._isCluster()

    def server(self, config):
        if ClientEnvironmentI6.isCluster:
            s = config.MASTER_HOST_CLUSTER
        else:
            s = config.MASTER_HOST_I6
        return s

    def cacheDir(self, config):
        if ClientEnvironmentI6.isCluster:
            cache = config.CACHE_DIR_CLUSTER
        else:
            cache = config.CACHE_DIR_I6
        return cache

    @staticmethod
    def _isCluster():
        hostname = socket.gethostname()
        return (hostname[0:10] == "cluster-cn" or hostname[:3] == "cn-")


