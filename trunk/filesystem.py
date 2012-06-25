# filesystem.py
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
file system access classes for the cache manager client
"""

import statvfs
import threading
import socket
import os
import os.path
import time
import subprocess
import datetime
import signal
from logging import *
import settings

__version__ = "$Rev$"
__author__  = "rybach@cs.rwth-aachen.de (David Rybach)"
__copyright__ = "Copyright 2012, RWTH Aachen University"


def _popenWithTimeout(cmd, timeout):
    start = datetime.datetime.now()
    process = subprocess.Popen(cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    while process.poll() is None:
        time.sleep(0.2)
        now = datetime.datetime.now()
        if (now - start).seconds > timeout:
            os.kill(process.pid, signal.SIGKILL)
            os.waitpid(-1, os.WNOHANG)
            return None
    return process.stdout.readlines()


class FileSystem:

    def __init__(self, config):
        self.config = config
        self.cacheDir = self.getCacheDir()
        self.lsofFailed = False

    def getCacheDir(self):
        cacheDir = settings.clientEnvironment().cacheDir(self.config)
        cacheDir = cacheDir.replace("$(HOST)", socket.gethostname())
        cacheDir = cacheDir.replace("$(USER)", os.environ.get("USER"))
        return cacheDir

    def getFileInfo(self, filename):
        try:
            return [filename, str(int(os.path.getsize(filename))), str(int(os.path.getmtime(filename)))]
        except Exception, e:
            error("cannot get file info for %s: %s" % (filename, str(e)))
            return None

    def getFileServer(self, filename):
        mounts = {}
        for line in file("/proc/mounts"):
            l = line.split()
            if ":" in l[0]:
                server = l[0].split(":")[0]
                mounts[l[1]] = server
        mp = ""
        dir = os.path.dirname(filename)
        for m in mounts.keys():
            cp = os.path.commonprefix([dir, m])
            if len(cp) > len(mp):
                mp = cp
        debug("mp: " + mp)
        try:
            return mounts[mp]
        except KeyError:
            return ""

    def diskFree(self, dir):
        stat = os.statvfs(dir)
        free = stat[statvfs.F_BAVAIL] * stat[statvfs.F_BSIZE]
        total = stat[statvfs.F_BLOCKS] * stat[statvfs.F_BSIZE]
        debug("free / total: %d %d" % (free, total))
        return free, total

    def diskUsage(self, dir):
        # du = os.popen("/usr/bin/du -sb %s 2>/dev/null" % dir).readlines()
        du = _popenWithTimeout("/usr/bin/du -sb %s " % dir, self.config.STAT_TIMEOUT)
        if du is None:
            warning("du %s timed out" % dir)
            used = 0
        elif not len(du):
            used = 0
        else:
            used = int(du[0].split()[0])
        return used

    def calculateSpaceToFree(self, filesize, destDir, logInfo = False):
        free, total = self.diskFree(destDir)
        used = self.diskUsage(self.cacheDir)
        debug("free: %d B = %d MB" % (free, free/(1024*1024)))
        debug("min free: %d B = %d MB" % (self.config.MIN_FREE, self.config.MIN_FREE / (1024*1024)))
        debug("used: %d B = %d MB" % (used, used/(1024*1024)))
        available = total * (self.config.MAX_USAGE / 100.0)
        debug("available: %0.2f = %0.2f" % (available, available/(1024*1024)))
        toFree = 0
        if (used + filesize) > available:
            toFree = (used + filesize) - available
        if (free - filesize) < self.config.MIN_FREE:
            toFree = max(toFree, (self.config.MIN_FREE + filesize) - free)
        toFree = int(toFree)
        debug("toFree: %d" % toFree)
        if logInfo:
            log("available disk space: %d MB" % (free/(1024*1024)))
            log("consumed for caching: %d MB = %0.2f %%" % \
                    (used/(1024*1024), (used/float(total))*100))
        return toFree

    def checkFreeSpace(self, filesize, destination):
        debug("checkFreeSpace: %d, %s" % (filesize, destination))
        destDir = os.path.dirname(destination)
        try:
            toFree = self.calculateSpaceToFree(filesize, destDir)
        except Exception, e:
            error("cannot get disk usage: %s" % str(e))
            return (False, [])
        # TODO: return toFree
        if toFree > 0:
            r = self.removeOldFiles(toFree, destination)
            if not r[0]:
                self.calculateSpaceToFree(filesize, destDir, True)
            return r
        else:
            return (True, [])

    def isFileOpen(self, filename):
        # r = os.popen("/usr/bin/lsof -Fp %s 2> /dev/null" % filename).readlines()
        if self.lsofFailed:
            return True
        r = _popenWithTimeout("/usr/bin/lsof -Fp %s" % filename, self.config.STAT_TIMEOUT)
        if r is None:
            warning("lsof %s timed out. skip further calls to lsof" % filename)
            self.lsofFailed = True
            return True
        elif len(r) > 0:
            debug("%s in use by %s" % (filename, r[0].strip()))
            return True
        else:
            return False

    def isFileOld(self, filename):
        try:
            age = int(time.time() - os.path.getatime(filename))
        except Exception, e:
            debug("debug cannot stat %s: %s" % (filename, str(e)))
            return False
        debug("age of %s: %d => %s" % (filename, age, str(age > self.config.MIN_AGE)))
        return (age > self.config.MIN_AGE)

    def setATime(self, filename):
        try:
            debug("setAtime(atime=%d, mtime=%f)" % (time.time(), os.path.getmtime(filename)))
            os.utime(filename, (time.time(), os.path.getmtime(filename)))
        except Exception, e:
            debug("cannot set atime of %s: %s" % (filename, str(e)))

    def removeOldFiles(self, spaceToFree, fileToKeep):
        debug("removeOldFiles: %d, %s" % (spaceToFree, fileToKeep))
        debug("os.walk: %s" % self.cacheDir)
        removed = []
        for root, dirs, files in os.walk(self.cacheDir):
            for name in files:
                path = os.path.join(root, name)
                debug(" root, name: " + root + ", " + name)
                debug(" check file: " + path)
                if path == fileToKeep or self.isFileOpen(path) or not self.isFileOld(path):
                    continue
                try:
                    size = os.path.getsize(path)
                    os.remove(path)
                    removed.append(path)
                    log("removed " + path)
                    debug("size = %d" % size)
                    spaceToFree -= size
                except Exception, e:
                    log("cannot remove " + path)
                if spaceToFree <= 0: break
            if spaceToFree <= 0: break
        try:
            debug("spaceToFree: %d" % int(spaceToFree))
        except Exception, e:
            error("unhandled exception: spaceToFree=%s" % str(spaceToFree))
            return (False, removed)
        # TODO: send deleted files to the master
        return ((spaceToFree <= 0), removed)


    def destinationExists(self, fileinfo, destination):
        debug("destinationExists: " + str(fileinfo) + "," + destination)
        # return: (exists, canCopy, removed)
        if not os.path.isfile(destination):
            debug("no file: " + destination)
            return (False, True, False)
        existingFile = self.getFileInfo(destination)
        if (existingFile[1] != fileinfo[1] or \
            int(float(existingFile[2])) != int(float(fileinfo[2]))):
            debug("attributes differ. remove. (%s - %s, %s - %s)" % (existingFile[1], fileinfo[1], existingFile[2], fileinfo[2]))
            if not self.isFileOpen(destination):
                try:
                    os.remove(destination)
                    log("removed " + destination)
                    return (False, True, True)
                except Exception, e:
                    error("cannot remove existing file: %s" % str(e))
                    return (False, False, False)
            else:
                error("cannot remove existing file: file is open")
                return (False, False, False)
        else:
            debug("file exists")
            return (True, True, False)


class RemoteFileSystem:


    class StatThread (threading.Thread):
        def __init__(self, filename):
            self.filename = filename
            threading.Thread.__init__(self)
            self.isFile = False
            self.fileSize = None
            self.mTime = None

        def run(self):
            try:
                self.isFile = os.path.isfile(self.filename)
                if self.isFile:
                    self.fileSize = int(os.path.getsize(self.filename))
                    self.mTime = int(os.path.getmtime(self.filename))
            except Exception, e:
                debug("error StatThread: %s" % filename)

    def __init__(self, config):
        self.config = config

    def isHostAlive(self, host):
	try:
	    devNull = open("/dev/null", "w")
	    debug("sending ping to %s" % host)
	    r = subprocess.call(["/bin/ping","-c 1", "-w 1", host],
		    shell=False, stdout=devNull, stderr=devNull)
	except Exception, e:
	    debug("exception in ping: %s" % str(e))
	    return True
	devNull.close()
	return r == 0


    def getFileStat(self, host, filename):
	# virtual method
	pass

    def execWithFlock(self, file, cmd):
	execCmd = '/usr/bin/flock -e -n %s -c "%s" 2>&1' % (file, cmd)
	debug(execCmd)
	msg = ""
	r = False
	try:
	    fd = os.popen(execCmd)
	    msg = fd.read()
	    r = (fd.close() is None)
	except Exception, e:
	    msg = "copy failed: str(e)"
	debug("r=%s, msg=%s" % (str(r), msg))
	return (r, msg)

    def copyFile(self, host, source, destination):
	# virtual method
	pass

    def copyUsingCp(self, source, destination):
	""" cp -p doesn't handle ACLs correctly
	    therefore we set the mode using chmod. """
	cmd = '/bin/cp --preserve=ownership,timestamps "%s" "%s"' % (source, destination)
	r = self.execWithFlock(destination, cmd)
	cmd = '/bin/chmod -f --reference="%s" "%s"' % (source, destination)
	debug(cmd)
	try:
    	    os.system(cmd)
	except Exception, e:
	    debug(str(e))
	if self.config.SLOW_COPY:
	    time.sleep(10)
	return r

    def brandFile(self, host, filename):
	# virtual method
	pass

class NfsRemoteFileSystem (RemoteFileSystem):

    def getFileStat(self, host, filename):
	if not self.isHostAlive(host):
	    return None
	st = self.StatThread(filename)
	st.setDaemon(True)
	st.start()
	st.join(self.config.STAT_TIMEOUT)
	if st.isAlive():
	    warning("stat of %s timed out" % filename)
	    r = None
	elif not st.isFile:
	    r = None
	else:
	    r = (st.fileSize, st.mTime)
	del st
	return r

    def brandFile(self, host, filename):
	try:
	    debug("setAtime(atime=%d, mtime=%f)" % (time.time(), os.path.getmtime(filename)))
	    os.utime(filename, (time.time(), os.path.getmtime(filename)))
	except Exception, e:
	    debug("cannot set atime of %s: %s" % (filename, str(e)))


    def copyFile(self, host, source, destination):
	return self.copyUsingCp(source, destination)


class SshRemoteFileSystem (RemoteFileSystem):

    SSH_SOCKET = "/tmp/cf_%d_ssh_%%r_%%h_%%p" % os.getpid()

    SSH_OPT = "-o PubkeyAuthentication=yes -o PasswordAuthentication=no " \
              "-o BatchMode=yes -o CheckHostIP=no -o LogLevel=quiet -c arcfour256 " \
	      "-o ControlPath=" + SSH_SOCKET

    SCP_OPT = SSH_OPT + " -p -q -B"


    def __init__(self, config):
	RemoteFileSystem.__init__(self, config)
	self.connections = {}

    def connectHost(self, host):
	if not host in self.connections:
	    self.connections[host] = SshMasterConnection(host)

    def getFileStat(self, host, filename):
	if not self.isHostAlive(host):
	    return None
	self.connectHost(host)
	cmd =  "/usr/bin/ssh -x %s %s " % (self.SSH_OPT, host)
	cmd += "'/usr/bin/stat --format=\"%%s %%Y\" %s 2>/dev/null'" % filename
	debug(cmd)
	b = os.popen(cmd).readlines()
	if len(b) == 0:
	    log("cannot get stat of %s:%s" % (host, filename))
	    return None
	else:
	    b = b[0].split()
	    return (int(b[0]), int(b[1]))

    def brandFile(self, host, filename):
	self.connectHost(host)
	cmd =  "/usr/bin/ssh -x %s %s " % (self.SSH_OPT, host)
	cmd += "'/usr/bin/touch -a %s 2>/dev/null'" % filename
	debug(cmd)
	try:
	    os.popen(cmd)
	except Exception, e:
	    debug("cannot set atime of %s over ssh: %s" % (filename, str(e)))

    def copyFile(self, host, source, destination):
	self.connectHost(host)
        cmd = "/usr/bin/scp %s %s:%s %s" % (self.SCP_OPT, host, source, destination)
	r = self.execWithFlock(destination, cmd)
	if self.config.SLOW_COPY:
	    time.sleep(10)
	return r

class SshMasterConnection:
    SSH_MASTER = [ "/usr/bin/ssh", "-M", "-N" ] + SshRemoteFileSystem.SSH_OPT.split()
    def __init__(self, host):
	try:
	    debug(" ".join(self.SSH_MASTER))
	    self.process = subprocess.Popen(self.SSH_MASTER + [ host ], shell=False,
		    stdout=subprocess.PIPE)
	except Exception, e:
	    debug("ssh master failed: " + str(e))
	    self.process = None

    def __del__(self):
	try:
	    if self.process:
		debug("terminating master connection: " + str(self.process.pid))
		os.kill(self.process.pid, signal.SIGTERM)
	except Exception, e:
	    debug("connection termination failed: " + str(e))

