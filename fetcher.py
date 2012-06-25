# fetcher.py
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

"""
file fetcher for cache manager client
"""

import os.path
import time
import threading
from shared import Message
from logging import *
import settings

__version__ = "$Rev$"
__author__  = "rybach@cs.rwth-aachen.de (David Rybach)"
__copyright__ = "Copyright 2012, RWTH Aachen University"


class PingThread (threading.Thread):
    """Send an empty message to the server to keep the connection
    alive during long copies"""

    def __init__(self, conn, interval):
        threading.Thread.__init__(self)
        self.conn = conn
	self.interval = interval
        self.finished = threading.Event()

    def run(self):
        while not self.finished.isSet():
	    try:
		self.finished.wait(self.interval)
		if not self.finished.isSet():
		    self.conn.sendMessage(Message(Message.PING, []))
	    except Exception, e:
		warning("exception in PingThread: %s" % str(e))
		break

    def stop(self):
        self.finished.set()

class CacheFetcher:

    def __init__(self, config, fileSystem, connection):
        self.config = config
        self.fileSystem = fileSystem
        self.remoteSystem = settings.clientEnvironment().remoteFileSystem(config)
        self.conn = connection

    def brandFile(self, host, filename):
        debug("brandFile: %s:%s" % (host, filename))
        self.remoteSystem.brandFile(host, filename)

    def checkRemote(self, originalFileInfo, host, filename):
        debug("checkRemote: " + filename)
        log("checking status of %s:%s" % (host, filename))
        stat = self.remoteSystem.getFileStat(host, filename)
        debug("stat:     %s" % str(stat))
        debug("original: %s" % str(originalFileInfo))
        if stat == None:
            return False
        return (int(float(originalFileInfo[1])) == stat[0] and \
                int(float(originalFileInfo[2])) == stat[1])

    def checkLocal(self, originalFileInfo, filename):
        debug("checkLocal " + filename)
        if not os.path.isfile(filename):
            debug("file not found")
            return False
        checkFileInfo = self.fileSystem.getFileInfo(filename)
        debug("local: " + str(checkFileInfo))
        return (int(originalFileInfo[1]) == int(checkFileInfo[1]) and \
                int(float(originalFileInfo[2])) == int(float(checkFileInfo[2])))

    def startPingThread(self):
	pt = PingThread(self.conn, self.config.SOCKET_TIMEOUT / 2)
	pt.start()
	return pt

    def copyFromNode(self, fileinfo, host, filename, destination):
        debug("copy from node: %s, %s, %s" % (host, filename, destination))
        log("start copying %s:%s" % (host, filename))
	pt = self.startPingThread()
        copyOK, msg = self.remoteSystem.copyFile(host, filename, destination)
	pt.stop()
	del pt
        if not copyOK:
            log("%s" % msg)
            error("cannot copy %s:%s to %s" % (host, filename, destination))
            return False
        else:
            log("copied %s:%s" % (host, filename))
            self.fileSystem.setATime(destination)
            return True

    def copyFromServer(self, fileinfo, destination):
        filename = fileinfo[0]
        debug("copy from server: %s, %s" % (filename, destination))
        log("start copying %s" % filename)
        try:
            # shutil.copy2(filename, destination)
	    pt = self.startPingThread()
	    copyOk, msg = self.remoteSystem.copyUsingCp(filename, destination)
	    pt.stop()
	    del pt
	    if not copyOk:
		raise Exception(msg)
            log("copied %s" % filename)
            self.fileSystem.setATime(destination)
            return True
        except Exception, e:
            error("cannot copy %s to %s: %s" % (filename, destination, str(e)))
            return False

    def requestFile(self, fileinfo, destination):
        debug("requestFile: " + str(fileinfo))
        fileserver = self.fileSystem.getFileServer(fileinfo[0])
        debug("file server: " + fileserver)
        return self.conn.sendMessage(Message(Message.REQUEST_FILE, fileinfo + [fileserver, destination]))

    def requestFileLocations(self, fileinfo):
        debug("requestFileLocations: " + str(fileinfo))
        return self.conn.sendMessage(Message(Message.GET_LOCATIONS, fileinfo))

    def sendFileLocation(self, fileinfo, destination):
        debug("sendFileLocation: %s, %s" % (str(fileinfo), destination))
        r = self.conn.sendMessage(Message(Message.HAVE_FILE, fileinfo + [destination]))
        debug(" => " + str(r))

    def sendFileRemoved(self, fileinfo, destination):
        debug("sendFileRemoved: %s, %s" % (str(fileinfo), destination))
        r = self.conn.sendMessage(Message(Message.DELETED_COPY, fileinfo + [destination]))
        debug(" => " + str(r))

    def sendExit(self):
        debug("sendExit")
        r = self.conn.sendMessage(Message(Message.EXIT, []))
        debug(" => " + str(r))

    def sendKeepAlive(self):
        debug("sendKeepAlive")
        r = self.conn.sendMessage(Message(Message.KEEP_ALIVE, []))
        debug(" => " + str(r))

    def isActive(self, destination):
        """ return: waiting time """
	debug("isActive: %s" % destination)
	r = self.conn.sendMessage(Message(Message.IS_ACTIVE, [destination]))
        msg = self.conn.receiveMessage()
	if not msg:
	    error("connection reset")
	    return 0
	elif msg.type == Message.WAIT:
	    return int(msg.content[0])
	else:
	    return 0


    def handleMessage(self, fileinfo, destination, msg):
        """ return (retFile, retval, terminate) """
        debug("handleMessage %s" % str(msg))
        debug("destination = %s" % destination)
        retval = False
        retFile = None
        reply = None
        terminate = False
        if msg == None:
            error("no connection to master")
            retFile = fileinfo[0]
            retval = True
        elif msg.type == Message.CHECK_LOCAL:
            if self.checkLocal(fileinfo, msg.content[0]):
                reply = Message(Message.FILE_OK)
                log("using existing copy %s" % msg.content[0])
                retFile = msg.content[0]
                retval = True
            else:
                reply = Message(Message.FILE_NOT_OK)
        elif msg.type == Message.CHECK_REMOTE:
            if self.checkRemote(fileinfo, msg.content[0], msg.content[1]):
                self.brandFile(msg.content[0], msg.content[1])
                reply = Message(Message.FILE_OK)
                retFile = (msg.content[0], msg.content[1])
            else:
                reply = Message(Message.FILE_NOT_OK)
        elif msg.type == Message.COPY_FROM_NODE:
            if self.copyFromNode(fileinfo, msg.content[0], msg.content[1], destination):
                reply = Message(Message.COPY_OK, [destination])
                debug("OK")
                retFile = destination
                retval = True
            else:
                reply = Message(Message.COPY_FAILED)
        elif msg.type == Message.COPY_FROM_SERVER:
            if self.copyFromServer(fileinfo, destination):
                reply = Message(Message.COPY_OK, [destination])
                debug("OK")
                retFile = destination
                retval = True
            else:
                reply = Message(Message.COPY_FAILED, [])
        elif msg.type == Message.FALLBACK:
            log("no local cache available")
            retFile = fileinfo[0]
            retval = True
        elif msg.type == Message.WAIT:
            log("no copy slot available. waiting.")
            time.sleep(int(msg.content[0]))
            self.requestFile(fileinfo, destination)
        elif msg.type == Message.EXIT:
            debug("exit received")
            terminate = True
            retval = True
        else:
            error("unknown message received: %d" % msg.type)
            retFile = fileinfo[0]
            retval = True
        debug("reply: " + str(reply))
        debug("retval: " + str(retval))
        debug("retFile: " + str(retFile))
        if reply != None:
            self.conn.sendMessage(reply)
        return (retFile, retval, terminate)
