# client.py
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
CacheManager client implementation
"""

import socket
import os, time
import shutil
from shared import Message, Connection, Configuration
from logging import *
from filesystem import FileSystem
from fetcher import CacheFetcher
import settings

__version__ = "$Rev$"
__author__  = "rybach@cs.rwth-aachen.de (David Rybach)"
__copyright__ = "Copyright 2012, RWTH Aachen University"

class ClientConfiguration (settings.ClientDefaultConfiguration):
    """configuration of the CacheManager client"""

    def __init__(self, configFile = None):
	if configFile:
	    self.read(configFile)

    def loadDefault(self):
	"""load default configuration file ~/.cmclient"""
        configfile = os.getenv("HOME") + "/.cmclient"
	return os.path.exists(configfile) and self.read(configfile)


class CmClient:
    """ CacheManager Client"""
    def __init__(self, config, single=True):
        """initialize client.
        set single=False if you need to fetch several files.
        """
	self.config = config
	self.single = single
	self.connection = self._connectToServer(config)

    def __del__(self):
	if not self.single:
	    try:
		self.connection.sendMessage(Message(Message.EXIT, []))
	    except Exception:
		pass

    def _connectToServer(self, config):
	try:
	    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	    server = settings.clientEnvironment().server(config)
	    if not server[0] in range(10):
		server = socket.gethostbyname(server)
	    s.connect((server, config.MASTER_PORT))
	    s.settimeout(config.SOCKET_TIMEOUT)
	except Exception, e:
	    error(str(e))
	    return None
	conn = Connection(s)
	if not self.single:
	    r = conn.sendMessage(Message(Message.KEEP_ALIVE, []))
        return conn

    def isConnected(self):
        """return True if the client is connected to the cache manager server"""
	return not self.connection is None

    @staticmethod
    def getDestination(fileSystem, filename):
	debug("cache dir: " + fileSystem.cacheDir)
	destination = os.path.normpath(fileSystem.cacheDir + "/" + filename)
	try:
	    os.makedirs(os.path.dirname(destination), 0755)
	except OSError, (errno, e):
	    if errno != 17:
		error("Cannot create destination directory: " + str(e))
		return None
	return destination

    def fetch(self, filename, forceBundle=False, conjunct=False):
        """request a local copy of a file on a file server.
        return the full path of the local copy. if no local copy
        can be created, the original path is returned.

        filename: requested file
        forceBundle: filename is assumbed to be a bundle file
        conjunct: cache all files of a bundle or nothing

        returns a tuple (f, r) with f the cached filename and
        r = True if the file was successfully cached.
        """
	filename = os.path.realpath(filename)

	if not self.isConnected():
	    return filename, False
	try:
	    if forceBundle or self._isBundleFile(filename):
		f, r = self._fetchBundle(filename, conjunct)
	    else:
		f, r = self._fetchFile(filename)
	    return f, (r == 0)
	except Exception, e:
	    error("unknown error: %s" % str(e))
	return filename, False

    def _fetchFile(self, filename, fileSystem=None, fetcher=None, closeOnError=True):
	if not fileSystem:
	    fileSystem = FileSystem(self.config)
	if not fetcher:
	    fetcher = CacheFetcher(self.config, fileSystem, self.connection)

	if not os.path.isfile(filename):
	    error("file not found '%s'" % filename)
	    if closeOnError and not self.single: fetcher.sendExit()
	    return (filename, 1)

	fileinfo = fileSystem.getFileInfo(filename)
	destination = self.getDestination(fileSystem, filename)
	if destination is None:
	    return (filename, 1)
	log("destination: " + str(destination))

	if os.path.isfile(destination):
	    wait = fetcher.isActive(destination)
	    while wait > 0:
		log("file transfer in progress. wait %ds" % wait)
		time.sleep(wait)
		wait = fetcher.isActive(destination)

	fileExists, canCopy, removed = fileSystem.destinationExists(fileinfo, destination)
	if fileExists:
	    if removed:
		log("removed " + destination)
		fetcher.sendFileRemoved(fileinfo, destination)
	    if not canCopy:
		error("cannot copy file to %s" % destination)
		return (filename, 1)
	    else:
		log("using existing file")
		fileSystem.setATime(destination)
		fetcher.sendFileLocation(fileinfo, destination)
		return (destination, 0)

	freeSpace, removed = fileSystem.checkFreeSpace(int(fileinfo[1]), destination)
	if not freeSpace:
	    log("not enough free space in %s" % fileSystem.cacheDir)
	    if closeOnError and self.single: fetcher.sendExit()
	    return (filename, 1)

	log("request: " + filename)
	fetcher.requestFile(fileinfo, destination)

	msg = self.connection.receiveMessage()
	resultFile, ok, term = fetcher.handleMessage(fileinfo, destination, msg)
	while not ok:
	    msg = self.connection.receiveMessage()
	    resultFile, ok, term = fetcher.handleMessage(fileinfo, destination, msg)
	return (resultFile, 0)


    def _getBundleSourceFiles(self, bundleFile, fileSystem):
	""" return: files, total size """
	srcFiles = []
	totalSize = 0
	for line in file(bundleFile):
	    fileItem = line.strip()
	    if not fileItem:
		continue
	    filename = os.path.realpath(fileItem)
	    fileinfo = fileSystem.getFileInfo(filename)
	    srcFiles.append(filename)
	    if fileinfo:
		totalSize += float(fileinfo[1])
	return srcFiles, totalSize


    def _fetchBundle(self, filename, conjunct):
	filename = os.path.realpath(filename)
	debug("fetchBundle: %s" % filename)
	fileSystem = FileSystem(self.config)
	fetcher = CacheFetcher(self.config, fileSystem, self.connection)

	if not os.path.isfile(filename):
	    error("file not found '%s'" % filename)
	    if self.single: fetcher.sendExit()
	    return (filename, 1)
	destination = self.getDestination(fileSystem, filename)
	debug("destination: " + str(destination))
	while (os.path.isfile(destination)):
	    destination = "%s%s.%0.3f.bundle" % (destination[:-6], socket.gethostname(), float(time.time()))

	try:
	    out = file(destination, "w")
	except Exception, e:
	    error("cannot open destination file %s" % destination)
	    if self.single: fetcher.sendExit()
	    return (filename, 1)

	fetcher.sendKeepAlive()
	isError = False
	nCopyFailed = 0
	srcFiles, totalSize = self._getBundleSourceFiles(filename, fileSystem)

	if conjunct:
	    freeSpace, removed = fileSystem.checkFreeSpace(int(totalSize), destination)
	    if not freeSpace or not srcFiles:
		log("not enough free space in %s" % fileSystem.cacheDir)
		log("result is not cached")
		if self.single: fetcher.sendExit()
		return (filename, 1)
	cachedFiles = []
	dstFiles = []
	for fileItem in srcFiles:
	    dst, retval = self._fetchFile(fileItem, fileSystem, fetcher, False)
	    if retval != 0:
		warning("cannot cache bundle content: %s" % fileItem)
		dst = fileItem
		nCopyFailed += 1
		if conjunct:
		    break
	    else:
		cachedFiles.append(dst)
	    dstFiles.append(dst)
	if self.single: fetcher.sendExit()

	if isError or (nCopyFailed == len(srcFiles)) or (conjunct and len(cachedFiles) != len(srcFiles)):
	    out.close()
	    error("caching of bundle archive failed")
	    os.remove(destination)
	    return (filename, 1)
	else:
	    out.write("\n".join(dstFiles))
	    out.write("\n")
	    out.close()
	    return (destination, 0)

    def getLocations(self, files, locations, forceBundle=False):
        """find local copies of a file.
        files: list of file names
        locations: list with locations (output)
        forceBundle: treat all files as bundle files
        """

	if not self.isConnected():
	    return False
	retval = 0
	debug("getLocations: " + str(files) + " " + str(locations))
	try:
	    fileSystem = FileSystem(self.config)
	    fetcher = CacheFetcher(self.config, fileSystem, self.connection)
	    nFiles = len(files)
	    for i in range(nFiles):
		filename = os.path.realpath(files[i])
		debug("locate " + filename)
		sendExit = (i == (nFiles - 1))
		if forceBundle or self._isBundleFile(filename):
		    loc, retval = self._findBundleLocations(filename, fileSystem, fetcher, sendExit)
		else:
		    loc, retval = self._findLocations(filename, fileSystem, fetcher, sendExit)
		locations += loc
	except Exception, e:
	    error("unknown error: %s" % str(e))
	    retval = 2
	return (retval == 0)

    def _findLocations(self, filename, fileSystem, fetcher, sendExit = True):
	debug("findLocations: " + filename)
	if not os.path.isfile(filename):
	    error("file not found '%s'" % filename)
	    if sendExit and self.single: fetcher.sendExit()
	    return ([], 1)
	fileinfo = fileSystem.getFileInfo(filename)
	debug("fileinfo " + str(fileinfo))
	destination = ""
	locations = []
	hostname = socket.gethostname()
	debug("hostname: " + hostname)
	def appendLocation(h, l, f):
	    if f != None:
		if type(f) == str:
		    l.append((h, f, fileinfo[1]))
		else:
		    l.append((f[0], f[1], fileinfo[1]))
	fetcher.sendKeepAlive()
	fetcher.requestFileLocations(fileinfo)
	msg = self.connection.receiveMessage()
	resultFile, ok, term = fetcher.handleMessage(fileinfo, destination, msg)
	debug("resultFile = %s, ok = %d, term = %d" % (resultFile, ok, term))
	appendLocation(hostname, locations, resultFile)
	while not term:
	    msg = self.connection.receiveMessage()
	    resultFile, ok, term = fetcher.handleMessage(fileinfo, destination, msg)
	    appendLocation(hostname, locations, resultFile)
	    debug("resultFile = %s, ok = %d, term = %d" % (resultFile, ok, term))
	if sendExit and self.single:
	    fetcher.sendExit()
	return (locations, 0)

    def _findBundleLocations(self, filename, fileSystem, fetcher, sendExit):
	debug("findBundleLocations: " + filename)
	allLocations = []

	if not os.path.isfile(filename):
	    error("file not found '%s'" % filename)
	    if sendExit: fetcher.sendExit()
	    return (allLocations, 1)
	fetcher.sendKeepAlive()
	isError = False
	for line in file(filename):
	    fileItem = line.strip()
	    if fileItem == "":
		continue
	    fileItem = os.path.realpath(fileItem)
	    locations, retval = self._findLocations(fileItem, fileSystem, fetcher, False)
	    if retval != 0:
		warning("cannot find bundle content: %s" % fileItem)
	    else:
		allLocations += locations
	if sendExit:
	    fetcher.sendExit()
	return (allLocations, 0)

    def copy(self, filename, destination, register=True, forceBundle=False):
	""" copy a file from the local disk (filename) to a file server (destination).
	filename: source file name
	destination: destination file name
	register: register the local copy in the server database
	forceBundle: the file is assumed a bundle file

	The file is attempted to be copied even if an error occured during the
	communication with the cache manager.

	Returns False if the file was not copied.
	"""
        err = True
	filename = os.path.realpath(filename)
        destination = os.path.realpath(destination)
	if self.isConnected():
	    isBundle = forceBundle or self._isBundleFile(filename)
            err = (self._copyFile(filename, destination, register, isBundle) != 0)
        if err:
            try:
                shutil.copy2(filename, destination)
                log("copied %s to %s" % (filename, destination))
            except Exception, e:
                error("cannot copy %s to %s: %s" % (filename, destination, str(e)))
                return False
        return True

    def _copyFile(self, source, destination, register, isBundle):
	debug("copyFile: %s, %s" % (source, destination))
	if not os.path.isfile(source):
	    error("file not found '%s'" % source)
	    return 1
	debug("isBundle: %d" % isBundle)
	if not isBundle:
	    if os.path.isdir(destination):
		destination = os.path.normpath(destination + "/" + os.path.basename(source))
	    debug("destination: " + destination)
	    if os.path.isfile(destination):
		log("warning: will overwrite %s" % destination)
	else:
	    if not os.path.isdir(destination):
		error("destination is not a directory")
		return 1
	fs = FileSystem(self.config)
	fileinfo = fs.getFileInfo(source)
	debug("fileinfo: " + str(fileinfo))
	fileserver = fs.getFileServer(destination)
	debug("fileserver: " + fileserver)
	request = Message(Message.REGISTER_COPY, fileinfo + [fileserver])
	if not self.connection.sendMessage(request):
	    error("cannot send message")
	    return 1
	retval = 0
	while True:
	    msg = self.connection.receiveMessage()
	    if msg == None:
		error("cannot receive message")
		retval = 1
		break
	    elif msg.type == Message.WAIT:
		time.sleep(int(msg.content[0]))
		if not self.connection.sendMessage(request):
		    error("cannot send message")
		    retval = 1
		    break
	    elif msg.type == Message.FILE_OK:
		if not isBundle:
		    retval, reply = self._copySingleFile(source, destination, register)
		else:
		    retval, reply = self._copyBundleFile(source, destination)
		if not self.connection.sendMessage(reply):
		    error("cannot send message")
		    retval = 1
		break
	    else:
		error("unexpected message: %s" % str(msg))
		retval = 1
		break
	return retval


    def _copySingleFile(self, source, destination, register):
	try:
	    shutil.copy2(source, destination)
	    log("copied %s to %s" % (source, destination))
	    if register:
		reply = Message(Message.COPY_OK, [ destination ])
	    else:
		debug("don't send location")
		reply = Message(Message.COPY_FAILED)
	    retval = 0
	except Exception, e:
	    error("cannot copy %s to %s: %s" % (source, destination, str(e)))
	    reply = Message(Message.COPY_FAILED)
	    retval = 1
	return (retval, reply)


    def _copyBundleFile(self, source, destination):
	debug("copy bundle file")
	retval = 0
	cnt = 0
	fail = 0
	for line in file(source):
	    fileItem = line.strip()
	    if fileItem == "":
		continue
	    fileItem = os.path.realpath(fileItem)
	    destFile = os.path.normpath(destination + "/" + os.path.basename(fileItem))
	    debug("destFile: " + destFile)
	    if os.path.isfile(destFile):
		warning("will overwrite %s" % destFile)
	    try:
		shutil.copy2(fileItem, destFile)
		log("copied %s to %s" % (fileItem, destFile))
		cnt += 1
	    except Exception, e:
		error("cannot copy %s to %s: %s" % (fileItem, destFile, str(e)))
		fail += 1
		retval = 1
	log("copied %d files, %d errors" % (cnt, fail))
	# force a copy failed such that the server does not register
	# the bundle file location as cache copy.
	warning("local copies not registered in database")
	return (retval, Message(Message.COPY_FAILED))


    def _isBundleFile(self, filename):
	return filename.endswith(".bundle") and not self.config.IGNORE_BUNDLE

