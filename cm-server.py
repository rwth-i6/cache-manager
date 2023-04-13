#!/usr/bin/env python3
"""
cache management server
supervises load balanced file caching on local harddisks
"""

import sys
import socket
import threading
import copy
import time
import cPickle as cpickle
import random
import signal
import gzip
from shared import Message, Configuration, Connection
from cmlogging import *

__version__ = "$Rev: 831 $"
__author__  = "rybach@cs.rwth-aachen.de (David Rybach)"
__copyright__ = "Copyright 2012, RWTH Aachen University"

class ServerConfiguration (Configuration):
    # master port
    PORT                = 10322
    # queue of the server socket
    CONNECTION_QUEUE    = 32
    # maximum number of parallel transfers from/to file servers
    MAX_COPY_SERVER     = 20
    # maximum number of parallel transfers from/to nodes
    MAX_COPY_NODE       = 1
    # database persistence file
    DB_FILE             = "/u/rybach/temp/test.db"
    # interval between database writes (seconds)
    DB_SAVE_INTERVAL    = 60
    # interval between statistics writes (seconds)
    STAT_INTERVAL       = 10
    # interval between database cleanups (seconds)
    CLEANUP_INTERVAL    = 60
    # timeout for client sockets (seconds)
    SOCKET_TIMEOUT      = 30*60.0
    # maximum time a client may spend copying (seconds)
    MAX_WAIT_COPY       = 10*60
    # time a client has to wait before next copy attempt (seconds)
    CLIENT_WAIT         = 10
    # time after a record in the database is deleted (seconds)
    MAX_AGE             = 60 * 60 * 24 * 14


class CopyCounter:

    def __init__(self, config):
        self.config = config
        self.counters = {}
	self.activeTransfers = {}
        self.lock = threading.Lock()

    def _startCopy(self, host, destNode, file):
        i = self.counters[host]
        debug(str(i))
        self._cleanup(i)
	self._cleanupActiveTransfers(destNode, False)
        if i[0] <= 0:
            r = 0
	elif destNode in self.activeTransfers and file in self.activeTransfers[destNode]:
	    r = 0
        else:
            token = int(time.time())
            i[0] -= 1
            i[1].append(token)
            r = token
	    if (self.activeTransfers[destNode].has_key(file)):
		error("copy constrained violated: %s %s %s" % (destNode, file, str(token)))
	    self.activeTransfers[destNode][file] = token
        debug("end startCopy: => %s" % str(r))
        return r

    def _cleanup(self, item):
        curTime = time.time()
        new = []
        for i in item[1]:
            if curTime - i > self.config.MAX_WAIT_COPY:
                item[0] += 1
                debug("removed waiting")
            else:
                new.append(i)
        item[1] = new

    def _cleanupActiveTransfers(self, destNode, removeNode):
	if not destNode in self.activeTransfers:
	    return
	curTime = time.time()
	try:
	    transfers = self.activeTransfers[destNode]
	    keys = transfers.keys()
	    for f in keys:
		if curTime - transfers[f] > self.config.MAX_WAIT_COPY:
		    del transfers[f]
	    if removeNode and not transfers:
		del self.activeTransfers[destNode]
	except KeyError: pass

    def hasAvailableSlots(self, host):
	self.lock.acquire()
	try:
	    r = True
	    if host in self.counters and self.counters[host][0] <= 0:
		r = False
	finally:
	    self.lock.release()
	return r

    def isActiveTransfer(self, destNode, file):
	self.lock.acquire()
	try:
	    self._initActiveTransfer(destNode)
	    self._cleanupActiveTransfers(destNode, True)
	    r = False
	    if (destNode in self.activeTransfers) and (file in self.activeTransfers[destNode]):
		r = True
	finally:
	    self.lock.release()
	debug("isActiveTransfer %s %s %s" % (destNode, file, str(r)))
	return r

    def startCopyFromNode(self, host, destNode, file):
        self.lock.acquire()
	try:
	    self._initCounter(host, destNode, self.config.MAX_COPY_NODE)
	    self._initActiveTransfer(destNode)
	    r = self._startCopy(host, destNode, file)
	finally:
    	    self.lock.release()
	return r

    def startCopyFromServer(self, host, destNode, file):
        self.lock.acquire()
	try:
	    self._initCounter(host, destNode, self.config.MAX_COPY_SERVER)
	    self._initActiveTransfer(destNode)
	    r = self._startCopy(host, destNode, file)
	finally:
	    self.lock.release()
	return r

    def endCopy(self, host, destNode, token):
        debug("endCopy: %s %s %d" % (host, destNode, token))
        self.lock.acquire()
	try:
	    self.counters[host][0] += 1
	    try:
		self.counters[host][1].remove(token)
	    except ValueError: pass
	    debug(str(self.counters[host]))
	    try:
		transfers = self.activeTransfers[destNode]
		toDelete = []
		for f in transfers:
		    if transfers[f] == token:
			toDelete.append(f)
		for f in toDelete:
		    del transfers[f]
		# debug("transfers[%s]: %s" % (destNode, str(transfers)))
		if not transfers:
		    del self.activeTransfers[destNode]
	    except KeyError: pass
	finally:
	    self.lock.release()

    def updateToken(self, host, destNode, token):
	debug("updateToken: %s %s %s" % (host, destNode, str(token)))
	self.lock.acquire()
	newToken = int(time.time())
	try:
	    self.counters[host][1].remove(token)
	    self.counters[host][1].append(newToken)
	    transfers = self.activeTransfers[destNode]
	    for f in transfers:
		if transfers[f] == token:
		    transfers[f] = newToken
		    break
	except Exception, e:
	    debug("update counters failed: %s" % str(e))
	    newToken = token
	finally:
	    self.lock.release()
	return newToken

    def _initActiveTransfer(self, destNode):
	if not self.activeTransfers.has_key(destNode):
	    self.activeTransfers[destNode] = {}

    def _initCounter(self, host, destNode, maxCopy):
        if not self.counters.has_key(host):
            self.counters[host] = [maxCopy, [] ]



class Location:
    def __init__(self, path, size, mtime, host):
        self.path = path
        self.size = size
        self.mtime = mtime
        self.host = host

    def __eq__(self, other):
        if other == None: return False
        return self.path == other.path and self.size == other.size and \
               self.mtime == other.mtime and self.host == other.host

    def __str__(self):
        return str([self.path, self.size, self.mtime, self.host])


class FileDatabaseRecord:
    def __init__(self, loc, atime = int(time.time())):
        self.loc = loc
        self.atime = atime

    def __len__(self):
        return len(self.loc)

    def __iter__(self):
        return self.loc.__iter__()

    def __getitem__(self, key):
        return self.loc[key]

    def __setitem__(self, key, value):
        self.loc[key] = value

    def __contains__(self, item):
        return self.loc.__contains__(item)

    def append(self, item):
        self.loc.append(item)

    def remove(self, item):
        self.loc.remove(item)

    def access(self):
        self.atime = int(time.time())

class FileDatabase:

    def __init__(self):
        self.lock = threading.Lock()
        self.files = {}
        self.changed = False

    def hasFile(self, filename):
        self.lock.acquire()
        r = self.files.has_key(filename)
        self.lock.release()
        return r

    def hasChanged(self):
        return self.changed

    def getLocation(self, filename, preferedHost = "", counter = None):
        r = None
        self.lock.acquire()
        if not self.files.has_key(filename):
            self.lock.release()
            return None
	record = self.files[filename]
        nFiles = len(record)
        if nFiles == 0:
            self.lock.release()
            return None
        if preferedHost != "":
	    for l in record:
                if l.host == preferedHost:
                    r = l
        if r == None:
	    if counter and nFiles > 1:
		locList = filter(lambda l: counter.hasAvailableSlots(l.host), record)
		if locList:
		    r = locList[random.randint(0, len(locList)-1)]
	if r == None:
	    r = record[random.randint(0, nFiles-1)]
        record.access()
        self.lock.release()
        return r

    def getAllLocations(self, filename):
        r = []
        self.lock.acquire()
        if (not self.files.has_key(filename)) or (len(self.files[filename]) == 0):
            self.lock.release()
            return r
        r = self.files[filename]
        self.files[filename].access()
        self.lock.release()
        return r

    def addLocation(self, filename, location):
        debug("addLocation: %s %s" % (filename, str(location)))
        self.lock.acquire()
        if not self.files.has_key(filename):
            self.files[filename] = FileDatabaseRecord([], int(time.time()))
        if not location in self.files[filename]:
            self.files[filename].append(location)
            self.changed = True
        self.files[filename].access()
        self.lock.release()

    def removeLocation(self, filename, location):
        debug("removeLocation: %s %s" % (filename, str(location)))
        self.lock.acquire()
        if self.files.has_key(filename):
            try:
                self.files[filename].remove(location)
            except ValueError: pass
            if len(self.files[filename]) == 0:
                del self.files[filename]
            self.changed = True
        self.lock.release()

    def write(self, filename):
        self.lock.acquire()
        if not self.changed:
            self.lock.release()
            return True
	# pickle a copy of the actual data such
	# that the lock is released early
	dbcopy = copy.copy(self.files)
	self.lock.release()
        f = gzip.open(filename, 'wb')
        if not f:
            return False
        try:
            cpickle.dump(dbcopy, f)
	    debug("wrote database. %d files" % len(dbcopy))
            self.changed = False
	except Exception:
	    return False
	return True

    def loadPlain(self, filename):
	fd = open(filename, 'rb')
	return cpickle.load(fd)

    def loadCompressed(self, filename):
	fd = gzip.open(filename, 'rb')
	return cpickle.load(fd)

    def load(self, filename):
	db = None
	try:
	    try:
		db = self.loadCompressed(filename)
	    except IOError:
		debug("loadCompressed failed")
		db = self.loadPlain(filename)
	except Exception, e:
	    warning("cannot open database file %s: %s" % (filename, str(e)))
	if not db:
	    return False
        self.lock.acquire()
	self.files = db
	converted = False
	for f in self.files.keys():
	    if type(self.files[f]) == list:
		self.files[f] = FileDatabaseRecord(self.files[f])
		converted = True
	if converted:
	    log("converted database")
	debug("%d files" % len(self.files))
        self.lock.release()
        return True

    def getStat(self):
        self.lock.acquire()
        numFiles = len(self.files)
        numLoc = 0
        if numFiles:
            # numLoc = reduce(lambda x,y: x+y, [ len(i) for i in self.files.values() ])
            for i in self.files.values():
                numLoc += len(i)
        self.lock.release()
        return (numFiles, numLoc)

    def removeOldRecords(self, minATime):
        removed = 0
        self.lock.acquire()
        for f in self.files.keys():
            if self.files[f].atime < minATime:
                del self.files[f]
                removed += 1
        if removed:
            self.changed = True
        self.lock.release()
        log("removed %d records" % removed)


class DatabaseWriter (threading.Thread):

    def __init__(self, db, dbFile, saveInterval):
        self.db = db
        self.dbFile = dbFile
        self.saveInterval = saveInterval
        self.finished = threading.Event()
        threading.Thread.__init__(self)

    def run(self):
        while not self.finished.isSet():
            try:
                self.db.write(self.dbFile)
            except Exception, e:
                log("error writing database to %s: %s" % (self.dbFile, str(e)))
            self.finished.wait(self.saveInterval)

    def stop(self):
        self.finished.set()


class ClientThread (threading.Thread):

    def __init__(self, config, connection, clientAddress, db, copycount, stat):
        threading.Thread.__init__(self)
        self.config = config
        self.conn = connection
        self.clientIP = clientAddress
        self.db = db
        self.copycount = copycount
        self.stat = stat
        self.clientName = ""

    def run(self):
        try:
            self.stat.inc("threads")
            self.stat.inc("requests")
            debug("starting client thread for " + str(self.clientIP))
            self.clientName = socket.gethostbyaddr(self.clientIP[0])[0].split(".")[0]
            debug("clientName = " + str(self.clientName))
            disconnect = False
            keepAlive = False
            while not disconnect:
                disconnect = not keepAlive
                msg = self.conn.receiveMessage()
                if msg == None:
                    debug("client died")
                    disconnect = True
                elif msg.type == Message.REQUEST_FILE:
                    retry = self.handleFileRequest(msg)
                    while retry:
                        debug("retry!")
                        msg = self.conn.receiveMessage()
                        if msg != None:
                            retry = self.handleFileRequest(msg)
                        else:
                            retry = False
                elif msg.type == Message.GET_LOCATIONS:
                    self.handleGetLocations(msg)
                elif msg.type == Message.HAVE_FILE:
                    self.handleHaveFile(msg)
                elif msg.type == Message.DELETED_COPY:
                    self.handleDeletedFile(msg)
                    disconnect = False
		elif msg.type == Message.IS_ACTIVE:
		    self.handleIsActive(msg)
		    disconnect = False
                elif msg.type == Message.KEEP_ALIVE:
                    keepAlive = True
                    disconnect = False
                elif msg.type == Message.EXIT:
                    debug("client send exit")
                    disconnect = True
                elif msg.type == Message.REGISTER_COPY:
                    retry = self.handleRegisterCopy(msg)
                    while retry:
                        debug("retry register copy")
                        msg = self.conn.receiveMessage()
                        retry = self.handleRegisterCopy(msg)
        finally:
            del self.conn
            debug("connection to " + self.clientName + " closed")
            self.stat.dec("threads")

    def handleHaveFile(self, msg):
        debug("handleHaveFile: " + str(msg))
        assert(msg.type == Message.HAVE_FILE)
        newloc = Location(msg.content[3], msg.content[1], msg.content[2], self.clientName)
        self.db.addLocation(msg.content[0], newloc)

    def handleIsActive(self, msg):
	debug("handleIsActive: " + str(msg))
	assert(msg.type == Message.IS_ACTIVE)
	dest = msg.content[0]
	if self.copycount.isActiveTransfer(self.clientName, dest):
	    reply = Message(Message.WAIT, [ str(self.config.CLIENT_WAIT) ])
	else:
	    reply = Message(Message.FILE_OK)
	if not self.conn.sendMessage(reply):
	    debug("client died")

    def handleDeletedFile(self, msg):
        debug("handleDeletedFile: " + str(msg))
        assert(msg.type == Message.DELETED_COPY)
        loc = Location(msg.content[3], msg.content[1], msg.content[2], self.clientName)
        self.db.removeLocation(msg.content[0], loc)

    def handleGetLocations(self, msg):
        debug("handleGetLocations: " + str(msg))
        assert(msg.type == Message.GET_LOCATIONS)
        requestedFile = msg.content
        locateLimit   = int(msg.content[3])
        if not self.db.hasFile(requestedFile[0]):
            debug("file not found in db: %s" % requestedFile[0])
            self.conn.sendMessage(Message(Message.EXIT, []))
        else:
            locations = self.db.getAllLocations(requestedFile[0])
            debug("%d locations found" % len(locations))

            foundCounter = 0
            for loc in locations:
                if loc.host == self.clientName:
                    found = self.checkLocal(loc, requestedFile)
                    foundCounter += 1
                else:
                    found, abort = self.checkRemote(loc, requestedFile)
                    foundCounter += 1
                    if abort:
                        debug("client died")
                        return
                if foundCounter == locateLimit: break
            self.conn.sendMessage(Message(Message.EXIT, []))

    def handleRegisterCopy(self, msg):
        debug("handleRegisterCopy: " + str(msg))
        assert(msg.type == Message.REGISTER_COPY)
        retval = False
        fileinfo   = msg.content[0:3]
        fileserver = msg.content[3]
        copyToken = self.copycount.startCopyFromServer(fileserver, self.clientName, fileinfo[0])
        debug("copyToken: %d" % copyToken)
        if copyToken == 0:
            if not self.conn.sendMessage(Message(Message.WAIT, [ str(self.config.CLIENT_WAIT) ])):
                debug("client died")
            else:
                retval = True
        else:
            if not self.conn.sendMessage(Message(Message.FILE_OK)):
                debug("client died")
                self.copycount.endCopy(fileserver, self.clientName, copyToken)
            else:
		msg, copyToken = self.waitForClient(fileserver, self.clientName, copyToken)
                self.copycount.endCopy(fileserver, self.clientName, copyToken)
                if msg == None:
                    debug("client died")
                elif msg.type == Message.COPY_OK:
                    newloc = Location(fileinfo[0], fileinfo[1], fileinfo[2], self.clientName)
                    self.db.addLocation(msg.content[0], newloc)
                else:
                    debug("copy failed")
        debug("retval: " + str(retval))
        return retval

    def handleFileRequest(self, msg):
        debug("handleFileRequest: " + str(msg))
        assert(msg.type == Message.REQUEST_FILE)
        requestedFile = msg.content
	localDestination = requestedFile[4]
        found = False
        wait  = False
	forceWait = False
        while True:
            loc = self.findLocation(requestedFile)
            debug("loc: " + str(loc))
	    if self.copycount.isActiveTransfer(self.clientName, localDestination):
		debug("currently active transfer. send wait")
		forceWait = wait = True
	    elif loc != None:
		forceWait = False
                if loc.host == self.clientName:
                    found = self.checkLocal(loc, requestedFile)
                else:
                    found, abort = self.checkRemote(loc, requestedFile)
                    debug("checkRemote -> found=%s, abort=%s" % (found, abort))
                    if not abort and found:
                        found, wait = self.copyFromRemote(loc, requestedFile)
            else:
		forceWait = False
                break
            debug("found: " + str(found))
            debug("wait:  " + str(wait))
            if found or forceWait: break
        if (not found) or wait:
            # if file was not found on a node or if we would have to wait for it,
            # check if we can get it without waiting from the server
	    if not forceWait:
		found, wait = self.copyFromOrigin(requestedFile)
		if not found:
		    log("copyFromOrigin failed: " + requestedFile[0])
		    self.conn.sendMessage(Message(Message.FALLBACK))
            if wait:
                debug("send wait")
                self.conn.sendMessage(Message(Message.WAIT, [ str(self.config.CLIENT_WAIT) ] ))
        return wait

    def findLocation(self, requestedFile):
        location = None
        if self.db.hasFile(requestedFile[0]):
            debug("have file")
            location = None
            l = self.db.getLocation(requestedFile[0], self.clientName, self.copycount)
            while l != None:
                debug("location: " + str(l))
                if l.size != requestedFile[1] or int(float(l.mtime)) != int(float(requestedFile[2])):
                    debug("invalid location")
                    self.db.removeLocation(requestedFile[0], l)
                else:
                    location = l
                    break
                l = self.db.getLocation(requestedFile[0], self.clientName, self.copycount)

            if location == None:
                debug("no location found")
        return location

    def checkLocal(self, loc, requestedFile):
        debug("checkLocal")
        self.conn.sendMessage(Message(Message.CHECK_LOCAL, [ loc.path ]))
        debug("send")
        msg = self.conn.receiveMessage()
        debug("recv: " + str(msg))
        if msg == None:
            return True # client died, don't care
        if msg.type != Message.FILE_OK:
            debug("local file invalid. remove " + loc.path)
            self.db.removeLocation(requestedFile[0], loc)
            return False
        return True

    def checkRemote(self, loc, requestedFile):
        """ return (found, abort) """
        debug("check remote")
        self.conn.sendMessage(Message(Message.CHECK_REMOTE, [ loc.host, loc.path ]))
        debug("send")
        msg = self.conn.receiveMessage()
        debug("recv: " + str(msg))
        if msg == None:
            return (True, True)
        if msg.type != Message.FILE_OK:
            debug("remote file invalid. remove " + loc.path)
            self.db.removeLocation(requestedFile[0], loc)
            return (False, False)
        else:
            return (True, False)

    def waitForClient(self, host, destNode, copyToken):
	""" wait until the client finished copying.
	returns (last_message, copyToken )"""
	tokenRefreshInterval = self.config.MAX_WAIT_COPY / 2
        msg = self.conn.receiveMessage()
	debug("recv: " + str(msg))
	while msg and msg.type == Message.PING:
	    if (time.time() - copyToken) > tokenRefreshInterval:
		# prevent token from expiring, for slow copies
		copyToken = self.copycount.updateToken(host, destNode, copyToken)
	    msg = self.conn.receiveMessage()
	    debug("recv ping: " + str(msg))
        debug("end copy: " + str(msg))
	return (msg, copyToken)

    def copyFromRemote(self, loc, requestedFile):
        """ return (copyOk, wait) """
	debug("copy from remote -> %s:%s" % (self.clientName, requestedFile[4]))
        cnt = 0
        copyToken = self.copycount.startCopyFromNode(loc.host, self.clientName, requestedFile[4])
        if copyToken == 0:
            self.stat.inc("wait")
            return (True, True)
        debug("start copy")
        self.conn.sendMessage(Message(Message.COPY_FROM_NODE, [ loc.host, loc.path ]))
	msg, copyToken = self.waitForClient(loc.host, self.clientName, copyToken)
        if msg == None:
            self.stat.inc("aborted")
            r = (True, False)
        elif msg.type == Message.COPY_OK:
            debug("COPY OK")
            newloc = copy.copy(loc)
            newloc.host = self.clientName
            newloc.path = msg.content[0]
            self.db.addLocation(requestedFile[0], newloc)
            self.stat.inc("copyFromNode")
            r = (True, False)
        else:
            debug("copy failed")
            #TODO: don't remove location if there is just not enogh disk space!
            #DONE: disk space is checked before sending the request
            self.db.removeLocation(requestedFile[0], loc)
            r = (False, False)
        self.copycount.endCopy(loc.host, self.clientName, copyToken)
	return r

    def copyFromOrigin(self, requestedFile):
	debug("copy from origin -> %s:%s" % (self.clientName, requestedFile[4]))
        cnt = 0
        fileserver = requestedFile[3]
        if fileserver == "": fileserver = "unknown"
        copyToken = self.copycount.startCopyFromServer(fileserver, self.clientName, requestedFile[4])
        if copyToken == 0:
            self.stat.inc("wait")
            return (True, True)
        debug("start copy")
        self.conn.sendMessage(Message(Message.COPY_FROM_SERVER))
	msg, copyToken = self.waitForClient(fileserver, self.clientName, copyToken)
        if msg == None:
            self.stat.inc("aborted")
            r = (True, False)
        elif msg.type == Message.COPY_OK:
            newloc = Location(msg.content[0], requestedFile[1], requestedFile[2], \
                                           self.clientName)
            self.db.addLocation(requestedFile[0], newloc)
            self.stat.inc("copyFromServer")
            r = (True, False)
        else:
            r = (False, False)
	# release lock on local copy _after_ adding the new location to the DB
	# such that parallel running clients on the same node use the existing
	# copy from now on
        self.copycount.endCopy(fileserver, self.clientName, copyToken)
	return r


class Statistics:

    def __init__(self):
        self.lock = threading.Lock()
        self.threads  = 0
        self.requests = 0
        self.copyFromServer = 0
        self.copyFromNode   = 0
        self.aborted = 0
        self.changed = False
        self.wait = 0

    def inc(self, attr):
        self.lock.acquire()
        val = getattr(self, attr)
        setattr(self, attr, val + 1)
        self.changed = True
        self.lock.release()

    def dec(self, attr):
        self.lock.acquire()
        val = getattr(self, attr)
        setattr(self, attr, val - 1)
        self.changed = True
        self.lock.release()

    def hasChanged(self):
        self.lock.acquire()
        r = self.changed
        self.lock.release()
        return r

    def get(self):
        self.lock.acquire()
        r = copy.copy(self)
        self.changed = False
        self.lock.release()
        del r.lock
        return r

class StatisticsWriter (threading.Thread):

    def __init__(self, stat, db, interval):
        threading.Thread.__init__(self)
        self.stat = stat
        self.db = db
        self.interval = interval
        self.finished = threading.Event()

    def run(self):
        while not self.finished.isSet():
            if self.stat.hasChanged():
                stat = self.stat.get()
                db   = self.db.getStat()
                total = max(1, stat.copyFromServer + stat.copyFromNode)
                if db[0] > 0:
                    locationsPerFile = db[1]/float(db[0])
                else:
                    locationsPerFile = 0
                log("statistics at " + time.strftime("%Y-%m-%d %H:%M:%S") + "\n" + \
                    "     requests:       %d\n" % stat.requests +\
                    "     active threads: %d\n" % stat.threads +\
                    "     server copy:    %d = %0.2f\n" % (stat.copyFromServer,\
                                                         stat.copyFromServer*100.0/total) +\
                    "     node copy:      %d = %0.2f\n" % (stat.copyFromNode, \
                                                         stat.copyFromNode*100.0/total) +\
                    "     waits:          %d\n" % stat.wait +\
                    "     aborted:        %d\n" % stat.aborted +\
                    "     files:          %d\n" % db[0] +\
                    "     locations:      %d = %0.2f per file\n" % (db[1], locationsPerFile))

            self.finished.wait(self.interval)

    def stop(self):
        self.finished.set()


class DatabaseCleaner (threading.Thread):

    def __init__(self, db, interval, maxAge):
        threading.Thread.__init__(self)
        self.db = db
        self.maxAge = maxAge
        self.interval = interval
        self.finished = threading.Event()

    def run(self):
        while not self.finished.isSet():
            self.db.removeOldRecords(int(time.time()) - self.maxAge)
            self.finished.wait(self.interval)

    def stop(self):
        self.finished.set()


class SignalException (BaseException):
    def __init__(self, signum):
        self.signum = signum

    def __str__(self):
        return "SignalException signal=%d" % self.signum

    @staticmethod
    def handler(signal, frame):
        raise SignalException(signal)

def main(argc, argv):
    config = ServerConfiguration()
    # LogLevel.enableDebug()
    if argc > 1:
        config.read(argv[1])
    else:
        log("no config file. using defaults")
    try:
        serverSocket= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        debug("trying to bind to port %d" % config.PORT)
        serverSocket.bind(('', config.PORT))
        serverSocket.listen(config.CONNECTION_QUEUE)
        log("listening on port: %d" % config.PORT)
    except Exception, e:
        error("cannot create server socket: %s" % str(e))
        return 1
    filedb = FileDatabase()
    if filedb.load(config.DB_FILE):
        log("loaded file database from %s" % config.DB_FILE)
    writer = DatabaseWriter(filedb, config.DB_FILE, config.DB_SAVE_INTERVAL)
    dbCleaner = DatabaseCleaner(filedb, config.CLEANUP_INTERVAL, config.MAX_AGE)
    copycount = CopyCounter(config)
    stat = Statistics()
    statWriter = StatisticsWriter(stat, filedb, config.STAT_INTERVAL)

    signal.signal(signal.SIGTERM, SignalException.handler)

    try:
        try:
            writer.start()
            dbCleaner.start()
            statWriter.start()
            while True:
		startClientThread = True
		clientSocket = None
		try:
    		    clientSocket, clientAddress = serverSocket.accept()
		except Exception, e:
		    error("socket accept failed: %s" % str(e))
		    del clientSocket
		    startClientThread = False
		if startClientThread:
		    clientSocket.settimeout(config.SOCKET_TIMEOUT)
		    clientConnection = Connection(clientSocket, clientAddress)
		    clientThread = ClientThread(config, clientConnection, clientAddress, filedb, copycount, stat)
		    clientThread.start()
        finally:
            serverSocket.close()
            writer.stop()
            dbCleaner.stop()
            statWriter.stop()
            if filedb.lock.acquire(False):
                filedb.lock.release()
                filedb.write(config.DB_FILE)
            log("exit")
    except (SignalException, KeyboardInterrupt), e:
        debug("Interrupt: %s" % str(e))
        pass
    return 0

if __name__ == "__main__":
    sys.exit( main(len(sys.argv), sys.argv) )

