"""
network communication classes, configuration and auxilary functions
for cm-client.py and cm-server.py
"""

from cmlogging import *

__version__ = "$Rev: 821 $"
__author__  = "rybach@cs.rwth-aachen.de (David Rybach)"
__copyright__ = "Copyright 2012, RWTH Aachen University"

class Message:
    SIZE_MSG_TYPE = 2
    SIZE_STRLEN   = 4

    REQUEST_FILE     = 1
    CHECK_LOCAL      = 2
    CHECK_REMOTE     = 3
    FILE_OK          = 4
    FILE_NOT_OK      = 9
    COPY_FROM_NODE   = 5
    COPY_FROM_SERVER = 6
    COPY_OK          = 7
    COPY_FAILED      = 8
    FALLBACK         = 10
    HAVE_FILE        = 11
    WAIT             = 12
    REGISTER_COPY    = 13
    DELETED_COPY     = 14
    EXIT             = 15
    KEEP_ALIVE       = 16
    GET_LOCATIONS    = 17
    IS_ACTIVE        = 18
    PING             = 19

    nMessageParts = { REQUEST_FILE     : 6 ,
                      CHECK_LOCAL      : 1 ,
                      CHECK_REMOTE     : 2 ,
                      FILE_OK          : 0 ,
                      FILE_NOT_OK      : 0 ,
                      COPY_FROM_NODE   : 2 ,
                      COPY_FROM_SERVER : 0 ,
                      COPY_OK          : 1 ,
                      COPY_FAILED      : 0 ,
                      FALLBACK         : 0 ,
                      HAVE_FILE        : 4 ,
                      WAIT             : 1 ,
                      REGISTER_COPY    : 4 ,
                      DELETED_COPY     : 4 ,
                      EXIT             : 0 ,
                      KEEP_ALIVE       : 0 ,
                      GET_LOCATIONS    : 4 ,
                      IS_ACTIVE        : 1 ,
                      PING             : 0
                    }

    def __init__(self, type, content = []):
        assert(Message.nMessageParts[type] == len(content))
        self.type = type
        self.content = content

    def __str__(self):
        return str([self.type, self.content])


class Connection:

    def __init__(self, socket, address = ""):
        self.conn = socket
        self.address = address

    def __del__(self):
        self.conn.close()

    def _readAll(self, size):
        # debug("_readAll(%d)" % size)
        result = ""
        while size > 0:
            try:
                buffer = self.conn.recv(size)
                buffer = str(buffer.decode('ascii'))
            except Exception as e:
                error("cannot receive: " + str(e))
                return None
            if buffer == None or buffer == 0 or len(buffer) == 0:
                # error("lost connection to %s" % str(self.address))
                return None
            size -= len(buffer)
            result += buffer
        # debug("_readAll() -> %s" % result)
        return result

    def receiveMessage(self):
        # debug("receiveMessage()")
        try:
            msgType = int(self._readAll(Message.SIZE_MSG_TYPE))
        except TypeError:
            return None
        try:
            nParts = Message.nMessageParts[msgType]
        except KeyError:
            error("unknown message type: '%d'" % msgType)
            return None
        msg = []
        for i in range(nParts):
            len = self._readAll(Message.SIZE_STRLEN)
            if len == None:
                return None
            len = int(len)
            s = self._readAll(len)
            if s == None:
                return None
            msg.append(s)
        # debug("receiveMessage() -> " + str((msgType, msg)))
        return Message(msgType, msg)

    def sendMessage(self, msg):
        # debug("send " + str(msg))
        mbuf = ("%%0%dd" % Message.SIZE_MSG_TYPE) % msg.type
        mbuf = mbuf.encode('ascii')
        if self.conn.sendall(mbuf) == 0:
            error("send message type failed")
            return False
        for m in msg.content:
            l = ("%%0%dd" % Message.SIZE_STRLEN) % len(m)
            l = l.encode('ascii')
            m = m.encode('ascii')
            assert(len(l) <= Message.SIZE_STRLEN)
            try:
                if self.conn.sendall(l) == 0:
                    error("send length failed")
                    return False
                if self.conn.sendall(m) == 0:
                    error("send string failed")
                    return False
            except Exception as e:
                error("send failed: " + str(e))
                return False
        return True

    def getPeerName(self):
       return self.conn.getpeername()

    def close(self):
        self.conn.close()


class Configuration:

    def read(self, filename):
        try:
            f = open(filename, 'r')
        except IOError:
            return False
        for line in f.readlines():
            l = line.strip()
            if l == "" or l[0] == "#":
                continue
            key, value = [i.strip() for i in l.split("=") ]
            debug("key: '%s'" % key)
            debug("value: '%s'" % value)
            try:
                getattr(self.__class__, key)
                try:
                    if value[0] == '"' and value[-1] == '"':
                        v = value.replace('"', '')
                    else:
                        if "(" in value or ";" in value:
                            raise Exception("invalid character")
                        v = eval(value)

                    setattr(self, key, v)
                    debug("set '%s' = '%s'" % (key, v))
                except Exception as e:
                    error("cannot parse value '%s': %s" % (value, str(e)))
            except AttributeError:
                error("unknown setting '%s' in '%s'" % (key, filename))
        return True

