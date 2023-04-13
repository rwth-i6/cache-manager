"""
Log messages and debug info
"""

import sys
import threading

class LogLevel:
    debug = False
    level = 3
    @staticmethod
    def enableDebug():
        LogLevel.level = 4
        LogLevel.debug = True
    @staticmethod
    def set(level):
        LogLevel.level = level


def _getCaller():
    return sys._getframe(2).f_code.co_name

def error(msg):
    if LogLevel.level > 0:
        sys.stderr.write("ERROR: " + msg + " [%s]\n" % _getCaller())

def log(msg):
    if LogLevel.level > 2:
        sys.stderr.write("LOG: " + msg + "\n")

def warning(msg):
    if LogLevel.level > 1:
        sys.stderr.write("WARN: " + msg + "\n")

def debug(msg):
    if LogLevel.debug:
        sys.stderr.write("DEBUG: " + threading.currentThread().getName() + " " + msg + " [%s]\n" % _getCaller())

