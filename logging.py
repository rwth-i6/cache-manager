# logging.py
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

