#!/usr/bin/env python3
"""
cache management command line client.
load balanced file caching on local harddisks
"""

import sys
import os
from cmlogging import *
from client import CmClient, ClientConfiguration
from filesystem import FileSystem

__version__ = "$Rev: 837 $"
__author__  = "rybach@cs.rwth-aachen.de (David Rybach)"
__copyright__ = "Copyright 2012, RWTH Aachen University"

def usage():
    p = os.path.basename(sys.argv[0])
    sys.stderr.write("usage: \n" +\
                     "  get local copy:\n" +\
                     "      %s [options] <filename>\n" % p +\
                     "  copy local file to file server:\n" +\
                     "      %s [options] -cp [-n|--noregister] [-m|--maxloc N] <source> <destination>\n" % p +\
                     "       --noregister    don't register local copy\n" +\
                     "       --maxloc N      only check N remote copies\n" +\
                     "  retrieve locations of local copies:\n" +\
                     "      %s [options] -l <filenames> \n" % p +\
                     "      %s [options] -ll <location-limit-per-file> <filenames> \n" % p +\
                     "  get destination for local copy (do not copy):\n" +\
                     "      %s [options] -d <filename>\n" % p +\
                     "  options:\n"+\
                     "       --config <file> use alternative configuration\n" +\
                     "       --debug         enable debug output\n"+\
                     "       --bundle        treat file as bundle\n"+\
                     "       --conjunct      cache all files or none (for bundles)\n"+\
                     "       --nobundle      ignore special meaning of *.bundle files\n")

class Options:
    def __init__(self, argv):
        self.copy = False
        self.register = True
        self.locate = False
        self.locateLimit = 9999
        self.help = False
        self.version = False
        self.debug = False
        self.config = ""
        self.nobundle = False
        self.bundle = False
        self.conjunct = False
        self.printDestination = False
        self.arg = []
        self.parseArguments(argv)

    def parseArguments(self, argv):
        n = len(argv)
        i = 1
        while i < n:
            a = argv[i]
            if a == "--":
                try:
                    self.arg = argv[i+1:]
                except Exception: pass
            elif a == "--help" or a == "-h":
                self.help = True
            elif a == "--version" or a == "-V":
                self.version = True
            elif a == "--copy" or a == "-cp":
                self.copy = True
            elif a == "--noregister" or a == "-n":
                self.register = False
            elif a == "--locate" or a == "-l":
                self.locate = True
            elif a == "--locate-limit" or a == "-ll":
                self.locate = True
                try:
                    self.locateLimit = int(argv[i+1])
                    i += 1
                except:
                    error("--locate-limit (-ll) expects an int")
                    return 1
            elif a == "--maxloc" or a == "-m":
                try:
                    self.locateLimit = int(argv[i+1])
                    i += 1
                except:
                    error("--maxloc (-m) expects an int")
                    return 1
            elif a == "--destination" or a == "-d":
                self.printDestination = True
            elif a == "--debug":
                self.debug = True
            elif a == "--nobundle":
                self.nobundle = True
            elif a == "--bundle":
                self.bundle = True
            elif a == "--conjunct":
                self.conjunct = True
            elif a == "--config":
                try:
                    self.config = argv[i+1]
                    i += 1
                except Exception as e:
                    warning("--config expects a file name" + str(e))
            elif a.startswith("-"):
                warning("unknown option: '%s'" % a)
            else:
                self.arg += [ a ]
            i += 1


def main(argc, argv):
    options = Options(argv)
    if options.version:
        sys.stderr.write("%s\n" % __version__)
        return 1
    if options.help or len(options.arg) < 1:
        usage()
        return 1
    if options.debug:
        LogLevel.enableDebug()
    if options.copy:
        if len(options.arg) < 2:
            usage()
            return 1

    filename = options.arg[0]
    config = ClientConfiguration(options.config)

    if options.config:
        if not config.read(options.config):
            error("cannot read config file %s. using build-in defaults" % options.config)
    else:
        if not config.loadDefault():
            error("cannot read default config file. using build-in defaults")

    if options.nobundle:
        config.IGNORE_BUNDLE = True

    if options.printDestination:
        print(CmClient.getDestination(FileSystem(config), os.path.realpath(filename)))
        return 0

    client = CmClient(config)

    if not client.isConnected():
        error("cannot connect to server")
    else:
        log("connected to %s" % str(client.connection.getPeerName()))

    r = False
    if options.copy:
        r = client.copy(filename, options.arg[1], options.register, options.bundle)
    elif options.locate:
        locations = []
        r = client.getLocations(options.arg, locations, options.bundle, options.locateLimit)
        log("%d locations found" % len(locations))
        for l in locations:
            print("%s:%s:%s" % l)
    else:
        f, r = client.fetch(filename, options.bundle, options.conjunct, options.locateLimit)
        print(f)
    return not r

if __name__ == "__main__":
    sys.exit( main(len(sys.argv), sys.argv) )
