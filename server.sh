#!/bin/bash
# server.sh
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

# start the CacheManager server

CMPATH=/opt/cache-manager
LOGFILE=${CMPATH}/cmserver.log

${CMPATH}/cm-server.py \
    ${CMPATH}/cmserver.config 2>&1 | \
    tee -a $LOGFILE
