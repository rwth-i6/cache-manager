#!/bin/bash
# start the CacheManager server

CMPATH=/opt/cache-manager
LOGFILE=${CMPATH}/cmserver.log

${CMPATH}/cm-server.py \
    ${CMPATH}/cmserver.config 2>&1 | \
    tee -a $LOGFILE
