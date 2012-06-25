# cftools.sh
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

# $Id$

if [ ${CFQ_INCLUDED:-0} -ne 1 ]; then # prevent multiple inclusion

# cache-manager directory
CFQ_BASEDIR=/u/rybach/apps/cache-manager

# cache manager client
CFQ_CMCLIENT=${CFQ_BASEDIR}/cf

# set prepare mode off 
CFQ_PREPARE=0

CFQ_INCLUDED=1

function cf()
{
    if [ $CFQ_PREPARE -eq 1 ]; then
        cfq_add_file $*
    else
        $CFQ_CMCLIENT $*
    fi
}

function cf_exec()
{
    if [ $CFQ_PREPARE -eq 1 ]; then
        cfq_log "prepare mode: ignoring $*"
    else
        $*
    fi
}

function cfq_log()
{
    echo "CFQ LOG: $*" > /dev/stderr
}

function cfq_error()
{
    echo "CFQ ERROR: $*" > /dev/stderr
    exit 1
}

function cfq_add_file()
{
    local opt
    local cfopt
    [ -n $CFQ_FILES ] || cfq_error "CFQ_FILES not set"
    [ -n $CFQ_CACHE ] || cfq_error "CFQ_CACHE not set"
    grep -e "^${!#}\$" $CFQ_CACHE
    if [ $? -eq 0 ]; then
        echo ${!#}
        return
    fi
    cfq_log "checking $*"
    for opt in $*; do
        if [ $opt = -cp ]; then
            cfq_log "cm-client copy action. -> ignored"
            return
        fi
    done
    expr match "${!#}" "\(.*${CFQ_DETECT_ID}.*\)" > /dev/null
    if [ $? -eq 0 ]; then
        cfq_log dependency on SGE_TASK_ID detected
        echo 1 > $CFQ_NEEDTASKID
        return
    fi
    $CFQ_CMCLIENT -l $* >> $CFQ_FILES
    echo ${!#} >> $CFQ_CACHE
    echo ${!#}
}

fi # EOF
