#!/bin/bash
# test.sh
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

# Regression tests for the CacheManager.
# configuration has to be changed in this script

# directory used for test data.
# has to be accessible from all machines used for testing.
DATADIR=/work/speech/rybach/temp/cm-test.$$
mkdir -p $DATADIR

# server port is chosen randomly
SERVERPORT=$[10000+(${RANDOM}%1000)]

# directory with log files (debug output)
LOGDIR=log.$$
mkdir -p $LOGDIR

# directory used as local cache (machine specific)
CACHEDIR=$(mktemp -d)

# two test files
TEST_A=${DATADIR}/testfile_a
TEST_B=${DATADIR}/testfile_b
TESTSIZE_A=5
TESTSIZE_B=100

# machines used for testing. must be pairwise different
HOST=$(hostname)
TESTHOST_A=kalk
TESTHOST_B=lanthan
if [ $(hostname | cut -f 1 -d -) = cluster ]; then
    TESTHOST_A=cluster-cn-180
    TESTHOST_B=cluster-cn-190
    CACHEDIR=$(mktemp -d --tmpdir=/var/tmp/${USER})
fi
trap "stopServer; cleanup" EXIT


function createDataFiles()
{
    createTestFile ${TEST_A} ${TESTSIZE_A}
    createTestFile ${TEST_B} ${TESTSIZE_B}
}

function createTestFile()
{
    local fileName=$1
    local fileSize=$2
    dd if=/dev/zero of=$fileName bs=1024k count=$fileSize &> /dev/null
}

function cleanup()
{
    rm -rf $DATADIR
    rm -rf $CACHEDIR
    ssh $TESTHOST_A "rm -rf $CACHEDIR" 2> /dev/null
    ssh $TESTHOST_B "rm -rf $CACHEDIR" 2> /dev/null
}

function startServer()
{
    cat > ${DATADIR}/server.config <<EOF
PORT                = $SERVERPORT
CONNECTION_QUEUE    = 32
MAX_COPY_SERVER     = 1
MAX_COPY_NODE       = 1
DB_FILE             = "${DATADIR}/test.db"
DB_SAVE_INTERVAL    = 10
STAT_INTERVAL       = 10
SOCKET_TIMEOUT      = 30*60.0
MAX_WAIT_COPY       = 10*60
CLIENT_WAIT         = 10
CLEANUP_INTERVAL    = 10
EOF
    ./cm-server.py ${DATADIR}/server.config 1> ${LOGDIR}/server.log 2>> ${LOGDIR}/server.stderr &
}

function stopServer()
{
    serverpid=$(ps aux | grep cm-server.py | grep -v "grep\|vim"  | awk '{print $2}')
    if [ "$serverpid" != "" ]; then
        kill $serverpid
    fi
}

function getRemoteCacheDir()
{
    local host=$1
    local myhost=$(hostname)
    echo "$CACHEDIR" | sed -e "s/${myhost}/${host}/"
}

function clientConfig()
{
    local configfile=${DATADIR}/client.${RANDOM}.config
    local host=$(hostname)
    local clusterdir=$(echo ${CACHEDIR} | sed -e "s/$(hostname)/\$(HOST)/")
    cat > $configfile << EOF
MASTER_HOST      = "${host}"
MASTER_HOST = "${host}"
MASTER_PORT         = $SERVERPORT
CACHE_DIR   = "${clusterdir}"
CACHE_DIR        = "${CACHEDIR}"
MIN_FREE            = 100 * 1024 * 1024
MAX_USAGE           = 10
MIN_AGE             = 24 * 60 * 60
SOCKET_TIMEOUT      = 2 * 60.0
SLOW_COPY           = False
EOF
    local n=$#
    for ((i=1; i<=${n}; i+=2)); do
        sed -i "s/^${1} .*$/${1} = ${2}/" $configfile
        shift 2
    done
    echo $configfile
}

function execute()
{
    local fn=$1
    local host=$2
    local file=$3
    local config=$4
    local suffix=$5
    local option=$6
    local nooutput=${7:-0}
    out=$(./cm-client.py --debug --config $config $option $file 2> ${LOGDIR}/${fn}.${host}.log$suffix)
    if [ $nooutput -eq 0 ]; then
        if [ "$out" = "" ]; then
            echo "error: $fn:$host produced no output"
        fi
    fi
    echo " => $out"
}

function rexecute()
{
    local fn=$1
    local host=$2
    local file=$3
    local config=$4
    local suffix=$5
    local option=$6
    out=$(ssh $host "cd $PWD && ./cm-client.py --debug --config $config $option $file 2> ${LOGDIR}/${fn}.${host}.log$suffix" 2> /dev/null)
    if [ -z $out ]; then
        echo "error: $fn:$host produced no output"
    fi
    echo " => $out"
}

function getLogFile()
{
    local fn=$1
    local host=$2
    local suffix=$3
    echo ${LOGDIR}/${fn}.${host}.log$suffix
}

function verify()
{
    local fn=$1
    local host=$2
    local str=$3
    local suffix=$4
    grep "$str" $(getLogFile $fn $host $suffix)
    if [ $? -ne 0 ];then
        echo "verify failed in $fn:$host '$str'"
    fi
}

function normalCopy()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    cp $TEST_A ${DATADIR}/${FUNCNAME}
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}" 
}

function nodeCopy()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    cp $TEST_A ${DATADIR}/${FUNCNAME}
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}"
    verify $FUNCNAME $TESTHOST_A "LOG: copied ${HOST}:$(getRemoteCacheDir $HOST)${DATADIR}/${FUNCNAME}"
}

function nodeCopyNewer()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    cp $TEST_A ${DATADIR}/${FUNCNAME}
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}"
    echo 1 > ${DATADIR}/${FUNCNAME}
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $TESTHOST_A "LOG: copied ${DATADIR}/${FUNCNAME}"
}

function nodeCopyTainted()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    cp $TEST_A ${DATADIR}/${FUNCNAME}
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}"
    echo 1 > ${CACHEDIR}${DATADIR}/${FUNCNAME}
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $TESTHOST_A "LOG: copied ${DATADIR}/${FUNCNAME}"
}

function nodeCopyDeleted()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    cp $TEST_A ${DATADIR}/${FUNCNAME}
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}"
    rm ${CACHEDIR}${DATADIR}/${FUNCNAME}
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $TESTHOST_A "LOG: copied ${DATADIR}/${FUNCNAME}"
}

function nodeCopyOther()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    cp $TEST_A ${DATADIR}/${FUNCNAME}
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}"
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $TESTHOST_A "LOG: copied ${HOST}:$(getRemoteCacheDir $HOST)${DATADIR}/${FUNCNAME}"
    rm -f ${CACHEDIR}/${DATADIR}/${FUNCNAME}
    rexecute $FUNCNAME $TESTHOST_B ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $TESTHOST_B "LOG: copied ${TESTHOST_A}:$(getRemoteCacheDir ${TESTHOST_A})${DATADIR}/${FUNCNAME}"
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config
    local logfile=$(getLogFile $FUNCNAME $HOST)
    local copiedHost=$(grep "LOG: copied .*:.*${DATADIR}/${FUNCNAME}" $logfile | cut -f 2 -d : | cut -f 3 -d " ")
    verify $FUNCNAME $HOST "LOG: copied ${copiedHost}:$(getRemoteCacheDir $copiedHost)${DATADIR}/${FUNCNAME}"
}

function removeNewFile()
{
    echo "********** $FUNCNAME **********"
    cp $TEST_A ${DATADIR}/${FUNCNAME}
    free=$(python -c "import os, statvfs; s = os.statvfs('$CACHEDIR'); print s[statvfs.F_BAVAIL]*s[statvfs.F_BSIZE];")
    dd if=/dev/zero of=${CACHEDIR}/dummy bs=1024k count=10 &> /dev/null
    local config=$(clientConfig MIN_FREE $(python -c "print $free - ${TESTSIZE_A}*1024*1024;"))
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $HOST "LOG: not enough free space in ${CACHEDIR}"
    rm -f ${CACHEDIR}/dummy
}

function removeOldFile()
{
    echo "********** $FUNCNAME **********"
    cp $TEST_A ${DATADIR}/${FUNCNAME}
    free=$(python -c "import os, statvfs; s = os.statvfs('$CACHEDIR'); print s[statvfs.F_BAVAIL]*s[statvfs.F_BSIZE];")
    dd if=/dev/zero of=${CACHEDIR}/dummy bs=1024k count=10 &> /dev/null
    local config=$(clientConfig MIN_FREE $(python -c "print ($free - ${TESTSIZE_A}*1024*1024) -400000;"))
    touch -a -d '2008-01-01 00:00:00' ${CACHEDIR}/dummy
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $HOST "LOG: removed ${CACHEDIR}/dummy"
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}"
    rm -f ${CACHEDIR}/dummy
}

function removeOpenFile()
{
    echo "********** $FUNCNAME **********"
    cp $TEST_A ${DATADIR}/${FUNCNAME}
    free=$(python -c "import os, statvfs; s = os.statvfs('$CACHEDIR'); print s[statvfs.F_BAVAIL]*s[statvfs.F_BSIZE];")
    dd if=/dev/zero of=${CACHEDIR}/dummy bs=1024k count=10 &> /dev/null
    local config=$(clientConfig MIN_FREE $(python -c "print $free - ${TESTSIZE_A}*1024*1024 -40000;"))
    tail -f ${CACHEDIR}/dummy &> /dev/null &
    tailpid=$!
    touch -a -d '2008-01-01 00:00:00' ${CACHEDIR}/dummy
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $HOST "LOG: not enough free space in ${CACHEDIR}"
    kill $tailpid
    rm -f ${CACHEDIR}/dummy
}

function useExisting()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    cp $TEST_A ${DATADIR}/${FUNCNAME}
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config .a
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config .b
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}" .a
    verify $FUNCNAME $HOST "LOG: using existing file" .b
}

function slowCopy()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig SLOW_COPY True SOCKET_TIMEOUT 10)
    cp $TEST_B ${DATADIR}/${FUNCNAME}
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}"
}

function slowNodeCopy()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    cp $TEST_B ${DATADIR}/${FUNCNAME}
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}"
    config=$(clientConfig SLOW_COPY True SOCKET_TIMEOUT 10)
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $TESTHOST_A "LOG: copied ${HOST}:${CACHEDIR}${DATADIR}/${FUNCNAME}"
}


function waitCopy()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig SLOW_COPY True)
    createTestFile ${DATADIR}/${FUNCNAME}.a 500
    # cp $TEST_B ${DATADIR}/${FUNCNAME}.a
    cp $TEST_A ${DATADIR}/${FUNCNAME}.b
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.a $config .a &
    pidA=$!
    sleep 1
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.b $config .b &
    pidB=$!
    wait $pidA $pidB
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}" .a
    verify $FUNCNAME $HOST "LOG: no copy slot available. waiting" .b
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}" .b
}

function waitCopyOther()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    createTestFile ${DATADIR}/${FUNCNAME}.a 1
    createTestFile ${DATADIR}/${FUNCNAME}.b 1
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME}.a $config .1
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME}.b $config .2
    rexecute $FUNCNAME $TESTHOST_B ${DATADIR}/${FUNCNAME}.a $config
    verify $FUNCNAME $TESTHOST_A "LOG: copied ${DATADIR}/${FUNCNAME}.a" .1
    verify $FUNCNAME $TESTHOST_A "LOG: copied ${DATADIR}/${FUNCNAME}.b" .2
    verify $FUNCNAME $TESTHOST_B "LOG: copied .*${DATADIR}/${FUNCNAME}"
    config=$(clientConfig SLOW_COPY True)
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.b $config .1 &
    pidA=$!
    sleep 1
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.a $config .2 &
    pidB=$!
    wait $pidA $pidB
    verify $FUNCNAME $HOST "LOG: copied ${TESTHOST_A}:$(getRemoteCacheDir ${TESTHOST_A})${DATADIR}/${FUNCNAME}.b" .1
    verify $FUNCNAME $HOST "LOG: copied ${TESTHOST_B}:$(getRemoteCacheDir ${TESTHOST_B})${DATADIR}/${FUNCNAME}.a" .2
}

function removeFiles()
{
    echo "********** $FUNCNAME **********"
    cp $TEST_A ${DATADIR}/${FUNCNAME}
    local free=$(python -c "import os, statvfs; s = os.statvfs('$CACHEDIR'); print s[statvfs.F_BAVAIL]*s[statvfs.F_BSIZE];")
    local config=$(clientConfig)
    for ((i=1;i<=7;i++)); do
        cp $TEST_A ${DATADIR}/${FUNCNAME}.$i
        execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.$i $config .$i
        verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}.$i" .$i
        if [ $i -lt 6 ]; then
            touch -a -d '2008-01-01 00:00:00' ${CACHEDIR}/${DATADIR}/${FUNCNAME}.${i}
        elif [ $i -eq 6 ]; then
            echo 1 > ${CACHEDIR}/${DATADIR}/${FUNCNAME}.${i}
        elif [ $i -eq 7 ]; then
            echo 2 > ${DATADIR}/${FUNCNAME}.$i
        fi
    done
    config=$(clientConfig MIN_FREE $(python -c "print $free - ${TESTSIZE_A}*1024*1024 -40000;"))
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config .remove
    config=$(clientConfig)
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.6 $config .r6
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.7 $config .r7
}


function notFound()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config 
    verify $FUNCNAME $HOST "ERROR: file not found '${DATADIR}/${FUNCNAME}'"
}

function bundleArchive()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    rm -f ${DATADIR}/${FUNCNAME}.bundle
    for ((i=1;i<=4;i++)); do
        cp $TEST_A ${DATADIR}/${FUNCNAME}.$i
        echo ${DATADIR}/${FUNCNAME}.$i >> ${DATADIR}/${FUNCNAME}.bundle
    done
    result=$(execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.bundle $config | sed -e 's/ *=> *//')
    cat $result
    for ((i=1;i<=4;i++)); do
        verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}.$i" 
    done
}

function bundleNodeCopy()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    rm -f ${DATADIR}/${FUNCNAME}.bundle
    for ((i=1;i<=4;i++)); do
        cp $TEST_A ${DATADIR}/${FUNCNAME}.$i
        echo ${DATADIR}/${FUNCNAME}.$i >> ${DATADIR}/${FUNCNAME}.bundle
    done
    result=$(execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.bundle $config | sed -e 's/ *=> *//')
    cat $result
    for ((i=1;i<=4;i++)); do
        verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}.$i" 
    done
    result=$(rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME}.bundle $config | sed -e 's/ *=> *//')
    for ((i=1;i<=4;i++)); do
	verify $FUNCNAME $TESTHOST_A "LOG: copied ${HOST}:$(getRemoteCacheDir $HOST)${DATADIR}/${FUNCNAME}.$i"
    done
}

function bundleArchiveConjunct()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    rm -f ${DATADIR}/${FUNCNAME}.bundle
    for ((i=1;i<=4;i++)); do
        cp $TEST_A ${DATADIR}/${FUNCNAME}.$i
        echo ${DATADIR}/${FUNCNAME}.$i >> ${DATADIR}/${FUNCNAME}.bundle
    done
    result=$(execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.bundle $config "" --conjunct | sed -e 's/ *=> *//')
    cat $result
    for ((i=1;i<=4;i++)); do
        verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}.$i" 
    done
}

function bundleArchiveNonStandardExt()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    rm -f ${DATADIR}/${FUNCNAME}.bundle
    for ((i=1;i<=4;i++)); do
        cp $TEST_A ${DATADIR}/${FUNCNAME}.$i
        echo ${DATADIR}/${FUNCNAME}.$i >> ${DATADIR}/${FUNCNAME}.bd
    done
    result=$(execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.bd $config "" --bundle | sed -e 's/ *=> *//')
    cat $result
    for ((i=1;i<=4;i++)); do
        verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}.$i" 
    done
}

function bundleFail()
{
    # this test may fail, because os.statvfs uses cached values
    # for f_BAVAIL and therefore the free disk space might not 
    # be correct if disk access is too fast.
    echo "********** $FUNCNAME **********"
    free=$(python -c "import os, statvfs; s = os.statvfs('$CACHEDIR'); print s[statvfs.F_BAVAIL]*s[statvfs.F_BSIZE];")
    dd if=/dev/zero of=${CACHEDIR}/dummy bs=1024k count=10 &> /dev/null
    local config=$(clientConfig MIN_FREE $(python -c "print $free - ${TESTSIZE_A}*1024*1024*4;"))
    rm -f ${DATADIR}/${FUNCNAME}.bundle
    for ((i=1;i<=4;i++)); do
        cp $TEST_A ${DATADIR}/${FUNCNAME}.$i
        echo ${DATADIR}/${FUNCNAME}.$i >> ${DATADIR}/${FUNCNAME}.bundle
    done
    result=$(execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.bundle $config | sed -e 's/ *=> *//')
    cat $result
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}.1" 
    for ((i=2;i<=4;i++)); do
        verify $FUNCNAME $HOST "WARN: cannot cache bundle content: ${DATADIR}/${FUNCNAME}.$i"
    done
}

function bundleFailConjunct()
{
    # this test may fail, because os.statvfs uses cached values
    # for f_BAVAIL and therefore the free disk space might not 
    # be correct if disk access is too fast.
    echo "********** $FUNCNAME **********"
    free=$(python -c "import os, statvfs; s = os.statvfs('$CACHEDIR'); print s[statvfs.F_BAVAIL]*s[statvfs.F_BSIZE];")
    local config=$(clientConfig MIN_FREE $(python -c "print $free - ${TESTSIZE_A}*1024*1024*3;"))
    rm -f ${DATADIR}/${FUNCNAME}.bundle
    for ((i=1;i<=4;i++)); do
        cp $TEST_A ${DATADIR}/${FUNCNAME}.$i
        echo ${DATADIR}/${FUNCNAME}.$i >> ${DATADIR}/${FUNCNAME}.bundle
    done
    result=$(execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.bundle $config "" --conjunct | sed -e 's/ *=> *//')
    cat $result
    verify $FUNCNAME $HOST "LOG: result is not cached"
}

function bundleFailAll()
{
    echo "********** $FUNCNAME **********"
    free=$(python -c "import os, statvfs; s = os.statvfs('$CACHEDIR'); print s[statvfs.F_BAVAIL]*s[statvfs.F_BSIZE];")
    dd if=/dev/zero of=${CACHEDIR}/dummy bs=1024k count=10 &> /dev/null
    local config=$(clientConfig MIN_FREE $free)
    rm -f ${DATADIR}/${FUNCNAME}.bundle
    for ((i=1;i<=4;i++)); do
        cp $TEST_A ${DATADIR}/${FUNCNAME}.$i
        echo ${DATADIR}/${FUNCNAME}.$i >> ${DATADIR}/${FUNCNAME}.bundle
    done
    result=$(execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.bundle $config | sed -e 's/ *=> *//')
    cat $result
    for ((i=1;i<=4;i++)); do
        verify $FUNCNAME $HOST "WARN: cannot cache bundle content: ${DATADIR}/${FUNCNAME}.$i"
    done
    verify $FUNCNAME $HOST "ERROR: caching of bundle archive failed"
}

function bundleFailMissingOne()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    rm -f ${DATADIR}/${FUNCNAME}.bundle
    for ((i=1;i<=4;i++)); do
        cp $TEST_A ${DATADIR}/${FUNCNAME}.$i
        echo ${DATADIR}/${FUNCNAME}.$i >> ${DATADIR}/${FUNCNAME}.bundle
    done
    # remove one bundle content
    rm ${DATADIR}/${FUNCNAME}.2
    result=$(execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.bundle $config | sed -e 's/ *=> *//')
    cat $result
    for ((i=1;i<=4;i++)); do
	[ $i -eq 2 ] && continue
        verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}.$i" 
    done
    verify $FUNCNAME $HOST "WARN: cannot cache bundle content: ${DATADIR}/${FUNCNAME}.2"
}

function bundleFailMissingOneConjunct()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    rm -f ${DATADIR}/${FUNCNAME}.bundle
    for ((i=1;i<=4;i++)); do
        cp $TEST_A ${DATADIR}/${FUNCNAME}.$i
        echo ${DATADIR}/${FUNCNAME}.$i >> ${DATADIR}/${FUNCNAME}.bundle
    done
    # remove one bundle content
    rm ${DATADIR}/${FUNCNAME}.2
    result=$(execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.bundle $config "" --conjunct | sed -e 's/ *=> *//')
    cat $result
    verify $FUNCNAME $HOST "WARN: cannot cache bundle content: ${DATADIR}/${FUNCNAME}.2"
    verify $FUNCNAME $HOST "ERROR: caching of bundle archive failed"
}

function locations()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    cp $TEST_A ${DATADIR}/${FUNCNAME}
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config .copy
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}" .copy
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME} $config .copy
    verify $FUNCNAME $TESTHOST_A "LOG: copied ${HOST}:${CACHEDIR}${DATADIR}/${FUNCNAME}" .copy
    rexecute $FUNCNAME $TESTHOST_B ${DATADIR}/${FUNCNAME} $config .copy
    verify $FUNCNAME $TESTHOST_B "LOG: copied .*:.*${DATADIR}/${FUNCNAME}" .copy
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config "" "-l"
}

function location()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    cp $TEST_A ${DATADIR}/${FUNCNAME}
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config .copy
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}" .copy
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME} $config .copy
    verify $FUNCNAME $TESTHOST_A "LOG: copied ${HOST}:${CACHEDIR}${DATADIR}/${FUNCNAME}" .copy
    rexecute $FUNCNAME $TESTHOST_B ${DATADIR}/${FUNCNAME} $config .copy
    verify $FUNCNAME $TESTHOST_B "LOG: copied .*:.*${DATADIR}/${FUNCNAME}" .copy
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config "" "-l"
    verify $FUNCNAME $HOST "LOG: 3 locations found"
}

function locationNewer()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    cp $TEST_A ${DATADIR}/${FUNCNAME}
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config .copy
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}" .copy
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME} $config .copy
    verify $FUNCNAME $TESTHOST_A "LOG: copied ${HOST}:${CACHEDIR}${DATADIR}/${FUNCNAME}" .copy
    echo 1 > ${DATADIR}/${FUNCNAME}
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config "" "-l" 1
    verify $FUNCNAME $HOST "LOG: 0 locations found"
}

function locationDeleted()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    cp $TEST_A ${DATADIR}/${FUNCNAME}
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config .copy
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}" .copy
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME} $config .copy
    verify $FUNCNAME $TESTHOST_A "LOG: copied ${HOST}:${CACHEDIR}${DATADIR}/${FUNCNAME}" .copy
    ssh $TESTHOST_A "rm $(getRemoteCacheDir ${TESTHOST_A})${DATADIR}/${FUNCNAME}" 2> /dev/null
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config "" "-l"
    verify $FUNCNAME $HOST "LOG: 1 locations found"
}

function locations()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    for ((i=1;i<=3;i++)); do
        cp $TEST_A ${DATADIR}/${FUNCNAME}.$i
        execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.$i $config .c$i
        verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}.$i" .c$i
        rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME}.$i $config .c$i
        verify $FUNCNAME $TESTHOST_A "LOG: copied ${HOST}:${CACHEDIR}${DATADIR}/${FUNCNAME}.$i" .c$i
        fn="$fn ${DATADIR}/${FUNCNAME}.$i" 
    done
    execute $FUNCNAME $HOST "$fn" $config "" "-l"
    verify $FUNCNAME $HOST "LOG: 6 locations found"
}

function locationsBundle()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    rm -f ${DATADIR}/${FUNCNAME}.bundle
    for ((i=1;i<=4;i++)); do
        cp $TEST_A ${DATADIR}/${FUNCNAME}.$i
        echo ${DATADIR}/${FUNCNAME}.$i >> ${DATADIR}/${FUNCNAME}.bundle
    done
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.bundle $config .copy
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME}.bundle $config .copy
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.bundle $config "" "-l"
    verify $FUNCNAME $HOST "LOG: 8 locations found"
}

function aTimeConfusion()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    cp $TEST_A ${DATADIR}/${FUNCNAME}.1
    cp $TEST_A ${DATADIR}/${FUNCNAME}.2
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.1 $config
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}.1"
    touch -a -d '2008-01-01 00:00:00' ${CACHEDIR}/${DATADIR}/${FUNCNAME}.1
    stat ${CACHEDIR}/${DATADIR}/${FUNCNAME}.1
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME}.1 $config
    verify $FUNCNAME $TESTHOST_A "LOG: copied ${HOST}:${CACHEDIR}${DATADIR}/${FUNCNAME}.1"
    stat ${CACHEDIR}/${DATADIR}/${FUNCNAME}.1
    free=$(python -c "import os, statvfs; s = os.statvfs('$CACHEDIR'); print s[statvfs.F_BAVAIL]*s[statvfs.F_BSIZE];")
    config=$(clientConfig MIN_FREE $(python -c "print $free - ${TESTSIZE_A}*1024*1024+1;"))
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.2 $config
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}.2"
}

function copyToServer()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    cp $TEST_A ${CACHEDIR}/${FUNCNAME}
    execute $FUNCNAME $HOST "${CACHEDIR}/${FUNCNAME} ${DATADIR}/${FUNCNAME}" $config "" "-cp" 1
    verify $FUNCNAME $HOST "LOG: copied .*/${FUNCNAME} to ${DATADIR}/${FUNCNAME}"
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $TESTHOST_A "LOG: copied ${HOST}:.*/${FUNCNAME}"
}

function copyNoRegister()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    cp $TEST_A ${CACHEDIR}/${FUNCNAME}
    execute $FUNCNAME $HOST "${CACHEDIR}/${FUNCNAME} ${DATADIR}/${FUNCNAME}" $config "" "-cp -n" 1
    verify $FUNCNAME $HOST "LOG: copied .*/${FUNCNAME} to ${DATADIR}/${FUNCNAME}"
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $TESTHOST_A "LOG: copied ${DATADIR}/${FUNCNAME}"
}

function copyBundleToServer()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    rm -f ${CACHEDIR}/${FUNCNAME}.bundle
    for ((i=1;i<=4;i++)); do
	cp $TEST_A ${CACHEDIR}/${FUNCNAME}.$i
	echo ${CACHEDIR}/${FUNCNAME}.$i >> ${CACHEDIR}/${FUNCNAME}.bundle
    done
    mkdir -p ${DATADIR}/${FUNCNAME}.d
    execute $FUNCNAME $HOST "${CACHEDIR}/${FUNCNAME}.bundle ${DATADIR}/${FUNCNAME}.d" $config "" "-cp" 1
    for ((i=1;i<=4;i++)); do
	verify $FUNCNAME $HOST "LOG: copied .*/${FUNCNAME}.$i to ${DATADIR}/${FUNCNAME}.d/${FUNCNAME}.$i"
    done
}

function copyBundleToServerWithError()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    echo "${CACHEDIR}/notexisting.1" > ${CACHEDIR}/${FUNCNAME}.bundle
    for ((i=1;i<=4;i++)); do
	cp $TEST_A ${CACHEDIR}/${FUNCNAME}.$i
	echo ${CACHEDIR}/${FUNCNAME}.$i >> ${CACHEDIR}/${FUNCNAME}.bundle
    done
    echo "${CACHEDIR}/notexisting.2" >> ${CACHEDIR}/${FUNCNAME}.bundle
    mkdir -p ${DATADIR}/${FUNCNAME}.d
    execute $FUNCNAME $HOST "${CACHEDIR}/${FUNCNAME}.bundle ${DATADIR}/${FUNCNAME}.d" $config "" "-cp" 1
    for ((i=1;i<=4;i++)); do
	verify $FUNCNAME $HOST "LOG: copied .*/${FUNCNAME}.$i to ${DATADIR}/${FUNCNAME}.d/${FUNCNAME}.$i"
    done
    for ((i=1;i<=2;i++)); do
	verify $FUNCNAME $HOST "ERROR: cannot copy .*/notexisting.$i"
    done
}


function waitActive()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig SLOW_COPY True)
    createTestFile ${DATADIR}/${FUNCNAME}.a 1000
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.a $config .a &
    pidA=$!
    sleep 1
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.a $config .b &
    pidB=$!
    wait $pidA $pidB
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}" .a
    verify $FUNCNAME $HOST "LOG: file transfer in progress. wait" .b
    verify $FUNCNAME $HOST "LOG: using existing file" .b
}

function waitActiveParallel()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    createTestFile ${DATADIR}/${FUNCNAME}.a 300
    local njob=5
    local nfile=10
    for ((i=1;i<=${nfile};i++)); do
	ln ${DATADIR}/${FUNCNAME}.a ${DATADIR}/${FUNCNAME}.$i
    done
    pids=
    for ((j=1;j<=${njob};j++)); do
    	for ((i=1;i<=${nfile};i++)); do
	    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME}.$i $config .${i}.${j} &
	    pids="$pids $!"
	done
    done
    wait $pids
    local ncopy=$(grep "LOG: copied" ${LOGDIR}/${FUNCNAME}.${HOST}.log.* | wc -l)
    if [ $ncopy -ne $nfile ]; then
	echo "verify failed: copied $ncopy"
    fi
    local nexisting=$(grep "LOG: using existing " ${LOGDIR}/${FUNCNAME}.${HOST}.log.* | wc -l)
    if [ $nexisting -ne $[ ${nfile} * $[ ${njob} - 1 ] ] ]; then
	echo "verify failed: existing $nexisting"
    fi
}


function readServerDb()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    cp $TEST_A ${DATADIR}/${FUNCNAME}
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}"
    stopServer
    sleep 5
    startServer
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $TESTHOST_A "LOG: copied ${HOST}:$(getRemoteCacheDir $HOST)${DATADIR}/${FUNCNAME}"
}

function readPlainServerDb()
{
    echo "********** $FUNCNAME **********"
    local config=$(clientConfig)
    cp $TEST_A ${DATADIR}/${FUNCNAME}
    execute $FUNCNAME $HOST ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $HOST "LOG: copied ${DATADIR}/${FUNCNAME}"
    stopServer
    mv ${DATADIR}/test.db ${DATADIR}/test.db.gz
    gzip -d --force ${DATADIR}/test.db.gz
    startServer
    rexecute $FUNCNAME $TESTHOST_A ${DATADIR}/${FUNCNAME} $config
    verify $FUNCNAME $TESTHOST_A "LOG: copied ${HOST}:$(getRemoteCacheDir $HOST)${DATADIR}/${FUNCNAME}"
}



# set -x
startServer
createDataFiles

if [ ! -z $1 ]; then
    $1
else
    normalCopy
    nodeCopy
    nodeCopyNewer
    nodeCopyTainted
    nodeCopyDeleted
    nodeCopyOther
    removeNewFile
    removeOldFile
    removeOpenFile
    useExisting
    waitCopy
    waitCopyOther
    slowCopy
    slowNodeCopy
    # removeFiles
    notFound
    bundleArchive
    bundleNodeCopy
    bundleArchiveNonStandardExt
    # bundleFail
    bundleFailAll
    bundleFailMissingOne
    bundleArchiveConjunct
    bundleFailConjunct
    bundleFailMissingOneConjunct
    location
    locationNewer
    locationDeleted
    locations
    locationsBundle
    copyToServer
    copyNoRegister
    copyBundleToServer
    copyBundleToServerWithError
    waitActive
    readServerDb
fi

