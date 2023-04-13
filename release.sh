#!/bin/bash

set -e

CPWD=$PWD
REPURL="http://www-i6/svn/cache-manager/trunk"
TMPSRC=$(mktemp -d)
VERSION=${1:-1.0}
SRCDIR=$TMPSRC/cache-manager-${VERSION}

mkdir $SRCDIR
svn export --force $REPURL $SRCDIR
cd $SRCDIR

for f in *.py *.sh cqsub; do
    rm -f ${f}.new
    sb=0
    head -n1 $f | grep -q '#!' && sb=1
    [ $sb -eq 1 ] && head -n1 $f >> ${f}.new
    echo "# "$(basename $f) >> ${f}.new
    echo "#" >> ${f}.new
    cat header.txt >> ${f}.new
    echo "" >> ${f}.new
    if [ $sb -eq 1 ]; then
	sed -e '1d' $f >> ${f}.new
    else
	cat $f >> ${f}.new
    fi
    mv ${f}.new ${f}
done

# remove internal files
rm -f release.sh header.txt

# replace i6 specific settings with generic ones
mv settings.py settings.multienv.py
mv settings.generic.py settings.py
mv cmclient.generic.config cmclient.config
sed -i 's/\(MASTER_HOST\|CACHE_DIR\)_[^ ]\+/\1/' test.sh

# set permissions
chmod a+x cf cm-client.py cm-server.py cqsub dbutil.py server.sh sysvinit test.sh

tar -cvzf ${CPWD}/cache-mananger-${VERSION}.tar.gz -C .. $(basename $SRCDIR)
