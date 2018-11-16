#!/bin/bash -e

set -x

VERSION=5.2.5
VERSION2=${VERSION}-1
BASE_URL=https://www.foundationdb.org/downloads/${VERSION}

dpkg --version &> /dev/null

if [ "$?" == "0" ]
then
    curl -O ${BASE_URL}/ubuntu/installers/foundationdb-clients_${VERSION2}_amd64.deb
    curl -O ${BASE_URL}/ubuntu/installers/foundationdb-server_${VERSION2}_amd64.deb

    sudo dpkg -i foundationdb-clients_${VERSION2}_amd64.deb
    sudo dpkg -i foundationdb-server_${VERSION2}_amd64.deb
else
    curl -O ${BASE_URL}/rhel6/installers/foundationdb-clients-${VERSION2}.el6.x86_64.rpm
    curl -O ${BASE_URL}/rhel6/installers/foundationdb-server-${VERSION2}.el6.x86_64.rpm

    sudo rpm -i foundationdb-clients-${VERSION2}.el6.x86_64.rpm
    sudo rpm -i foundationdb-server-${VERSION2}.el6.x86_64.rpm

fi
