#!/bin/bash -e

set -x

VERSION=6.0.15
BASE_URL=https://www.foundationdb.org/downloads/${VERSION}

curl -O "${BASE_URL}/windows/installers/foundationdb-${VERSION}-x64.msi"
msiexec -i "foundationdb-${VERSION}-x64.msi" -quiet -passive -norestart -log install.log
