#!/bin/bash -e

set -x

curl -O https://www.foundationdb.org/downloads/5.2.5/macOS/installers/FoundationDB-5.2.5.pkg

sudo installer -pkg FoundationDB-5.2.5.pkg -target /
