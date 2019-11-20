#!/bin/bash -e

set -x

curl -O https://www.foundationdb.org/downloads/6.1.12/macOS/installers/FoundationDB-6.1.12.pkg

sudo installer -pkg FoundationDB-6.1.12.pkg -target /
