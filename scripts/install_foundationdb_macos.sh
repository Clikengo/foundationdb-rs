#!/bin/bash -e

set -x

curl -O https://www.foundationdb.org/downloads/6.0.15/macOS/installers/FoundationDB-6.0.15.pkg

sudo installer -pkg FoundationDB-6.0.15.pkg -target /
