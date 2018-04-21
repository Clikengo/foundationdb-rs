#!/bin/bash -e

curl -O https://www.foundationdb.org/downloads/5.1.5/macOS/installers/FoundationDB-5.1.5.pkg

sudo installer -pkg FoundationDB-5.1.5.pkg -target /
