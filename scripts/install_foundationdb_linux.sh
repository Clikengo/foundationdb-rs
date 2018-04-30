#!/bin/bash -e

set -x

curl -O https://www.foundationdb.org/downloads/5.1.5/ubuntu/installers/foundationdb-clients_5.1.5-1_amd64.deb
curl -O https://www.foundationdb.org/downloads/5.1.5/ubuntu/installers/foundationdb-server_5.1.5-1_amd64.deb

sudo dpkg -i foundationdb-clients_5.1.5-1_amd64.deb
sudo dpkg -i foundationdb-server_5.1.5-1_amd64.deb