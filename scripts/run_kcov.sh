#!/bin/bash -e

set -x

trust_dns_dir=$(dirname $0)/..
cd ${trust_dns_dir:?}

case $(uname) in
  Darwin) exit 0;;
  Linux) KCOV=true;;
esac

# don't run on nightly or beta
rustc --version | grep beta && exit 0;
rustc --version | grep nightly && exit 0;
if [ -z ${RUN_KCOV} ] ; then exit 0; fi

rm -rf kcov-master master.tar.gz*

# install kcov
# sudo apt-get install libcurl4-openssl-dev libelf-dev libdw-dev
sudo apt-get install cmake libcurl4-openssl-dev libelf-dev libdw-dev
wget https://github.com/SimonKagstrom/kcov/archive/master.tar.gz
tar xzf master.tar.gz
mkdir kcov-master/build
cd kcov-master/build
cmake ..
make
sudo make install
cd ../..

# run kcov on all tests, rerunning all tests with coverage report
mkdir -p target

cargo install cargo-kcov
cargo kcov --manifest-path foundationdb/Cargo.toml

echo "----> ran $test_count test(s)"

echo "----> uploading to codecov.io"
bash <(curl -s https://codecov.io/bash)
echo "----> coverage reports done"