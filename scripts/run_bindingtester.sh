#! /bin/bash -e

set -x

fdb_rs_dir=$(pwd)
bindingtester="${fdb_rs_dir:?}/$1"
case $(uname) in
  Darwin)
    brew install mono
  ;;
  Linux)
    sudo apt update
    sudo apt install mono-devel -y
  ;;
  *) echo "only macOS or Ubuntu is supported"
esac

## build the python bindings
(
  fdb_builddir=${fdb_rs_dir:?}/target/foundationdb_build
  mkdir -p ${fdb_builddir:?}
  cd ${fdb_builddir:?}

  ## Get foundationdb source
  git clone --depth 1 https://github.com/apple/foundationdb.git -b release-6.1
  cd foundationdb
  git checkout release-6.1

  ## need the python api bindings
  make fdb_python

  ## Run the test
  ./bindings/bindingtester/bindingtester.py --test-name scripted ${bindingtester}
  ./bindings/bindingtester/bindingtester.py --num-ops 1000 --test-name api --api-version 610 ${bindingtester}
  ./bindings/bindingtester/bindingtester.py --num-ops 1000 --concurrency 5 --test-name api --api-version 610 ${bindingtester}
  ./bindings/bindingtester/bindingtester.py --num-ops 1000 --concurrency 5 --test-name tuple --api-version 610 ${bindingtester}
)
