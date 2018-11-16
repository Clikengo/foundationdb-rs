#! /bin/bash -e

set -x

fdb_rs_dir=$(pwd)

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

## build the rust bindingtester
(
  cd ${fdb_rs_dir:?}
  cargo build --manifest-path foundationdb/Cargo.toml  --bin bindingtester 
)

## build the python bindings
(
  fdb_builddir=${fdb_rs_dir:?}/target/foundationdb_build
  mkdir -p ${fdb_builddir:?}
  cd ${fdb_builddir:?}

  ## Get foundationdb source
  git clone --depth 1 https://github.com/apple/foundationdb.git -b release-5.2
  cd foundationdb
  git checkout release-5.2

  ## need the python api bindings
  make fdb_python
  
  ## Run the test
  ./bindings/bindingtester/bindingtester.py --no-threads --seed 100 ${fdb_rs_dir:?}/target/debug/bindingtester
)