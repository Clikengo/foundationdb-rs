# FoundationDB Rust Client API

This is a wrapper library around the FoundationDB (Fdb) C API. It implements futures based interfaces over the Fdb future C implementations.

## Prerequisites

### Install FoundationDB

Install FoundationDB on your system, see [FoundationDB Local Development](https://apple.github.io/foundationdb/local-dev.html), or these instructions:

- Ubuntu Linux (this may work on the Linux subsystem for Windows as well)

```console
$> curl -O https://www.foundationdb.org/downloads/5.1.5/ubuntu/installers/foundationdb-clients_5.1.5-1_amd64.deb
$> curl -O https://www.foundationdb.org/downloads/5.1.5/ubuntu/installers/foundationdb-server_5.1.5-1_amd64.deb
$> sudo dpkg -i foundationdb-clients_5.1.5-1_amd64.deb
$> sudo dpkg -i foundationdb-server_5.1.5-1_amd64.deb
```

- macOS

```console
$> curl -O https://www.foundationdb.org/downloads/5.1.5/macOS/installers/FoundationDB-5.1.5.pkg
$> sudo installer -pkg FoundationDB-5.1.5.pkg -target /
```

## Add dependencies on foundationdb-rs

```toml
[dependencies]
foundationdb = "*"
```

## Extern the crate in `bin.rs` or `lib.rs`

```rust
extern crate foundationdb;
```

## Initialization

Due to limitations in the C API, the Client and it's associated Network can only be initialized and run once per the life of a process. Generally the `foundationdb::init` function will be enough to initialize the Client. See `foundationdb::default_api` and `foundationdb::builder` for more configuration options of the Fdb Client.

## Example

```rust
extern crate futures;
extern crate foundationdb;

use std::thread;
use futures::future::*;
use foundationdb::{self, *};

let network = foundationdb::init().expect("failed to initialize Fdb client");

let handle = std::thread::spawn(move || {
    let error = network.run();

    if let Err(error) = error {
        panic!("fdb_run_network: {}", error);
    }
});

// wait for the network thread to be started
network.wait();

// work with Fdb
let db = Cluster::new(foundationdb::default_config_path())
    .and_then(|cluster| cluster.create_database())
    .wait().expect("failed to create Cluster");

// set a value
let trx = db.create_trx().expect("failed to create transaction");

trx.set(b"hello", b"world"); // errors will be returned in the future result
trx.commit()
    .wait()
    .expect("failed to set hello to world");

// read a value
let trx = db.create_trx().expect("failed to create transaction");
let result = trx.get(b"hello").wait().expect("failed to read world from hello");

let value: &[u8] = result.value()
    .expect("failed to get value from result") // unwrap the error
    .unwrap();   // unwrap the option

// should print "hello world"
println!("hello {}", String::from_utf8_lossy(value));

// cleanly shutdown the client
network.stop().expect("failed to stop Fdb client");
handle.join();
```

## API stability

*WARNING* Until the 1.0 release of this library, the API may be in constant flux.