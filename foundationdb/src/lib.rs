// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! # FoundationDB Rust Client API
//!
//! This is a wrapper library around the FoundationDB (Fdb) C API. It implements futures based interfaces over the Fdb future C implementations.
//!
//! ## Prerequisites
//!
//! ### Install FoundationDB
//!
//! Install FoundationDB on your system, see [FoundationDB Local Development](https://apple.github.io/foundationdb/local-dev.html), or these instructions:
//!
//! - Ubuntu Linux (this may work on the Linux subsystem for Windows as well)
//!
//! ```console
//! $> curl -O https://www.foundationdb.org/downloads/5.1.5/ubuntu/installers/foundationdb-clients_5.1.5-1_amd64.deb
//! $> curl -O https://www.foundationdb.org/downloads/5.1.5/ubuntu/installers/foundationdb-server_5.1.5-1_amd64.deb
//! $> sudo dpkg -i foundationdb-clients_5.1.5-1_amd64.deb
//! $> sudo dpkg -i foundationdb-server_5.1.5-1_amd64.deb
//! ```
//!
//! - macOS
//!
//! ```console
//! $> curl -O https://www.foundationdb.org/downloads/5.1.5/macOS/installers/FoundationDB-5.1.5.pkg
//! $> sudo installer -pkg FoundationDB-5.1.5.pkg -target /
//! ```
//!
//! ## Add dependencies on foundationdb-rs
//!
//! ```toml
//! [dependencies]
//! foundationdb = "*"
//! ```
//!
//! ## Extern the crate in `bin.rs` or `lib.rs`
//!
//! ```rust
//! extern crate foundationdb;
//! ```
//!
//! ## Initialization
//!
//! Due to limitations in the C API, the Client and it's associated Network can only be initialized and run once per the life of a process. Generally the `foundationdb::init` function will be enough to initialize the Client. See `foundationdb::default_api` and `foundationdb::builder` for more configuration options of the Fdb Client.
//!
//! ## Example
//!
//! ```rust
//! extern crate futures;
//! extern crate foundationdb;
//!
//! # fn main() {
//!
//! use std::thread;
//! use futures::future::*;
//! use foundationdb::{self, *};
//!
//! let network = foundationdb::init().expect("failed to initialize Fdb client");
//!
//! let handle = std::thread::spawn(move || {
//!     let error = network.run();
//!
//!     if let Err(error) = error {
//!         panic!("fdb_run_network: {}", error);
//!     }
//! });
//!
//! // wait for the network thread to be started
//! network.wait();
//!
//! // work with Fdb
//! let db = Cluster::new(foundationdb::default_config_path())
//!     .and_then(|cluster| cluster.create_database())
//!     .wait().expect("failed to create Cluster");
//!
//! // set a value
//! let trx = db.create_trx().expect("failed to create transaction");
//!
//! trx.set(b"hello", b"world"); // errors will be returned in the future result
//! trx.commit()
//!     .wait()
//!     .expect("failed to set hello to world");
//!
//! // read a value
//! let trx = db.create_trx().expect("failed to create transaction");
//! let result = trx.get(b"hello", false).wait().expect("failed to read world from hello");
//!
//! let value: &[u8] = result.value()
//!     .expect("failed to get value from result") // unwrap the error
//!     .unwrap();   // unwrap the option
//!
//! // should print "hello world"
//! println!("hello {}", String::from_utf8_lossy(value));
//!
//! // cleanly shutdown the client
//! network.stop().expect("failed to stop Fdb client");
//! handle.join();
//!
//! # }
//! ```
//!
//! ## API stability
//!
//! *WARNING* Until the 1.0 release of this library, the API may be in constant flux.

#![deny(missing_docs)]

extern crate byteorder;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate foundationdb_sys;
extern crate futures;
extern crate uuid;

pub mod cluster;
pub mod database;
pub mod error;
pub mod fdb_api;
pub mod future;
#[allow(missing_docs)]
pub mod keyselector;
pub mod network;
/// Generated configuration types for use with the various `set_option` functions
#[allow(missing_docs)]
pub mod options;
pub mod transaction;
pub mod tuple;

//move to prelude?
pub use cluster::Cluster;
pub use database::Database;
pub use transaction::Transaction;

/// Initialize the FoundationDB Client API, this can only be called once per process.
///
/// # Returns
///
/// `Network` which must be run before the Client is ready. `Network::run` will not return until the
///   network is stopped with the associated `Network::stop` and should be run in a separate thread.
///
/// # Examples
///
/// ```rust
/// use std::thread;
/// use foundationdb;
///
/// let network = foundationdb::init().expect("failed to initialize Fdb");
///
/// let handle = std::thread::spawn(move || {
///     let error = network.run();
///
///     if let Err(error) = error {
///         panic!("fdb_run_network: {}", error);
///     }
/// });
///
/// network.wait();
///
/// // do some interesting things with the API...
///
/// network.stop().expect("failed to stop network");
/// handle.join().expect("failed to join fdb thread");
/// ```
pub fn init() -> error::Result<network::Network> {
    fdb_api::FdbApiBuilder::default().build()?.network().build()
}

/// Initialize the FoundationDB Client API, this can only be called once per process.
///
/// # Returns
///
/// A `NetworkBuilder` which can be used to configure the FoundationDB Client API Network.
///
/// # Example
///
/// ```rust
/// use foundationdb;
/// use foundationdb::options::NetworkOption;
///
/// let network = foundationdb::default_api()
///     .expect("failed to initialize API version")
///     .set_option(NetworkOption::DisableClientStatisticsLogging)
///     .expect("failed to set option")
///     .build()
///     .expect("failed to initialize network");
///
/// // see example on `init`
/// ```
pub fn default_api() -> error::Result<network::NetworkBuilder> {
    Ok(fdb_api::FdbApiBuilder::default().build()?.network())
}

/// Allows the API version, etc, to be configured before starting.
///
/// # Returns
///
/// A `FdbApiBuilder` which can be used to configure the FoundationDB Client API version, etc.
pub fn builder() -> fdb_api::FdbApiBuilder {
    fdb_api::FdbApiBuilder::default()
}

/// Returns the default Fdb cluster configuration file path
#[cfg(target_os = "linux")]
pub fn default_config_path() -> &'static str {
    "/etc/foundationdb/fdb.cluster"
}

/// Returns the default Fdb cluster configuration file path
#[cfg(target_os = "macos")]
pub fn default_config_path() -> &'static str {
    "/usr/local/etc/foundationdb/fdb.cluster"
}
