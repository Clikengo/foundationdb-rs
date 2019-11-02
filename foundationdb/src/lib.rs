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
//! futures = "0.1"
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
//! use futures::prelude::*;
//! use std::ops::Deref;
//!
//! async fn async_main() -> foundationdb::error::Result<()> {
//!     let db = foundationdb::Database::default()?;
//!     
//!     // write a value
//!     let trx = db.create_trx()?;
//!     trx.set(b"hello", b"world"); // errors will be returned in the future result
//!     trx.commit().await?;
//!
//!     // read a value
//!     let trx = db.create_trx()?;
//!     let maybe_value = trx.get(b"hello", false).await?;
//!     let value = maybe_value.unwrap(); // unwrap the option
//!
//!     assert_eq!(b"world", value.deref());
//!
//!     Ok(())
//! }
//!
//! let network = foundationdb::boot().expect("failed to initialize Fdb");
//! futures::executor::block_on(async_main()).expect("failed to run");
//! // cleanly shutdown the client
//! drop(network);
//! ```
//!
//! ## API stability
//!
//! *WARNING* Until the 1.0 release of this library, the API may be in constant flux.

//#![deny(missing_docs)]

#[macro_use]
extern crate failure;

#[macro_use]
extern crate static_assertions;

pub mod api;
#[cfg(any(feature = "fdb-5_1", feature = "fdb-5_2", feature = "fdb-6_0"))]
pub mod cluster;
pub mod database;
pub mod error;
pub mod future;
pub mod keyselector;
/// Generated configuration types for use with the various `set_option` functions
#[allow(missing_docs)]
pub mod options;
pub mod transaction;

#[cfg(feature = "serde")]
pub mod tuple;

#[cfg(any(feature = "fdb-5_1", feature = "fdb-5_2", feature = "fdb-6_0"))]
pub use crate::cluster::Cluster;

pub use crate::database::{Database, TransactError, TransactOption};
pub use crate::error::Error as FdbError;
pub use crate::error::Result as FdbResult;
pub use crate::keyselector::KeySelector;
pub use crate::transaction::{
    RangeOptionBuilder, Transaction, TransactionCancelled, TransactionCommitError,
    TransactionCommitted,
};

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
/// let network = foundationdb::boot().expect("failed to initialize Fdb");
///
/// // do some interesting things with the API...
///
/// drop(network)
/// ```
pub fn boot() -> error::Result<api::NetworkAutoStop> {
    api::FdbApiBuilder::default().build()?.boot()
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

/// Returns the default Fdb cluster configuration file path
#[cfg(target_os = "windows")]
pub fn default_config_path() -> &'static str {
    "C:/ProgramData/foundationdb/fdb.cluster"
}
