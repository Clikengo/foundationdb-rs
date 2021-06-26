// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Directory provides a tool for managing related subspaces.
//!
//! The FoundationDB API provides directories as a tool for managing related Subspaces.
//! For general guidance on directory usage, see the discussion in the [Developer Guide](https://apple.github.io/foundationdb/developer-guide.html#directories).
//!
//! Directories are identified by hierarchical paths analogous to the paths in a Unix-like file system.
//! A path is represented as a slice of strings. Each directory has an associated subspace used to
//! store its content. The directory layer maps each path to a short prefix used for the
//! corresponding subspace. In effect, directories provide a level of indirection for access to subspaces.
//! Directory operations are transactional.
//!
//! It is a direct backport of the [Flow implementation](https://github.com/apple/foundationdb/tree/master/bindings/flow).
//!
//! Examples:
//!
//! ```rust
//! use futures::prelude::*;
//! use foundationdb::directory::Directory;
//!
//! async fn async_main() -> foundationdb::FdbResult<()> {
//!     let db = foundationdb::Database::default()?;
//!
//!     // creates a transaction
//!     let trx = db.create_trx()?;
//!
//!     // creates a directory
//!     let directory = foundationdb::directory::directory_layer::DirectoryLayer::default();
//!
//!     // use the directory to create a subspace to use
//!     let content_subspace = directory.create_or_open(
//!         // the transaction used to read/write the directory.
//!         &trx,
//!         // the path used, which can view as a UNIX path like `/app/my-app`.
//!         vec![String::from("my-awesome-app"), String::from("my-awesome-user")],
//!         // do not use any custom prefix or layer
//!         None, None,
//!     ).await;
//!     assert_eq!(true, content_subspace.is_ok());
//!     
//!     // Don't forget to commit your transaction to persist the subspace
//!     trx.commit().await?;
//!
//!     Ok(())
//! }
//!
//! // Safe because drop is called before the program exits
//! let network = unsafe { foundationdb::boot() };
//! futures::executor::block_on(async_main()).expect("failed to run");
//! drop(network);
//! ```
pub mod directory_layer;
pub mod directory_partition;
pub mod directory_subspace;
pub mod error;
pub(crate) mod node;

use crate::directory::directory_subspace::DirectorySubspace;
use crate::directory::error::DirectoryError;
use async_trait::async_trait;

use crate::Transaction;

use crate::directory::directory_partition::DirectoryPartition;
use crate::tuple::{PackResult, Subspace, TuplePack, TupleUnpack};
use core::cmp;
use std::cmp::Ordering;

/// `Directory` represents a subspace of keys in a FoundationDB database, identified by a hierarchical path.
#[async_trait]
pub trait Directory {
    /// Creates or opens the subdirectory of this Directory located at path (creating parent directories, if necessary).
    async fn create_or_open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError>;

    /// Creates a subdirectory of this Directory located at path (creating parent directories if necessary).
    async fn create(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError>;

    /// Opens the subdirectory of this Directory located at path.
    async fn open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError>;

    /// Checks if the subdirectory of this Directory located at path exists.
    async fn exists(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError>;

    /// Moves this Directory to the specified newAbsolutePath.
    async fn move_directory(
        &self,
        trx: &Transaction,
        new_path: Vec<String>,
    ) -> Result<DirectoryOutput, DirectoryError>;

    /// Moves the subdirectory of this Directory located at oldpath to newpath.
    async fn move_to(
        &self,
        trx: &Transaction,
        old_path: Vec<String>,
        new_path: Vec<String>,
    ) -> Result<DirectoryOutput, DirectoryError>;

    /// Removes the subdirectory of this Directory located at path and all of its subdirectories, as well as all of their contents.
    async fn remove(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError>;

    /// Removes the subdirectory of this Directory located at path (if the path exists) and all of its subdirectories, as well as all of their contents.
    async fn remove_if_exists(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<bool, DirectoryError>;

    /// List the subdirectories of this directory at a given subpath.
    async fn list(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<Vec<String>, DirectoryError>;
}

pub(crate) fn compare_slice<T: Ord>(a: &[T], b: &[T]) -> cmp::Ordering {
    for (ai, bi) in a.iter().zip(b.iter()) {
        match ai.cmp(&bi) {
            Ordering::Equal => continue,
            ord => return ord,
        }
    }

    // if every single element was equal, compare length
    a.len().cmp(&b.len())
}

/// DirectoryOutput represents the different output of a Directory.
#[derive(Clone, Debug)]
pub enum DirectoryOutput {
    /// Under classic usage, you will obtain an `DirectorySubspace`
    DirectorySubspace(DirectorySubspace),
    /// You can open an `DirectoryPartition` by using the "partition" layer
    DirectoryPartition(DirectoryPartition),
}

// TODO: should we have a Subspace trait?
impl DirectoryOutput {
    pub fn subspace<T: TuplePack>(&self, t: &T) -> Subspace {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.subspace(t),
            DirectoryOutput::DirectoryPartition(_) => {
                panic!("cannot open subspace in the root of a directory partition")
            }
        }
    }

    pub fn bytes(&self) -> &[u8] {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.bytes(),
            DirectoryOutput::DirectoryPartition(_) => {
                panic!("cannot get key for the root of a directory partition")
            }
        }
    }

    pub fn pack<T: TuplePack>(&self, t: &T) -> Vec<u8> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.pack(t),
            DirectoryOutput::DirectoryPartition(_) => {
                panic!("cannot pack for the root of a directory partition")
            }
        }
    }

    pub fn unpack<'de, T: TupleUnpack<'de>>(&self, key: &'de [u8]) -> PackResult<T> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.unpack(key),
            DirectoryOutput::DirectoryPartition(_) => {
                panic!("cannot unpack keys using the root of a directory partition")
            }
        }
    }

    pub fn range(&self) -> (Vec<u8>, Vec<u8>) {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.range(),
            DirectoryOutput::DirectoryPartition(_) => {
                panic!("cannot get range for the root of a directory partition")
            }
        }
    }

    pub fn get_path(&self) -> Vec<String> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.get_path(),
            DirectoryOutput::DirectoryPartition(d) => d.get_path(),
        }
    }

    pub fn get_layer(&self) -> Vec<u8> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.get_layer(),
            DirectoryOutput::DirectoryPartition(d) => d.get_layer(),
        }
    }
}

#[async_trait]
impl Directory for DirectoryOutput {
    async fn create_or_open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => {
                d.create_or_open(txn, path, prefix, layer).await
            }
            DirectoryOutput::DirectoryPartition(d) => {
                d.create_or_open(txn, path, prefix, layer).await
            }
        }
    }

    async fn create(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        prefix: Option<Vec<u8>>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.create(txn, path, prefix, layer).await,
            DirectoryOutput::DirectoryPartition(d) => d.create(txn, path, prefix, layer).await,
        }
    }

    async fn open(
        &self,
        txn: &Transaction,
        path: Vec<String>,
        layer: Option<Vec<u8>>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.open(txn, path, layer).await,
            DirectoryOutput::DirectoryPartition(d) => d.open(txn, path, layer).await,
        }
    }

    async fn exists(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.exists(trx, path).await,
            DirectoryOutput::DirectoryPartition(d) => d.exists(trx, path).await,
        }
    }

    async fn move_directory(
        &self,
        trx: &Transaction,
        new_path: Vec<String>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.move_directory(trx, new_path).await,
            DirectoryOutput::DirectoryPartition(d) => d.move_directory(trx, new_path).await,
        }
    }

    async fn move_to(
        &self,
        trx: &Transaction,
        old_path: Vec<String>,
        new_path: Vec<String>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.move_to(trx, old_path, new_path).await,
            DirectoryOutput::DirectoryPartition(d) => d.move_to(trx, old_path, new_path).await,
        }
    }

    async fn remove(&self, trx: &Transaction, path: Vec<String>) -> Result<bool, DirectoryError> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.remove(trx, path).await,
            DirectoryOutput::DirectoryPartition(d) => d.remove(trx, path).await,
        }
    }

    async fn remove_if_exists(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<bool, DirectoryError> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.remove_if_exists(trx, path).await,
            DirectoryOutput::DirectoryPartition(d) => d.remove_if_exists(trx, path).await,
        }
    }

    async fn list(
        &self,
        trx: &Transaction,
        path: Vec<String>,
    ) -> Result<Vec<String>, DirectoryError> {
        match self {
            DirectoryOutput::DirectorySubspace(d) => d.list(trx, path).await,
            DirectoryOutput::DirectoryPartition(d) => d.list(trx, path).await,
        }
    }
}

// Strinc returns the first key that would sort outside the range prefixed by prefix.
pub(crate) fn strinc(key: Vec<u8>) -> Vec<u8> {
    let mut key = key;

    for i in (0..key.len()).rev() {
        if key[i] != 0xff {
            key[i] += 1;
            return key;
        } else {
            // stripping key from trailing 0xFF bytes
            key.remove(i);
        }
    }
    panic!("failed to strinc");
}

#[cfg(test)]
mod tests {
    use super::*;

    // https://github.com/apple/foundationdb/blob/e34df983ee8c0db333babf36fb620318d026553d/bindings/c/test/unit/unit_tests.cpp#L95
    #[test]
    fn test_strinc() {
        assert_eq!(strinc(Vec::from("a".as_bytes())), Vec::from("b".as_bytes()));
        assert_eq!(strinc(Vec::from("y".as_bytes())), Vec::from("z".as_bytes()));
        assert_eq!(
            strinc(Vec::from("!".as_bytes())),
            Vec::from("\"".as_bytes())
        );
        assert_eq!(strinc(Vec::from("*".as_bytes())), Vec::from("+".as_bytes()));
        assert_eq!(
            strinc(Vec::from("fdb".as_bytes())),
            Vec::from("fdc".as_bytes())
        );
        assert_eq!(
            strinc(Vec::from("foundation database 6".as_bytes())),
            Vec::from("foundation database 7".as_bytes())
        );

        assert_eq!(strinc(vec![61u8, 62u8, 255u8]), vec![61u8, 63u8]);
        // from seed 3180880087
        assert_eq!(strinc(vec![253u8, 255u8]), vec![254u8]);
        assert_eq!(strinc(vec![253u8, 255u8, 255u8]), vec![254u8]);
    }
}
