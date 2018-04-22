// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Implementations of the FDBTransaction C API
//!
//! https://apple.github.io/foundationdb/api-c.html#transaction

use foundationdb_sys as fdb;
use futures::{Async, Future};
use std;
use std::sync::Arc;

use database::*;
use error::*;
use future::*;
use options;

/// In FoundationDB, a transaction is a mutable snapshot of a database.
///
/// All read and write operations on a transaction see and modify an otherwise-unchanging version of the database and only change the underlying database if and when the transaction is committed. Read operations do see the effects of previous write operations on the same transaction. Committing a transaction usually succeeds in the absence of conflicts.
///
/// Applications must provide error handling and an appropriate retry loop around the application code for a transaction. See the documentation for [fdb_transaction_on_error()](https://apple.github.io/foundationdb/api-c.html#transaction).
///
/// Transactions group operations into a unit with the properties of atomicity, isolation, and durability. Transactions also provide the ability to maintain an application’s invariants or integrity constraints, supporting the property of consistency. Together these properties are known as ACID.
///
/// Transactions are also causally consistent: once a transaction has been successfully committed, all subsequently created transactions will see the modifications made by it.
#[derive(Clone)]
pub struct Transaction {
    database: Database,
    inner: Arc<TransactionInner>,
}

// TODO: many implementations left
impl Transaction {
    pub(crate) fn new(database: Database, trx: *mut fdb::FDBTransaction) -> Self {
        let inner = Arc::new(TransactionInner::new(trx));
        Self { database, inner }
    }

    /// Called to set an option on an FDBTransaction.
    pub fn set_option(&self, opt: options::TransactionOption) -> Result<()> {
        unsafe { opt.apply(self.inner.inner) }
    }

    /// Returns a clone of this transactions Database
    pub fn database(&self) -> Database {
        self.database.clone()
    }

    /// Modify the database snapshot represented by transaction to change the given key to have the given value.
    ///
    /// If the given key was not previously present in the database it is inserted. The modification affects the actual database only if transaction is later committed with `Transaction::commit`.
    ///
    /// # Arguments
    ///
    /// * `key_name` - the name of the key to be inserted into the database.
    /// * `value` - the value to be inserted into the database
    pub fn set(&self, key: &[u8], value: &[u8]) {
        let trx = self.inner.inner;
        unsafe {
            fdb::fdb_transaction_set(
                trx,
                key.as_ptr(),
                key.len() as i32,
                value.as_ptr(),
                value.len() as i32,
            )
        }
    }

    /// Modify the database snapshot represented by transaction to remove the given key from the database.
    ///
    /// If the key was not previously present in the database, there is no effect. The modification affects the actual database only if transaction is later committed with `Transaction::commit`.
    ///
    /// # Arguments
    ///
    /// * `key_name` - the name of the key to be removed from the database.
    pub fn clear(&self, key: &[u8]) {
        let trx = self.inner.inner;
        unsafe { fdb::fdb_transaction_clear(trx, key.as_ptr(), key.len() as i32) }
    }

    /// Reads a value from the database snapshot represented by transaction.
    ///
    /// Returns an FDBFuture which will be set to the value of key_name in the database. You must first wait for the FDBFuture to be ready, check for errors, call fdb_future_get_value() to extract the value, and then destroy the FDBFuture with fdb_future_destroy().
    ///
    /// See `FdbFutureResult::value` to see exactly how results are unpacked. If key_name is not present in the database, the result is not an error, but a zero for *out_present returned from that function.
    ///
    /// # Arguments
    ///
    /// * `key_name` - the name of the key to be looked up in the database
    ///
    /// TODO: implement: snapshot Non-zero if this is a snapshot read.
    pub fn get(&self, key: &[u8]) -> TrxGet {
        let trx = self.inner.inner;

        let f =
            unsafe { fdb::fdb_transaction_get(trx, key.as_ptr() as *const _, key.len() as i32, 0) };
        let f = FdbFuture::new(f);
        TrxGet {
            trx: self.clone(),
            inner: f,
        }
    }

    /// Attempts to commit the sets and clears previously applied to the database snapshot represented by transaction to the actual database.
    ///
    /// The commit may or may not succeed – in particular, if a conflicting transaction previously committed, then the commit must fail in order to preserve transactional isolation. If the commit does succeed, the transaction is durably committed to the database and all subsequently started transactions will observe its effects.
    ///
    /// It is not necessary to commit a read-only transaction – you can simply call fdb_transaction_destroy().
    ///
    /// Returns an `FdbFuture` representing an empty value.
    ///
    /// Callers will usually want to retry a transaction if the commit or a prior fdb_transaction_get_*() returns a retryable error (see fdb_transaction_on_error()).
    ///
    /// As with other client/server databases, in some failure scenarios a client may be unable to determine whether a transaction succeeded. In these cases, `Transaction::commit` will return a commit_unknown_result error. The fdb_transaction_on_error() function treats this error as retryable, so retry loops that don’t check for commit_unknown_result could execute the transaction twice. In these cases, you must consider the idempotence of the transaction. For more information, see Transactions with unknown results.
    ///
    /// Normally, commit will wait for outstanding reads to return. However, if those reads were snapshot reads or the transaction option for disabling “read-your-writes” has been invoked, any outstanding reads will immediately return errors.
    pub fn commit(self) -> TrxCommit {
        let trx = self.inner.inner;

        let f = unsafe { fdb::fdb_transaction_commit(trx) };
        let f = FdbFuture::new(f);
        TrxCommit {
            trx: Some(self),
            inner: f,
        }
    }
}

struct TransactionInner {
    inner: *mut fdb::FDBTransaction,
}
impl TransactionInner {
    fn new(inner: *mut fdb::FDBTransaction) -> Self {
        Self { inner }
    }
}
impl Drop for TransactionInner {
    fn drop(&mut self) {
        unsafe {
            fdb::fdb_transaction_destroy(self.inner);
        }
    }
}

/// Represents the data of a `Transaction::get`
pub struct GetResult {
    trx: Transaction,
    inner: FdbFutureResult,
}
impl GetResult {
    /// Returns a clone of the Transaction this get is a part of
    pub fn transaction(&self) -> Transaction {
        self.trx.clone()
    }

    /// Returns the values associated with this get
    pub fn value(&self) -> Result<Option<&[u8]>> {
        self.inner.get_value()
    }
}

/// A future results of a get operation
pub struct TrxGet {
    trx: Transaction,
    inner: FdbFuture,
}
impl Future for TrxGet {
    type Item = GetResult;
    type Error = FdbError;

    fn poll(&mut self) -> std::result::Result<Async<Self::Item>, Self::Error> {
        match self.inner.poll() {
            Ok(Async::Ready(r)) => Ok(Async::Ready(GetResult {
                trx: self.trx.clone(),
                inner: r,
            })),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(e),
        }
    }
}

/// A future result of a `Transaction::commit`
pub struct TrxCommit {
    trx: Option<Transaction>,
    inner: FdbFuture,
}

impl Future for TrxCommit {
    type Item = Transaction;
    type Error = FdbError;

    fn poll(&mut self) -> std::result::Result<Async<Self::Item>, Self::Error> {
        match self.inner.poll() {
            Ok(Async::Ready(_r)) => Ok(Async::Ready(
                self.trx.take().expect("should not poll after ready"),
            )),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(e),
        }
    }
}
