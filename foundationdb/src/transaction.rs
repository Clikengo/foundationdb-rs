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
use error::{self, *};
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

    /// Modify the database snapshot represented by transaction to perform the operation indicated
    /// by operationType with operand param to the value stored by the given key.
    ///
    /// An atomic operation is a single database command that carries out several logical steps:
    /// reading the value of a key, performing a transformation on that value, and writing the
    /// result. Different atomic operations perform different transformations. Like other database
    /// operations, an atomic operation is used within a transaction; however, its use within a
    /// transaction will not cause the transaction to conflict.
    ///
    /// Atomic operations do not expose the current value of the key to the client but simply send
    /// the database the transformation to apply. In regard to conflict checking, an atomic
    /// operation is equivalent to a write without a read. It can only cause other transactions
    /// performing reads of the key to conflict.
    ///
    /// By combining these logical steps into a single, read-free operation, FoundationDB can
    /// guarantee that the transaction will not conflict due to the operation. This makes atomic
    /// operations ideal for operating on keys that are frequently modified. A common example is
    /// the use of a key-value pair as a counter.
    pub fn atomic_op(&self, key: &[u8], param: &[u8], op_type: options::MutationType) {
        let trx = self.inner.inner;
        unsafe {
            fdb::fdb_transaction_atomic_op(
                trx,
                key.as_ptr() as *const _,
                key.len() as i32,
                param.as_ptr() as *const _,
                param.len() as i32,
                op_type.code(),
            )
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

    /// Cancels the transaction. All pending or future uses of the transaction will return a
    /// transaction_cancelled error. The transaction can be used again after it is reset.
    ///
    /// # Warning
    ///
    /// * Be careful if you are using fdb_transaction_reset() and fdb_transaction_cancel()
    /// concurrently with the same transaction. Since they negate each other’s effects, a race
    /// condition between these calls will leave the transaction in an unknown state.
    ///
    /// * If your program attempts to cancel a transaction after fdb_transaction_commit() has been
    /// called but before it returns, unpredictable behavior will result. While it is guaranteed
    /// that the transaction will eventually end up in a cancelled state, the commit may or may not
    /// occur. Moreover, even if the call to fdb_transaction_commit() appears to return a
    /// transaction_cancelled error, the commit may have occurred or may occur in the future. This
    /// can make it more difficult to reason about the order in which transactions occur.
    pub fn cancel(self) {
        let trx = self.inner.inner;
        unsafe { fdb::fdb_transaction_cancel(trx) }
    }

    /// Retrieves the database version number at which a given transaction was committed.
    /// fdb_transaction_commit() must have been called on transaction and the resulting future must
    /// be ready and not an error before this function is called, or the behavior is undefined.
    /// Read-only transactions do not modify the database when committed and will have a committed
    /// version of -1. Keep in mind that a transaction which reads keys and then sets them to their
    /// current values may be optimized to a read-only transaction.
    ///
    /// Note that database versions are not necessarily unique to a given transaction and so cannot
    /// be used to determine in what order two transactions completed. The only use for this
    /// function is to manually enforce causal consistency when calling
    /// fdb_transaction_set_read_version() on another subsequent transaction.
    ///
    /// Most applications will not call this function.
    pub fn committed_version(&self) -> Result<i64> {
        let trx = self.inner.inner;

        let mut version: i64 = 0;
        let e = unsafe { fdb::fdb_transaction_get_committed_version(trx, &mut version as *mut _) };
        error::eval(e)?;
        Ok(version)
    }

    /// Returns a list of public network addresses as strings, one for each of the storage servers
    /// responsible for storing key_name and its associated value.
    ///
    /// Returns an FDBFuture which will be set to an array of strings. You must first wait for the
    /// FDBFuture to be ready, check for errors, call fdb_future_get_string_array() to extract the
    /// string array, and then destroy the FDBFuture with fdb_future_destroy().
    pub fn get_addresses_for_key(&self, key: &[u8]) -> TrxGetAddressesForKey {
        let trx = self.inner.inner;

        let f = unsafe {
            fdb::fdb_transaction_get_addresses_for_key(
                trx,
                key.as_ptr() as *const _,
                key.len() as i32,
            )
        };
        let f = FdbFuture::new(f);
        TrxGetAddressesForKey {
            trx: self.clone(),
            inner: f,
        }
    }

    /// A watch’s behavior is relative to the transaction that created it. A watch will report a
    /// change in relation to the key’s value as readable by that transaction. The initial value
    /// used for comparison is either that of the transaction’s read version or the value as
    /// modified by the transaction itself prior to the creation of the watch. If the value changes
    /// and then changes back to its initial value, the watch might not report the change.
    ///
    /// Until the transaction that created it has been committed, a watch will not report changes
    /// made by other transactions. In contrast, a watch will immediately report changes made by
    /// the transaction itself. Watches cannot be created if the transaction has set the
    /// READ_YOUR_WRITES_DISABLE transaction option, and an attempt to do so will return an
    /// watches_disabled error.
    ///
    /// If the transaction used to create a watch encounters an error during commit, then the watch
    /// will be set with that error. A transaction whose commit result is unknown will set all of
    /// its watches with the commit_unknown_result error. If an uncommitted transaction is reset or
    /// destroyed, then any watches it created will be set with the transaction_cancelled error.
    ///
    /// Returns an FDBFuture representing an empty value that will be set once the watch has
    /// detected a change to the value at the specified key. You must first wait for the FDBFuture
    /// to be ready, check for errors, and then destroy the FDBFuture with fdb_future_destroy().
    ///
    /// By default, each database connection can have no more than 10,000 watches that have not yet
    /// reported a change. When this number is exceeded, an attempt to create a watch will return a
    /// too_many_watches error. This limit can be changed using the MAX_WATCHES database option.
    /// Because a watch outlives the transaction that creates it, any watch that is no longer
    /// needed should be cancelled by calling fdb_future_cancel() on its returned future.
    pub fn watch(&self, key: &[u8]) -> TrxWatch {
        let trx = self.inner.inner;

        let f =
            unsafe { fdb::fdb_transaction_watch(trx, key.as_ptr() as *const _, key.len() as i32) };
        let f = FdbFuture::new(f);
        TrxWatch { inner: f }
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

/// Represents the data of a `Transaction::get_addresses_for_key`
pub struct GetAddressResult {
    trx: Transaction,
    inner: FdbFutureResult,
}
impl GetAddressResult {
    /// Returns a clone of the Transaction this get is a part of
    pub fn transaction(&self) -> Transaction {
        self.trx.clone()
    }

    /// Returns the addresses for the key
    pub fn address(&self) -> Result<Vec<&[u8]>> {
        self.inner.get_string_array()
    }
}

/// A future result of a `Transaction::get_addresses_for_key`
pub struct TrxGetAddressesForKey {
    trx: Transaction,
    inner: FdbFuture,
}
impl Future for TrxGetAddressesForKey {
    type Item = GetAddressResult;
    type Error = FdbError;

    fn poll(&mut self) -> std::result::Result<Async<Self::Item>, Self::Error> {
        match self.inner.poll() {
            Ok(Async::Ready(r)) => Ok(Async::Ready(GetAddressResult {
                trx: self.trx.clone(),
                inner: r,
            })),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(e),
        }
    }
}

/// A future result of a `Transaction::watch`
pub struct TrxWatch {
    inner: FdbFuture,
}
impl Future for TrxWatch {
    type Item = ();
    type Error = FdbError;

    fn poll(&mut self) -> std::result::Result<Async<Self::Item>, Self::Error> {
        match self.inner.poll() {
            Ok(Async::Ready(_r)) => Ok(Async::Ready(())),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(e),
        }
    }
}
