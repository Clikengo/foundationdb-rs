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
use futures::{Async, Future, Stream};
use std;
use std::sync::Arc;

use database::*;
use error::{self, *};
use future::*;
use keyselector::*;
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

/// Converts Rust `bool` into `fdb::fdb_bool_t`
fn fdb_bool(v: bool) -> fdb::fdb_bool_t {
    if v {
        1
    } else {
        0
    }
}

/// Foundationdb API uses `c_int` type as a length of a value, while Rust uses `usize` for. Rust
/// inteface uses `usize` if it represents length or size of something. Those `usize` values should
/// be converted to `c_int` before passed to ffi, because naive casting with `v as i32` will
/// convert some `usize` values to unsigned one.
/// TODO: check if inverse function is needed, `cint_to_usize(v: c_int) -> usize`?
fn usize_trunc(v: usize) -> std::os::raw::c_int {
    if v > std::i32::MAX as usize {
        std::i32::MAX
    } else {
        v as i32
    }
}

/// TBD
pub struct RangeOption<'a> {
    begin: KeySelector<'a>,
    end: KeySelector<'a>,
    limit: usize,
    target_bytes: usize,
    mode: options::StreamingMode,
    //TODO: move snapshot out from `RangeOption`, as other methods like `Transaction::get` do?
    snapshot: bool,
    reverse: bool,
}

impl<'a> Default for RangeOption<'a> {
    fn default() -> Self {
        Self {
            begin: KeySelector::first_greater_or_equal(&[0]),
            end: KeySelector::first_greater_or_equal(&[0]),
            limit: std::usize::MAX,
            target_bytes: 0,
            mode: options::StreamingMode::Iterator,
            snapshot: false,
            reverse: false,
        }
    }
}

/// TBD
pub struct RangeOptionBuilder<'a>(RangeOption<'a>);
impl<'a> RangeOptionBuilder<'a> {
    /// TBD
    pub fn new(begin: KeySelector<'a>, end: KeySelector<'a>) -> Self {
        let mut opt = RangeOption::default();
        opt.begin = begin;
        opt.end = end;
        RangeOptionBuilder(opt)
    }

    /// If non-zero, indicates the maximum number of key-value pairs to return.
    pub fn limit(mut self, limit: usize) -> Self {
        self.0.limit = limit;
        self
    }

    /// If non-zero, indicates a (soft) cap on the combined number of bytes of keys and values to
    /// return for each item.
    pub fn target_bytes(mut self, target_bytes: usize) -> Self {
        self.0.target_bytes = target_bytes;
        self
    }

    /// One of the options::StreamingMode values indicating how the caller would like the data in
    /// the range returned.
    pub fn mode(mut self, mode: options::StreamingMode) -> Self {
        self.0.mode = mode;
        self
    }

    /// Non-zero if this is a snapshot read.
    pub fn snapshot(mut self, snapshot: bool) -> Self {
        self.0.snapshot = snapshot;
        self
    }

    /// If non-zero, key-value pairs will be returned in reverse lexicographical order beginning at
    /// the end of the range.
    pub fn reverse(mut self, reverse: bool) -> Self {
        self.0.reverse = reverse;
        self
    }

    /// Finalizes the construction of the RangeOption
    pub fn build(self) -> RangeOption<'a> {
        self.0
    }
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
    pub fn get(&self, key: &[u8], snapshot: bool) -> TrxGet {
        let trx = self.inner.inner;

        let f = unsafe {
            fdb::fdb_transaction_get(
                trx,
                key.as_ptr() as *const _,
                key.len() as i32,
                fdb_bool(snapshot),
            )
        };
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

    /// Resolves a key selector against the keys in the database snapshot represented by
    /// transaction.
    ///
    /// Returns an FDBFuture which will be set to the key in the database matching the key
    /// selector. You must first wait for the FDBFuture to be ready, check for errors, call
    /// fdb_future_get_key() to extract the key, and then destroy the FDBFuture with
    /// fdb_future_destroy().
    pub fn get_key(&self, selector: KeySelector, snapshot: bool) -> TrxGetKey {
        let trx = self.inner.inner;

        let key = selector.key();

        let f = unsafe {
            fdb::fdb_transaction_get_key(
                trx,
                key.as_ptr() as *const _,
                key.len() as i32,
                fdb_bool(selector.or_equal()),
                selector.offset() as i32,
                fdb_bool(snapshot),
            )
        };
        let f = FdbFuture::new(f);
        TrxGetKey {
            trx: self.clone(),
            inner: f,
        }
    }

    /// TBD
    //TODO: proper naming
    pub fn get_range(&self, opt: RangeOption) -> KeyValuesStream {
        let iteration = 0;

        let mut stream = KeyValuesStream {
            begin: opt.begin.to_owned(),
            end: opt.end.to_owned(),
            limit: opt.limit,
            target_bytes: opt.target_bytes,
            mode: opt.mode,
            iteration,
            snapshot: opt.snapshot,
            reverse: opt.reverse,

            trx: self.clone(),
            inner: None,
            index: 0,
        };
        stream.update_inner();
        stream
    }

    //TODO: should we expose safe low-level API?
    //TODO: should we accepts `RangeOption` for the API?
    #[allow(unused)]
    fn get_range_inner(
        &self,
        begin: KeySelector,
        end: KeySelector,
        limit: usize,
        target_bytes: usize,
        mode: options::StreamingMode,
        iteration: usize,
        snapshot: bool,
        reverse: bool,
    ) -> TrxGetRange {
        let trx = self.inner.inner;

        let key_begin = begin.key();
        let key_end = end.key();

        let f = unsafe {
            fdb::fdb_transaction_get_range(
                trx,
                key_begin.as_ptr() as *const _,
                key_begin.len() as i32,
                fdb_bool(begin.or_equal()),
                begin.offset() as i32,
                key_end.as_ptr() as *const _,
                key_end.len() as i32,
                fdb_bool(end.or_equal()),
                end.offset() as i32,
                usize_trunc(limit),
                usize_trunc(target_bytes),
                mode.code(),
                iteration as i32,
                fdb_bool(snapshot),
                fdb_bool(reverse),
            )
        };

        let f = FdbFuture::new(f);
        TrxGetRange {
            trx: self.clone(),
            inner: f,
        }
    }

    /// Modify the database snapshot represented by transaction to remove all keys (if any) which
    /// are lexicographically greater than or equal to the given begin key and lexicographically
    /// less than the given end_key.
    ///
    /// The modification affects the actual database only if transaction is later committed with
    /// `Tranasction::commit`.
    pub fn clear_range(&self, begin: &[u8], end: &[u8]) {
        let trx = self.inner.inner;
        unsafe {
            fdb::fdb_transaction_clear_range(
                trx,
                begin.as_ptr() as *const _,
                begin.len() as i32,
                end.as_ptr() as *const _,
                end.len() as i32,
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

/// Represents the data of a `Transaction::get_key`
pub struct GetKeyResult {
    trx: Transaction,
    inner: FdbFutureResult,
}
impl GetKeyResult {
    /// Returns a clone of the Transaction this get is a part of
    pub fn transaction(&self) -> Transaction {
        self.trx.clone()
    }

    /// Returns the values associated with this get
    pub fn value(&self) -> Result<&[u8]> {
        self.inner.get_key()
    }
}

/// A future results of a `get_key` operation
pub struct TrxGetKey {
    trx: Transaction,
    inner: FdbFuture,
}
impl Future for TrxGetKey {
    type Item = GetKeyResult;
    type Error = FdbError;

    fn poll(&mut self) -> std::result::Result<Async<Self::Item>, Self::Error> {
        match self.inner.poll() {
            Ok(Async::Ready(r)) => Ok(Async::Ready(GetKeyResult {
                trx: self.trx.clone(),
                inner: r,
            })),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(e),
        }
    }
}

/// Represents the data of a `Transaction::get_range`
pub struct GetRangeResult {
    trx: Transaction,
    inner: FdbFutureResult,
}
impl GetRangeResult {
    /// Returns a clone of the Transaction this get is a part of
    pub fn transaction(&self) -> Transaction {
        self.trx.clone()
    }

    /// Returns the values associated with this get
    pub fn keyvalues(&self) -> Result<KeyValues> {
        self.inner.get_keyvalue_array()
    }
}

/// A future results of a `get_range` operation
struct TrxGetRange {
    trx: Transaction,
    inner: FdbFuture,
}
impl Future for TrxGetRange {
    type Item = GetRangeResult;
    type Error = FdbError;

    fn poll(&mut self) -> std::result::Result<Async<Self::Item>, Self::Error> {
        match self.inner.poll() {
            Ok(Async::Ready(r)) => Ok(Async::Ready(GetRangeResult {
                trx: self.trx.clone(),
                inner: r,
            })),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(e),
        }
    }
}

/// TODO
pub struct KeyValuesStream {
    //TODO: use `RangeOption` instead?
    begin: OwnedKeySelector,
    end: OwnedKeySelector,

    limit: usize,
    target_bytes: usize,
    mode: options::StreamingMode,
    iteration: usize,
    snapshot: bool,
    reverse: bool,

    trx: Transaction,
    inner: Option<TrxGetRange>,
    index: usize,
}

impl KeyValuesStream {
    fn update_inner(&mut self) {
        self.iteration += 1;
        self.inner = Some(self.trx.get_range_inner(
            self.begin.as_selector(),
            self.end.as_selector(),
            self.limit - self.index,
            self.target_bytes,
            self.mode,
            self.iteration,
            self.snapshot,
            self.reverse,
        ));
    }

    fn advance(&mut self, res: &GetRangeResult) -> Result<()> {
        let kvs = res.keyvalues()?;
        let more = kvs.more();

        if !more {
            return Ok(());
        }

        let slice = kvs.as_ref();
        if slice.is_empty() {
            return Ok(());
        }
        self.index += slice.len();

        let last = slice.last().unwrap();
        let last_key = last.key();

        if self.reverse {
            self.end = KeySelector::first_greater_or_equal(last_key).to_owned();
        } else {
            self.begin = KeySelector::first_greater_than(last_key).to_owned();
        }

        self.update_inner();
        Ok(())
    }
}

impl<'a> Stream for KeyValuesStream {
    type Item = GetRangeResult;
    type Error = FdbError;

    fn poll(&mut self) -> std::result::Result<Async<Option<Self::Item>>, Self::Error> {
        if self.inner.is_none() {
            return Ok(Async::Ready(None));
        }

        let mut inner = self.inner.take().unwrap();
        match inner.poll() {
            Ok(Async::NotReady) => {
                self.inner = Some(inner);
                Ok(Async::NotReady)
            }
            Ok(Async::Ready(res)) => {
                self.advance(&res)?;
                Ok(Async::Ready(Some(res)))
            }
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
