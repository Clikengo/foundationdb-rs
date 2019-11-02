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

use foundationdb_sys as fdb_sys;
use std::borrow::Cow;
use std::fmt;
use std::ops::Deref;
use std::ptr::NonNull;

use crate::error::{self, *};
use crate::future::*;
use crate::keyselector::*;
use crate::options;

use futures::{
    future, future::Either, stream, Future, FutureExt, TryFutureExt, TryStream, TryStreamExt,
};

pub struct TransactionCommitted {
    tr: Transaction,
}

impl TransactionCommitted {
    pub fn committed_version(&self) -> Result<i64> {
        let mut version: i64 = 0;
        error::eval(unsafe {
            fdb_sys::fdb_transaction_get_committed_version(self.tr.inner.as_ptr(), &mut version)
        })?;
        Ok(version)
    }

    pub fn reset(mut self) -> Transaction {
        self.tr.reset();
        self.tr
    }
}

pub struct TransactionCommitError {
    tr: Transaction,
    err: Error,
}

impl TransactionCommitError {
    pub fn is_maybe_committed(&self) -> bool {
        self.err.is_maybe_committed()
    }

    pub fn on_error(self) -> impl Future<Output = Result<Transaction>> {
        FdbFuture::<()>::new(unsafe {
            fdb_sys::fdb_transaction_on_error(self.tr.inner.as_ptr(), self.err.code())
        })
        .map_ok(|()| self.tr)
    }

    pub fn reset(mut self) -> Transaction {
        self.tr.reset();
        self.tr
    }
}

impl Deref for TransactionCommitError {
    type Target = Error;
    fn deref(&self) -> &Error {
        &self.err
    }
}

impl From<TransactionCommitError> for Error {
    fn from(tce: TransactionCommitError) -> Error {
        tce.err
    }
}
pub struct TransactionCancelled {
    tr: Transaction,
}
impl TransactionCancelled {
    pub fn reset(mut self) -> Transaction {
        self.tr.reset();
        self.tr
    }
}

impl fmt::Debug for TransactionCommitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.err.fmt(f)
    }
}

type TransactionResult = std::result::Result<TransactionCommitted, TransactionCommitError>;

/// In FoundationDB, a transaction is a mutable snapshot of a database.
///
/// All read and write operations on a transaction see and modify an otherwise-unchanging version of the database and only change the underlying database if and when the transaction is committed. Read operations do see the effects of previous write operations on the same transaction. Committing a transaction usually succeeds in the absence of conflicts.
///
/// Applications must provide error handling and an appropriate retry loop around the application code for a transaction. See the documentation for [fdb_transaction_on_error()](https://apple.github.io/foundationdb/api-c.html#transaction).
///
/// Transactions group operations into a unit with the properties of atomicity, isolation, and durability. Transactions also provide the ability to maintain an application’s invariants or integrity constraints, supporting the property of consistency. Together these properties are known as ACID.
///
/// Transactions are also causally consistent: once a transaction has been successfully committed, all subsequently created transactions will see the modifications made by it.
pub struct Transaction {
    // Order of fields should not be changed, because Rust drops field top-to-bottom, and
    // transaction should be dropped before cluster.
    inner: NonNull<fdb_sys::FDBTransaction>,
}
unsafe impl Send for Transaction {}
unsafe impl Sync for Transaction {}

/// Converts Rust `bool` into `fdb_sys::fdb_bool_t`
#[inline]
fn fdb_bool(v: bool) -> fdb_sys::fdb_bool_t {
    if v {
        1
    } else {
        0
    }
}
#[inline]
fn fdb_len(len: usize, context: &'static str) -> std::os::raw::c_int {
    assert!(
        len <= i32::max_value() as usize,
        "{}.len() > i32::max_value()",
        context
    );
    len as i32
}
#[inline]
fn fdb_iteration(iteration: usize) -> std::os::raw::c_int {
    if iteration > i32::max_value() as usize {
        0 // this will cause client_invalid_operation
    } else {
        iteration as i32
    }
}
#[inline]
fn fdb_limit(v: usize) -> std::os::raw::c_int {
    if v > i32::max_value() as usize {
        i32::max_value()
    } else {
        v as i32
    }
}

/// `RangeOption` represents a query parameters for range scan query.
#[derive(Debug, Clone)]
pub struct RangeOption<'a> {
    begin: KeySelector<'a>,
    end: KeySelector<'a>,
    limit: Option<usize>,
    target_bytes: usize,
    mode: options::StreamingMode,
    reverse: bool,
}

impl<'a> RangeOption<'a> {
    fn next_range(mut self, kvs: &FdbFutureValues) -> Option<Self> {
        if !kvs.more {
            return None;
        }

        let last = kvs.last()?;
        let last_key = last.key();

        if let Some(limit) = self.limit.as_mut() {
            *limit -= kvs.len();
            if *limit == 0 {
                return None;
            }
        }

        if self.reverse {
            self.end.make_first_greater_or_equal(last_key);
        } else {
            self.begin.make_first_greater_than(last_key);
        }
        Some(self)
    }
}

impl<'a> Default for RangeOption<'a> {
    fn default() -> Self {
        Self {
            begin: KeySelector::first_greater_or_equal(Cow::Borrowed(&[])),
            end: KeySelector::first_greater_or_equal(Cow::Borrowed(&[])),
            limit: None,
            target_bytes: 0,
            mode: options::StreamingMode::Iterator,
            reverse: false,
        }
    }
}

impl<'a> From<(&'a [u8], &'a [u8])> for RangeOption<'a> {
    fn from((begin, end): (&'a [u8], &'a [u8])) -> Self {
        RangeOptionBuilder::from((begin, end)).build()
    }
}

/// A Builder with which options need to used for a range query.
pub struct RangeOptionBuilder<'a>(RangeOption<'a>);

impl<'a> From<(&'a [u8], &'a [u8])> for RangeOptionBuilder<'a> {
    fn from((begin, end): (&'a [u8], &'a [u8])) -> Self {
        Self::new(
            KeySelector::first_greater_or_equal(Cow::Borrowed(begin)),
            KeySelector::first_greater_or_equal(Cow::Borrowed(end)),
        )
    }
}

impl<'a> RangeOptionBuilder<'a> {
    /// Creates new builder with given key selectors.
    pub fn new(begin: KeySelector<'a>, end: KeySelector<'a>) -> Self {
        RangeOptionBuilder(RangeOption {
            begin,
            end,
            ..RangeOption::default()
        })
    }

    /// If non-zero, indicates the maximum number of key-value pairs to return.
    pub fn limit(mut self, limit: usize) -> Self {
        if limit > 0 {
            self.0.limit = Some(limit);
        }
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
    pub(crate) fn new(inner: NonNull<fdb_sys::FDBTransaction>) -> Self {
        Self { inner }
    }

    /// Called to set an option on an FDBTransaction.
    pub fn set_option(&self, opt: options::TransactionOption) -> Result<()> {
        unsafe { opt.apply(self.inner.as_ptr()) }
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
        unsafe {
            fdb_sys::fdb_transaction_set(
                self.inner.as_ptr(),
                key.as_ptr(),
                fdb_len(key.len(), "key"),
                value.as_ptr(),
                fdb_len(value.len(), "value"),
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
        unsafe {
            fdb_sys::fdb_transaction_clear(
                self.inner.as_ptr(),
                key.as_ptr(),
                fdb_len(key.len(), "key"),
            )
        }
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
    pub fn get(&self, key: &[u8], snapshot: bool) -> FdbFuture<Option<FdbFutureSlice>> {
        FdbFuture::new(unsafe {
            fdb_sys::fdb_transaction_get(
                self.inner.as_ptr(),
                key.as_ptr(),
                fdb_len(key.len(), "key"),
                fdb_bool(snapshot),
            )
        })
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
        unsafe {
            fdb_sys::fdb_transaction_atomic_op(
                self.inner.as_ptr(),
                key.as_ptr(),
                fdb_len(key.len(), "key"),
                param.as_ptr(),
                fdb_len(param.len(), "param"),
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
    pub fn get_key(&self, selector: &KeySelector, snapshot: bool) -> FdbFuture<FdbFutureSlice> {
        let key = selector.key();
        FdbFuture::new(unsafe {
            fdb_sys::fdb_transaction_get_key(
                self.inner.as_ptr(),
                key.as_ptr(),
                fdb_len(key.len(), "key"),
                fdb_bool(selector.or_equal()),
                selector.offset(),
                fdb_bool(snapshot),
            )
        })
    }

    ///
    pub fn get_ranges<'a>(
        &'a self,
        opt: RangeOption<'a>,
        snapshot: bool,
    ) -> impl TryStream<Ok = FdbFutureValues, Error = Error> + 'a {
        stream::unfold((1, Some(opt)), move |(iteration, maybe_opt)| {
            if let Some(opt) = maybe_opt {
                Either::Left(self.get_range(&opt, iteration as usize, snapshot).map(
                    move |maybe_values| {
                        let next_opt = match &maybe_values {
                            Ok(values) => opt.next_range(values),
                            Err(..) => None,
                        };
                        Some((maybe_values, (iteration + 1, next_opt)))
                    },
                ))
            } else {
                Either::Right(future::ready(None))
            }
        })
    }

    pub fn get_ranges_keyvalues<'a>(
        &'a self,
        opt: RangeOption<'a>,
        snapshot: bool,
    ) -> impl TryStream<Ok = FdbFutureValue, Error = Error> + 'a {
        self.get_ranges(opt, snapshot)
            .map_ok(|values| stream::iter(values.into_iter().map(Ok)))
            .try_flatten()
    }
    /// Reads all key-value pairs in the database snapshot represented by transaction (potentially
    /// limited by limit, target_bytes, or mode) which have a key lexicographically greater than or
    /// equal to the key resolved by the begin key selector and lexicographically less than the key
    /// resolved by the end key selector.
    pub fn get_range(
        &self,
        opt: &RangeOption,
        iteration: usize,
        snapshot: bool,
    ) -> FdbFuture<FdbFutureValues> {
        let begin = &opt.begin;
        let end = &opt.end;
        let key_begin = begin.key();
        let key_end = end.key();

        FdbFuture::new(unsafe {
            fdb_sys::fdb_transaction_get_range(
                self.inner.as_ptr(),
                key_begin.as_ptr(),
                fdb_len(key_begin.len(), "key_begin"),
                fdb_bool(begin.or_equal()),
                begin.offset(),
                key_end.as_ptr(),
                fdb_len(key_end.len(), "key_end"),
                fdb_bool(end.or_equal()),
                end.offset(),
                fdb_limit(opt.limit.unwrap_or(0)),
                fdb_limit(opt.target_bytes),
                opt.mode.code(),
                fdb_iteration(iteration),
                fdb_bool(snapshot),
                fdb_bool(opt.reverse),
            )
        })
    }
    /// Modify the database snapshot represented by transaction to remove all keys (if any) which
    /// are lexicographically greater than or equal to the given begin key and lexicographically
    /// less than the given end_key.
    ///
    /// The modification affects the actual database only if transaction is later committed with
    /// `Tranasction::commit`.
    pub fn clear_range(&self, begin: &[u8], end: &[u8]) {
        unsafe {
            fdb_sys::fdb_transaction_clear_range(
                self.inner.as_ptr(),
                begin.as_ptr(),
                fdb_len(begin.len(), "begin"),
                end.as_ptr(),
                fdb_len(end.len(), "end"),
            )
        }
    }

    /// Attempts to commit the sets and clears previously applied to the database snapshot represented by transaction to the actual database.
    ///
    /// The commit may or may not succeed – in particular, if a conflicting transaction previously committed, then the commit must fail in order to preserve transactional isolation. If the commit does succeed, the transaction is durably committed to the database and all subsequently started transactions will observe its effects.
    ///
    /// It is not necessary to commit a read-only transaction – you can simply call fdb_transaction_destroy().
    ///
    /// Returns an `TrxCommit` representing an empty value.
    ///
    /// Callers will usually want to retry a transaction if the commit or a prior fdb_transaction_get_*() returns a retryable error (see fdb_transaction_on_error()).
    ///
    /// As with other client/server databases, in some failure scenarios a client may be unable to determine whether a transaction succeeded. In these cases, `Transaction::commit` will return a commit_unknown_result error. The fdb_transaction_on_error() function treats this error as retryable, so retry loops that don’t check for commit_unknown_result could execute the transaction twice. In these cases, you must consider the idempotence of the transaction. For more information, see Transactions with unknown results.
    ///
    /// Normally, commit will wait for outstanding reads to return. However, if those reads were snapshot reads or the transaction option for disabling “read-your-writes” has been invoked, any outstanding reads will immediately return errors.
    pub fn commit(self) -> impl Future<Output = TransactionResult> {
        FdbFuture::<()>::new(unsafe { fdb_sys::fdb_transaction_commit(self.inner.as_ptr()) }).map(
            move |r| match r {
                Ok(()) => Ok(TransactionCommitted { tr: self }),
                Err(err) => Err(TransactionCommitError { tr: self, err }),
            },
        )
    }

    pub fn on_error(self, err: &Error) -> impl Future<Output = Result<Transaction>> {
        FdbFuture::<()>::new(unsafe {
            fdb_sys::fdb_transaction_on_error(self.inner.as_ptr(), err.code())
        })
        .map_ok(|()| self)
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
    pub fn cancel(self) -> TransactionCancelled {
        unsafe { fdb_sys::fdb_transaction_cancel(self.inner.as_ptr()) };
        TransactionCancelled { tr: self }
    }

    /// Returns a list of public network addresses as strings, one for each of the storage servers
    /// responsible for storing key_name and its associated value.
    ///
    /// Returns an FDBFuture which will be set to an array of strings. You must first wait for the
    /// FDBFuture to be ready, check for errors, call fdb_future_get_string_array() to extract the
    /// string array, and then destroy the FDBFuture with fdb_future_destroy().
    pub fn get_addresses_for_key(&self, key: &[u8]) -> FdbFuture<FdbFutureAddresses> {
        FdbFuture::new(unsafe {
            fdb_sys::fdb_transaction_get_addresses_for_key(
                self.inner.as_ptr(),
                key.as_ptr(),
                fdb_len(key.len(), "key"),
            )
        })
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
    pub fn watch(&self, key: &[u8]) -> FdbFuture<()> {
        FdbFuture::new(unsafe {
            fdb_sys::fdb_transaction_watch(
                self.inner.as_ptr(),
                key.as_ptr(),
                fdb_len(key.len(), "key"),
            )
        })
    }

    /// Returns an FDBFuture which will be set to the approximate transaction size so far in the
    /// returned future, which is the summation of the estimated size of mutations, read conflict
    /// ranges, and write conflict ranges. You must first wait for the FDBFuture to be ready,
    /// check for errors, call fdb_future_get_int64() to extract the size, and then destroy the
    /// FDBFuture with fdb_future_destroy().
    ///
    /// This can be called multiple times before the transaction is committed.
    #[cfg(feature = "fdb-6_2")]
    pub fn get_approximate_size(&self) -> FdbFuture<i64> {
        FdbFuture::new(unsafe {
            fdb_sys::fdb_transaction_get_approximate_size(self.inner.as_ptr())
        })
    }

    /// Returns an FDBFuture which will be set to the versionstamp which was used by any
    /// versionstamp operations in this transaction. You must first wait for the FDBFuture to be
    /// ready, check for errors, call fdb_future_get_key() to extract the key, and then destroy the
    /// FDBFuture with fdb_future_destroy().
    ///
    /// The future will be ready only after the successful completion of a call to
    /// fdb_transaction_commit() on this Transaction. Read-only transactions do not modify the
    /// database when committed and will result in the future completing with an error.  Keep in
    /// mind that a transaction which reads keys and then sets them to their current values may be
    /// optimized to a read-only transaction.
    ///
    /// Most applications will not call this function.
    pub fn get_versionstamp(&self) -> FdbFuture<FdbFutureSlice> {
        FdbFuture::new(unsafe { fdb_sys::fdb_transaction_get_versionstamp(self.inner.as_ptr()) })
    }

    /// The transaction obtains a snapshot read version automatically at the time of the first call
    /// to fdb_transaction_get_*() (including this one) and (unless causal consistency has been
    /// deliberately compromised by transaction options) is guaranteed to represent all
    /// transactions which were reported committed before that call.
    pub fn get_read_version(&self) -> FdbFuture<i64> {
        FdbFuture::new(unsafe { fdb_sys::fdb_transaction_get_read_version(self.inner.as_ptr()) })
    }

    /// Sets the snapshot read version used by a transaction.
    ///
    /// This is not needed in simple cases.
    /// If the given version is too old, subsequent reads will fail with error_code_past_version;
    /// if it is too new, subsequent reads may be delayed indefinitely and/or fail with
    /// error_code_future_version. If any of fdb_transaction_get_*() have been called on this
    /// transaction already, the result is undefined.
    pub fn set_read_version(&self, version: i64) {
        unsafe { fdb_sys::fdb_transaction_set_read_version(self.inner.as_ptr(), version) }
    }

    /// Reset transaction to its initial state.
    ///
    /// In order to protect against a race condition with cancel(), this call require a mutable
    /// access to the transaction.
    ///
    /// This is similar to calling fdb_transaction_destroy() followed by
    /// fdb_database_create_transaction(). It is not necessary to call fdb_transaction_reset()
    /// when handling an error with fdb_transaction_on_error() since the transaction has already been reset.
    pub fn reset(&mut self) {
        unsafe { fdb_sys::fdb_transaction_reset(self.inner.as_ptr()) }
    }

    /// Implements the recommended retry and backoff behavior for a transaction. This function
    /// knows which of the error codes generated by other fdb_transaction_*() functions represent
    /// temporary error conditions and which represent application errors that should be handled by
    /// the application. It also implements an exponential backoff strategy to avoid swamping the
    /// database cluster with excessive retries when there is a high level of conflict between
    /// transactions.
    ///
    /// # Warning
    ///
    /// The API is exposed mainly for `bindingtester`, and it is not recommended to call the API
    /// directly from application. Use `Database::transact` instead.
    /*#[doc(hidden)]
    pub fn on_error(&self, error: Error) -> TrxErrFuture {
        TrxErrFuture::new(self.clone(), error)
    }*/

    /// Adds a conflict range to a transaction without performing the associated read or write.
    ///
    /// # Note
    ///
    /// Most applications will use the serializable isolation that transactions provide by default
    /// and will not need to manipulate conflict ranges.
    pub fn add_conflict_range(
        &self,
        begin: &[u8],
        end: &[u8],
        ty: options::ConflictRangeType,
    ) -> Result<()> {
        let err = unsafe {
            fdb_sys::fdb_transaction_add_conflict_range(
                self.inner.as_ptr(),
                begin.as_ptr(),
                fdb_len(begin.len(), "begin"),
                end.as_ptr(),
                fdb_len(end.len(), "end"),
                ty.code(),
            )
        };
        eval(err)
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        unsafe {
            fdb_sys::fdb_transaction_destroy(self.inner.as_ptr());
        }
    }
}
