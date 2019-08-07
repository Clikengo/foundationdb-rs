// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Implementations of the FDBDatabase C API
//!
//! https://apple.github.io/foundationdb/api-c.html#database

use std;
use std::sync::Arc;

use foundationdb_sys as fdb;
use futures::future::*;
use futures::Future;

use crate::cluster::*;
use crate::error::{self, Error as FdbError, Result};
use crate::options;
use crate::transaction::*;

/// Represents a FoundationDB database — a mutable, lexicographically ordered mapping from binary keys to binary values.
///
/// Modifications to a database are performed via transactions.
#[derive(Clone)]
pub struct Database {
    // Order of fields should not be changed, because Rust drops field top-to-bottom (rfc1857), and
    // database should be dropped before cluster.
    inner: Arc<DatabaseInner>,
    cluster: Cluster,
}
impl Database {
    pub(crate) fn new(cluster: Cluster, db: *mut fdb::FDBDatabase) -> Self {
        let inner = Arc::new(DatabaseInner::new(db));
        Self { cluster, inner }
    }

    /// Called to set an option an on `Database`.
    pub fn set_option(&self, opt: options::DatabaseOption) -> Result<()> {
        unsafe { opt.apply(self.inner.inner) }
    }

    /// Creates a new transaction on the given database.
    pub fn create_trx(&self) -> Result<Transaction> {
        unsafe {
            let mut trx: *mut fdb::FDBTransaction = std::ptr::null_mut();
            error::eval(fdb::fdb_database_create_transaction(
                self.inner.inner,
                &mut trx as *mut _,
            ))?;
            Ok(Transaction::new(self.clone(), trx))
        }
    }

    /// `transact` returns a future which retries on error. It tries to resolve a future created by
    /// caller-provided function `f` inside a retry loop, providing it with a newly created
    /// transaction. After caller-provided future resolves, the transaction will be committed
    /// automatically.
    ///
    /// # Warning
    ///
    /// It might retry indefinitely if the transaction is highly contentious. It is recommended to
    /// set `TransactionOption::RetryLimit` or `TransactionOption::SetTimeout` on the transaction
    /// if the task need to be guaranteed to finish.
    pub fn transact<F, Fut, Item, Error>(
        &self,
        f: F,
    ) -> Box<dyn Future<Item = Fut::Item, Error = Error>>
    where
        F: FnMut(Transaction) -> Fut + 'static,
        Fut: IntoFuture<Item = Item, Error = Error> + 'static,
        Item: 'static,
        Error: From<FdbError> + 'static,
    {
        let db = self.clone();

        let f = result(db.create_trx())
            .map_err(Error::from)
            .and_then(|trx| {
                loop_fn((trx, f), |(trx, mut f)| {
                    let trx0 = trx.clone();
                    f(trx.clone()).into_future().and_then(move |res| {
                        // try to commit the transaction
                        trx0.commit().map(|_| res).then(|res| match res {
                            Ok(v) => {
                                // committed
                                Ok(Loop::Break(v))
                            }
                            Err(e) => {
                                if e.should_retry() {
                                    Ok(Loop::Continue((trx, f)))
                                } else {
                                    Err(Error::from(e))
                                }
                            }
                        })
                    })
                })
            });

        Box::new(f)
    }
}

struct DatabaseInner {
    inner: *mut fdb::FDBDatabase,
}
impl DatabaseInner {
    fn new(inner: *mut fdb::FDBDatabase) -> Self {
        Self { inner }
    }
}
impl Drop for DatabaseInner {
    fn drop(&mut self) {
        unsafe {
            fdb::fdb_database_destroy(self.inner);
        }
    }
}
unsafe impl Send for DatabaseInner {}
unsafe impl Sync for DatabaseInner {}
