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

use failure::Fail;
use foundationdb_sys as fdb;
use futures::Future;

use crate::error::{self, Error as FdbError, Result};
use crate::options;
use crate::transaction::*;

/// Represents a FoundationDB database — a mutable, lexicographically ordered mapping from binary keys to binary values.
///
/// Modifications to a database are performed via transactions.
#[derive(Clone)]
pub struct Database {
    inner: Arc<DatabaseInner>,
}
impl Database {
    /// open a new database connection to the cluster
    pub fn new(path: &str) -> Result<Self> {
        let path_str = std::ffi::CString::new(path).unwrap();

        unsafe {
            let mut db: *mut fdb::FDBDatabase = std::ptr::null_mut();
            let err = fdb::fdb_create_database(path_str.as_ptr(), &mut db);
            error::eval(err)?;
            Ok(Database {
                inner: Arc::new(DatabaseInner { inner: db }),
            })
        }
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
    pub async fn transact<F, Fut, Output>(
        &self,
        func: F,
    ) -> std::result::Result<Output, failure::Error>
    where
        F: Fn(Transaction) -> Fut,
        Fut: Future<Output = std::result::Result<Output, failure::Error>>,
    {
        let trx = self.create_trx().unwrap();

        loop {
            let res = func(trx.clone()).await;

            // did the closure return an error?
            if let Err(e) = res {
                let res = e.downcast::<FdbError>();
                if let Ok(e) = res {
                    if e.should_retry() {
                        //debug!("retrying error in transaction body: {}", e);
                        continue;
                    } else {
                        return Err(e.context("non-retryable error in transaction body").into());
                    }
                }

                // non-fdb error, abort
                trx.cancel();
                return Err(res.unwrap_err());
            } else {
                // commit
                match trx.clone().commit().await {
                    // and return the value from the closure on success
                    Ok(_) => return res,
                    Err(e) => {
                        if e.should_retry() {
                            //debug!("retrying error in transaction commit: {}", e);
                        } else {
                            return Err(e
                                .context("non-retryable error in transaction commit")
                                .into());
                        }
                    }
                }
            }
        }
    }
}

struct DatabaseInner {
    inner: *mut fdb::FDBDatabase,
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
