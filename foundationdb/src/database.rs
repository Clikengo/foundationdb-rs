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

use std::convert::TryInto;
use std::pin::Pin;
use std::ptr::NonNull;
use std::time::{Duration, Instant};

use foundationdb_sys as fdb_sys;

use crate::error::{self, Error as FdbError, Result};
use crate::options;
use crate::transaction::*;

use futures::prelude::*;

/// Represents a FoundationDB database — a mutable, lexicographically ordered mapping from binary keys to binary values.
///
/// Modifications to a database are performed via transactions.
///
pub struct Database {
    pub(crate) inner: NonNull<fdb_sys::FDBDatabase>,
}
unsafe impl Send for Database {}
unsafe impl Sync for Database {}
impl Drop for Database {
    fn drop(&mut self) {
        unsafe {
            fdb_sys::fdb_database_destroy(self.inner.as_ptr());
        }
    }
}

#[cfg(not(any(feature = "fdb-5_1", feature = "fdb-5_2", feature = "fdb-6_0")))]
impl Database {
    pub fn new(path: Option<&str>) -> Result<Database> {
        let path_str = path.map(|path| std::ffi::CString::new(path).unwrap());
        let path_ptr = path_str
            .map(|path| path.as_ptr())
            .unwrap_or(std::ptr::null());
        let mut v: *mut fdb_sys::FDBDatabase = std::ptr::null_mut();
        let err = unsafe { fdb_sys::fdb_create_database(path_ptr, &mut v) };
        error::eval(err)?;
        Ok(Database {
            inner: NonNull::new(v)
                .expect("fdb_create_database to not return null if there is no error"),
        })
    }

    pub fn from_path(path: &str) -> Result<Database> {
        Self::new(Some(path))
    }

    pub fn default() -> Result<Database> {
        Self::new(None)
    }
}

impl Database {
    pub async fn new_compat(path: Option<&str>) -> Result<Database> {
        #[cfg(any(feature = "fdb-5_1", feature = "fdb-5_2", feature = "fdb-6_0"))]
        {
            let cluster = crate::cluster::Cluster::new(path).await?;
            let database = cluster.create_database().await?;
            Ok(database)
        }

        #[cfg(not(any(feature = "fdb-5_1", feature = "fdb-5_2", feature = "fdb-6_0")))]
        {
            Database::new(path)
        }
    }

    /// Called to set an option an on `Database`.
    pub fn set_option(&self, opt: options::DatabaseOption) -> Result<()> {
        unsafe { opt.apply(self.inner.as_ptr()) }
    }

    /// Creates a new transaction on the given database.
    pub fn create_trx(&self) -> Result<Transaction> {
        let mut trx: *mut fdb_sys::FDBTransaction = std::ptr::null_mut();
        let err =
            unsafe { fdb_sys::fdb_database_create_transaction(self.inner.as_ptr(), &mut trx) };
        error::eval(err)?;
        Ok(Transaction::new(NonNull::new(trx).expect(
            "fdb_database_create_transaction to not return null if there is no error",
        )))
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
    ///
    /// Once [Generic Associated Types](https://github.com/rust-lang/rfcs/blob/master/text/1598-generic_associated_types.md)
    /// lands in stable rust, the returned future of f won't need to be boxed anymore,
    /// also the lifetime limitations around f might be lowered
    pub async fn transact<F, D, Item, Error>(
        &self,
        data: D,
        mut f: F,
        options: TransactOption,
    ) -> std::result::Result<Item, Error>
    where
        for<'a> F: FnMut(
            &'a Transaction,
            &'a D,
        )
            -> Pin<Box<dyn Future<Output = std::result::Result<Item, Error>> + 'a>>,
        Error: TransactError,
    {
        let db = self.clone();

        let is_idempotent = options.is_idempotent;
        let time_out = options.time_out.map(|d| Instant::now() + d);
        let retry_limit = options.retry_limit;
        let mut tries: u32 = 0;
        let mut trx = db.create_trx()?;
        let mut can_retry = move || {
            tries += 1;
            retry_limit.filter(|&limit| tries < limit).is_none()
                && time_out.filter(|&t| Instant::now() < t).is_none()
        };
        loop {
            trx = match f(&trx, &data).await {
                Ok(item) => match trx.commit().await {
                    Ok(_) => break Ok(item),
                    Err(e) => {
                        if (is_idempotent || !e.is_maybe_committed()) && can_retry() {
                            e.on_error().await?
                        } else {
                            break Err(Error::from(e.into()));
                        }
                    }
                },
                Err(user_err) => match user_err.try_into_fdb_error() {
                    Ok(e) => {
                        if (is_idempotent || !e.is_maybe_committed()) && can_retry() {
                            trx.on_error(&e).await?
                        } else {
                            break Err(Error::from(e));
                        }
                    }
                    Err(user_err) => break Err(user_err),
                },
            };
        }
    }
}

pub trait TransactError: From<FdbError> {
    fn try_into_fdb_error(self) -> std::result::Result<FdbError, Self>;
}
impl<T> TransactError for T
where
    T: From<FdbError> + TryInto<FdbError, Error = T>,
{
    fn try_into_fdb_error(self) -> std::result::Result<FdbError, Self> {
        self.try_into()
    }
}
impl TransactError for FdbError {
    fn try_into_fdb_error(self) -> std::result::Result<FdbError, Self> {
        Ok(self)
    }
}

#[derive(Default, Clone)]
pub struct TransactOption {
    retry_limit: Option<u32>,
    time_out: Option<Duration>,
    is_idempotent: bool,
}
