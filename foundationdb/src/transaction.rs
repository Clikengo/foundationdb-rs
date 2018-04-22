use foundationdb_sys as fdb;
use futures::{Async, Future};
use std;
use std::sync::Arc;

use database::*;
use error::*;
use future::*;
use options;

#[derive(Clone)]
pub struct Transaction {
    database: Database,
    inner: Arc<TransactionInner>,
}
impl Transaction {
    pub(crate) fn new(database: Database, trx: *mut fdb::FDBTransaction) -> Self {
        let inner = Arc::new(TransactionInner::new(trx));
        Self { database, inner }
    }

    pub fn set_option(&self, opt: options::TransactionOption) -> Result<()> {
        unsafe { opt.apply(self.inner.inner) }
    }

    pub fn database(&self) -> Database {
        self.database.clone()
    }

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

    pub fn clear(&self, key: &[u8]) {
        let trx = self.inner.inner;
        unsafe { fdb::fdb_transaction_clear(trx, key.as_ptr(), key.len() as i32) }
    }

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

pub struct GetResult {
    trx: Transaction,
    inner: FdbFutureResult,
}
impl GetResult {
    pub fn transaction(&self) -> Transaction {
        self.trx.clone()
    }
    pub fn value(&self) -> Result<Option<&[u8]>> {
        self.inner.get_value()
    }
}

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
