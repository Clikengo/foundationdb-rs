use foundationdb_sys as fdb;
use std;
use std::sync::Arc;

use cluster::*;
use error::*;
use transaction::*;

#[derive(Clone)]
pub struct Database {
    cluster: Cluster,
    inner: Arc<DatabaseInner>,
}
impl Database {
    pub(crate) fn new(cluster: Cluster, db: *mut fdb::FDBDatabase) -> Self {
        let inner = Arc::new(DatabaseInner::new(db));
        Self { cluster, inner }
    }
    pub fn create_trx(&self) -> Result<Transaction> {
        unsafe {
            let mut trx: *mut fdb::FDBTransaction = std::ptr::null_mut();
            let err = fdb::fdb_database_create_transaction(self.inner.inner, &mut trx as *mut _);
            if err != 0 {
                return Err(FdbError::from(err));
            }
            Ok(Transaction::new(self.clone(), trx))
        }
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
