extern crate foundationdb;
extern crate foundationdb_sys;
extern crate futures;
extern crate tokio_core;

use std::sync::Arc;

use foundationdb::error;
use foundationdb_sys as fdb;

use futures::future::*;

use error::FdbError;

type Result<T> = std::result::Result<T, FdbError>;

#[derive(Clone)]
struct FdbCluster {
    inner: Arc<FdbClusterInner>,
}
impl FdbCluster {
    fn new(inner: *mut fdb::FDBCluster) -> Self {
        Self {
            inner: Arc::new(FdbClusterInner::new(inner)),
        }
    }

    //TODO: impl Future
    fn create_database(&self) -> Box<Future<Item = FdbDatabase, Error = FdbError>> {
        let f = unsafe {
            let f_db = fdb::fdb_cluster_create_database(self.inner.inner, b"DB" as *const _, 2);
            let cluster = self.clone();
            FdbFuture::new(f_db)
                .and_then(|f| f.get_database())
                .map(|db| FdbDatabase {
                    cluster,
                    inner: Arc::new(FdbDatabaseInner::new(db)),
                })
        };
        Box::new(f)
    }
}

struct FdbClusterInner {
    inner: *mut fdb::FDBCluster,
}
impl FdbClusterInner {
    fn new(inner: *mut fdb::FDBCluster) -> Self {
        Self { inner }
    }
}
impl Drop for FdbClusterInner {
    fn drop(&mut self) {
        unsafe {
            fdb::fdb_cluster_destroy(self.inner);
        }
    }
}

#[derive(Clone)]
struct FdbDatabase {
    cluster: FdbCluster,
    inner: Arc<FdbDatabaseInner>,
}
impl FdbDatabase {
    fn create_trx(&self) -> Result<FdbTransaction> {
        unsafe {
            let mut trx: *mut fdb::FDBTransaction = std::ptr::null_mut();
            let err = fdb::fdb_database_create_transaction(self.inner.inner, &mut trx as *mut _);
            if err != 0 {
                return Err(FdbError::from(err));
            }
            Ok(FdbTransaction {
                database: self.clone(),
                inner: Arc::new(FdbTransactionInner::new(trx)),
            })
        }
    }
}

struct FdbDatabaseInner {
    inner: *mut fdb::FDBDatabase,
}
impl FdbDatabaseInner {
    fn new(inner: *mut fdb::FDBDatabase) -> Self {
        Self { inner }
    }
}
impl Drop for FdbDatabaseInner {
    fn drop(&mut self) {
        unsafe {
            fdb::fdb_database_destroy(self.inner);
        }
    }
}

struct FdbTransaction {
    database: FdbDatabase,
    inner: Arc<FdbTransactionInner>,
}
impl FdbTransaction {
    fn database(&self) -> FdbDatabase {
        self.database.clone()
    }
}

struct FdbTransactionInner {
    inner: *mut fdb::FDBTransaction,
}
impl FdbTransactionInner {
    fn new(inner: *mut fdb::FDBTransaction) -> Self {
        Self { inner }
    }
}
impl Drop for FdbTransactionInner {
    fn drop(&mut self) {
        unsafe {
            fdb::fdb_transaction_destroy(self.inner);
        }
    }
}

struct FdbFutureResult {
    f: *mut fdb::FDBFuture,
}
impl FdbFutureResult {
    fn new(f: *mut fdb::FDBFuture) -> Self {
        Self { f }
    }

    unsafe fn get_cluster(&self) -> Result<*mut fdb::FDBCluster> {
        let mut v: *mut fdb::FDBCluster = std::ptr::null_mut();
        let err = fdb::fdb_future_get_cluster(self.f, &mut v as *mut _);
        if err != 0 {
            return Err(FdbError::from(err));
        }
        Ok(v)
    }

    unsafe fn get_database(&self) -> Result<*mut fdb::FDBDatabase> {
        let mut v: *mut fdb::FDBDatabase = std::ptr::null_mut();
        let err = fdb::fdb_future_get_database(self.f, &mut v as *mut _);
        if err != 0 {
            return Err(FdbError::from(err));
        }
        Ok(v)
    }

    unsafe fn get_value<'a>(&'a self) -> Result<Option<&'a [u8]>> {
        let mut present = 0;
        let mut out_value = std::ptr::null();
        let mut out_len = 0;
        let err = fdb::fdb_future_get_value(
            self.f,
            &mut present as *mut _,
            &mut out_value as *mut _,
            &mut out_len as *mut _,
        );
        if err != 0 {
            return Err(FdbError::from(err));
        }
        if present == 0 {
            return Ok(None);
        }
        let slice = std::slice::from_raw_parts(out_value, out_len as usize);
        Ok(Some(slice))
    }
}

impl Drop for FdbFutureResult {
    fn drop(&mut self) {
        unsafe { fdb::fdb_future_destroy(self.f) }
    }
}

struct FdbFuture {
    //
    f: *mut fdb::FDBFuture,
    task: Option<Box<futures::task::Task>>,
}

impl FdbFuture {
    fn new(f: *mut fdb::FDBFuture) -> Self {
        Self { f, task: None }
    }
}

impl futures::Future for FdbFuture {
    type Item = FdbFutureResult;
    type Error = FdbError;

    fn poll(&mut self) -> std::result::Result<futures::Async<Self::Item>, Self::Error> {
        if self.task.is_none() {
            let task = futures::task::current();
            let task = Box::new(task);
            let task_ptr = task.as_ref() as *const _;
            unsafe {
                fdb::fdb_future_set_callback(self.f, Some(fdb_future_callback), task_ptr as *mut _);
            }
            self.task = Some(task);

            return Ok(futures::Async::NotReady);
        }

        let ready = unsafe { fdb::fdb_future_is_ready(self.f) };
        if ready == 0 {
            return Ok(futures::Async::NotReady);
        }

        let err = unsafe { fdb::fdb_future_get_error(self.f) };
        if err != 0 {
            return Err(FdbError::from(err));
        }

        let g = FdbFutureResult::new(self.f);
        self.f = std::ptr::null_mut();

        Ok(futures::Async::Ready(g))
    }
}

extern "C" fn fdb_future_callback(
    _f: *mut fdb::FDBFuture,
    callback_parameter: *mut ::std::os::raw::c_void,
) {
    let task: *const futures::task::Task = callback_parameter as *const _;
    let task: &futures::task::Task = unsafe { std::mem::transmute(task) };
    task.notify();
}

fn main() {
    let mut core = tokio_core::reactor::Core::new().unwrap();

    unsafe {
        let version = fdb::fdb_get_max_api_version();
        let err = fdb::fdb_select_api_version_impl(version, version);
        if err != 0 {
            panic!("fdb_select_api_version: {:?}", FdbError::from(err));
        }

        let err = fdb::fdb_setup_network();
        if err != 0 {
            panic!("fdb_setup_network: {:?}", FdbError::from(err));
        }

        let handle = std::thread::spawn(|| {
            let err = fdb::fdb_run_network();
            if err != 0 {
                panic!("fdb_run_network: {:?}", FdbError::from(err));
            }
        });

        let path_str = std::ffi::CString::new("/etc/foundationdb/fdb.cluster").unwrap();
        let f = fdb::fdb_create_cluster(path_str.as_ptr());
        let fut = FdbFuture::new(f)
            .and_then(|f| result(f.get_cluster()).map(|cluter| FdbCluster::new(cluter)))
            .and_then(|cluster| cluster.create_database())
            .and_then(|db| result(db.create_trx()))
            .and_then(|trx| {
                let db = trx.database();
                let inner = trx.inner.inner;
                fdb::fdb_transaction_set(inner, b"hello" as *const _, 5, b"world" as *const _, 5);

                let f = fdb::fdb_transaction_get(inner, b"hello" as *const _, 5, 0);
                FdbFuture::new(f).map(move |f| (db, f))
            })
            .and_then(|(db, f)| {
                let val = f.get_value();
                eprintln!("value: {:?}", val);

                result(db.create_trx())
            })
            .and_then(|trx| {
                let db = trx.database();
                let inner = trx.inner.inner;

                fdb::fdb_transaction_clear(inner, b"hello" as *const _, 5);

                let f = fdb::fdb_transaction_get(inner, b"hello" as *const _, 5, 0);
                FdbFuture::new(f).map(move |f| (db, f))
            })
            .and_then(|(_db, f)| {
                let val = f.get_value();
                eprintln!("value: {:?}", val);
                fdb::fdb_stop_network();
                Ok(())
            });

        core.run(fut).expect("failed to run");

        handle.join().expect("failed to join fdb thread");
    }
}
