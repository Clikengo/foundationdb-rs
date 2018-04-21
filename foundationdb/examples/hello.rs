extern crate foundationdb;
extern crate foundationdb_sys;
extern crate futures;
extern crate tokio_core;

use std::sync::Arc;

use foundationdb::error;
use foundationdb_sys as fdb;

use futures::future::*;
use futures::Async;

use error::FdbError;

type Result<T> = std::result::Result<T, FdbError>;

#[derive(Clone)]
struct FdbCluster {
    inner: Arc<FdbClusterInner>,
}
impl FdbCluster {
    fn new(path: &str) -> FdbClusterGet {
        let path_str = std::ffi::CString::new(path).unwrap();
        let f = unsafe { fdb::fdb_create_cluster(path_str.as_ptr()) };
        FdbClusterGet {
            inner: FdbFuture::new(f),
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

struct FdbClusterGet {
    inner: FdbFuture,
}
impl Future for FdbClusterGet {
    type Item = FdbCluster;
    type Error = FdbError;

    fn poll(&mut self) -> std::result::Result<Async<Self::Item>, Self::Error> {
        match self.inner.poll() {
            Ok(Async::Ready(r)) => match unsafe { r.get_cluster() } {
                Ok(c) => Ok(Async::Ready(FdbCluster {
                    inner: Arc::new(FdbClusterInner::new(c)),
                })),
                Err(e) => Err(e),
            },
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(e),
        }
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

#[derive(Clone)]
struct FdbTransaction {
    database: FdbDatabase,
    inner: Arc<FdbTransactionInner>,
}
impl FdbTransaction {
    pub fn database(&self) -> FdbDatabase {
        self.database.clone()
    }

    fn set(&self, key: &[u8], value: &[u8]) {
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

    fn clear(&self, key: &[u8]) {
        let trx = self.inner.inner;
        unsafe { fdb::fdb_transaction_clear(trx, key.as_ptr(), key.len() as i32) }
    }

    fn get(&self, key: &[u8]) -> FdbTrxGet {
        let trx = self.inner.inner;

        let f =
            unsafe { fdb::fdb_transaction_get(trx, key.as_ptr() as *const _, key.len() as i32, 0) };
        let f = FdbFuture::new(f);
        FdbTrxGet {
            trx: self.clone(),
            inner: f,
        }
    }

    fn commit(self) -> FdbTrxCommit {
        let trx = self.inner.inner;

        let f = unsafe { fdb::fdb_transaction_commit(trx) };
        let f = FdbFuture::new(f);
        FdbTrxCommit {
            trx: self,
            inner: f,
        }
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

struct FdbGetResult {
    trx: FdbTransaction,
    inner: FdbFutureResult,
}
impl FdbGetResult {
    pub fn transaction(&self) -> FdbTransaction {
        self.trx.clone()
    }
    pub fn value(&self) -> Result<Option<&[u8]>> {
        self.inner.get_value()
    }
}

struct FdbTrxGet {
    trx: FdbTransaction,
    inner: FdbFuture,
}
impl Future for FdbTrxGet {
    type Item = FdbGetResult;
    type Error = FdbError;

    fn poll(&mut self) -> std::result::Result<Async<Self::Item>, Self::Error> {
        match self.inner.poll() {
            Ok(Async::Ready(r)) => Ok(Async::Ready(FdbGetResult {
                trx: self.trx.clone(),
                inner: r,
            })),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(e),
        }
    }
}

struct FdbTrxCommit {
    trx: FdbTransaction,
    inner: FdbFuture,
}
impl Future for FdbTrxCommit {
    type Item = FdbDatabase;
    type Error = FdbError;

    fn poll(&mut self) -> std::result::Result<Async<Self::Item>, Self::Error> {
        match self.inner.poll() {
            Ok(Async::Ready(_r)) => Ok(Async::Ready(self.trx.database.clone())),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(e),
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

    fn get_value<'a>(&'a self) -> Result<Option<&'a [u8]>> {
        let mut present = 0;
        let mut out_value = std::ptr::null();
        let mut out_len = 0;
        let err = unsafe {
            fdb::fdb_future_get_value(
                self.f,
                &mut present as *mut _,
                &mut out_value as *mut _,
                &mut out_len as *mut _,
            )
        };
        if err != 0 {
            return Err(FdbError::from(err));
        }
        if present == 0 {
            return Ok(None);
        }
        let slice = unsafe { std::slice::from_raw_parts(out_value, out_len as usize) };
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

    fn poll(&mut self) -> std::result::Result<Async<Self::Item>, Self::Error> {
        if self.task.is_none() {
            let task = futures::task::current();
            let task = Box::new(task);
            let task_ptr = task.as_ref() as *const _;
            unsafe {
                fdb::fdb_future_set_callback(self.f, Some(fdb_future_callback), task_ptr as *mut _);
            }
            self.task = Some(task);

            return Ok(Async::NotReady);
        }

        let ready = unsafe { fdb::fdb_future_is_ready(self.f) };
        if ready == 0 {
            return Ok(Async::NotReady);
        }

        let err = unsafe { fdb::fdb_future_get_error(self.f) };
        if err != 0 {
            return Err(FdbError::from(err));
        }

        let g = FdbFutureResult::new(self.f);
        self.f = std::ptr::null_mut();

        Ok(Async::Ready(g))
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

//TODO: impl Future
fn example_set_get() -> Box<Future<Item = (), Error = FdbError>> {
    let fut = FdbCluster::new("/etc/foundationdb/fdb.cluster")
        .and_then(|cluster| cluster.create_database())
        .and_then(|db| result(db.create_trx()))
        .and_then(|trx| {
            trx.set(b"hello", b"world");
            trx.commit()
        })
        .and_then(|db| result(db.create_trx()))
        .and_then(|trx| trx.get(b"hello"))
        .and_then(|res| {
            let val = res.value();
            eprintln!("value: {:?}", val);

            let trx = res.transaction();
            trx.clear(b"hello");
            trx.commit()
        })
        .and_then(|db| result(db.create_trx()))
        .and_then(|trx| trx.get(b"hello"))
        .and_then(|res| {
            eprintln!("value: {:?}", res.value());
            Ok(())
        });

    Box::new(fut)
}

fn example_get_multi() -> Box<Future<Item = (), Error = FdbError>> {
    let fut = FdbCluster::new("/etc/foundationdb/fdb.cluster")
        .and_then(|cluster| cluster.create_database())
        .and_then(|db| result(db.create_trx()))
        .and_then(|trx| {
            let keys: &[&[u8]] = &[b"hello", b"world", b"foo", b"bar"];

            let futs = keys.iter().map(|k| trx.get(k)).collect::<Vec<_>>();
            join_all(futs)
        })
        .and_then(|results| {
            for (i, res) in results.into_iter().enumerate() {
                eprintln!("res[{}]: {:?}", i, res.value());
            }
            Ok(())
        });

    Box::new(fut)
}

fn main() {
    let mut core = tokio_core::reactor::Core::new().unwrap();

    let handle = unsafe {
        let version = fdb::fdb_get_max_api_version();
        let err = fdb::fdb_select_api_version_impl(version, version);
        if err != 0 {
            panic!("fdb_select_api_version: {:?}", FdbError::from(err));
        }

        let err = fdb::fdb_setup_network();
        if err != 0 {
            panic!("fdb_setup_network: {:?}", FdbError::from(err));
        }

        std::thread::spawn(|| {
            let err = fdb::fdb_run_network();
            if err != 0 {
                panic!("fdb_run_network: {:?}", FdbError::from(err));
            }
        })
    };

    core.run(example_set_get()).expect("failed to run");
    core.run(example_get_multi()).expect("failed to run");

    unsafe {
        fdb::fdb_stop_network();
    }
    handle.join().expect("failed to join fdb thread");
}
