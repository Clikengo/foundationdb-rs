// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use foundationdb_sys as fdb;
use future::*;
use futures::{Async, Future};
use std;
use std::sync::Arc;

use database::*;
use error::*;

#[derive(Clone)]
pub struct Cluster {
    inner: Arc<ClusterInner>,
}
impl Cluster {
    pub fn new(path: &str) -> ClusterGet {
        let path_str = std::ffi::CString::new(path).unwrap();
        let f = unsafe { fdb::fdb_create_cluster(path_str.as_ptr()) };
        ClusterGet {
            inner: FdbFuture::new(f),
        }
    }

    //TODO: impl Future
    pub fn create_database(&self) -> Box<Future<Item = Database, Error = FdbError>> {
        let f = unsafe {
            let f_db = fdb::fdb_cluster_create_database(self.inner.inner, b"DB" as *const _, 2);
            let cluster = self.clone();
            FdbFuture::new(f_db)
                .and_then(|f| f.get_database())
                .map(|db| Database::new(cluster, db))
        };
        Box::new(f)
    }
}

pub struct ClusterGet {
    inner: FdbFuture,
}
impl Future for ClusterGet {
    type Item = Cluster;
    type Error = FdbError;

    fn poll(&mut self) -> Result<Async<Self::Item>> {
        match self.inner.poll() {
            Ok(Async::Ready(r)) => match unsafe { r.get_cluster() } {
                Ok(c) => Ok(Async::Ready(Cluster {
                    inner: Arc::new(ClusterInner::new(c)),
                })),
                Err(e) => Err(e),
            },
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(e),
        }
    }
}

//TODO: should check if `fdb::FDBCluster` is thread-safe.
struct ClusterInner {
    inner: *mut fdb::FDBCluster,
}
impl ClusterInner {
    fn new(inner: *mut fdb::FDBCluster) -> Self {
        Self { inner }
    }
}
impl Drop for ClusterInner {
    fn drop(&mut self) {
        unsafe {
            fdb::fdb_cluster_destroy(self.inner);
        }
    }
}
