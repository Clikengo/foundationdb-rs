// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Most functions in the FoundationDB API are asynchronous, meaning that they may return to the caller before actually delivering their result.
//!
//! These functions always return FDBFuture*. An FDBFuture object represents a result value or error to be delivered at some future time. You can wait for a Future to be “ready” – to have a value or error delivered – by setting a callback function, or by blocking a thread, or by polling. Once a Future is ready, you can extract either an error code or a value of the appropriate type (the documentation for the original function will tell you which fdb_future_get_*() function you should call).
//!
//! Futures make it easy to do multiple operations in parallel, by calling several asynchronous functions before waiting for any of the results. This can be important for reducing the latency of transactions.
//!
//! The Rust API Client has been implemented to use the Rust futures crate, and should work within that ecosystem (suchas Tokio). See Rust [futures](https://docs.rs/crate/futures/0.1.21) documentation.

use std;

use foundationdb_sys as fdb;
use futures;
use futures::Async;

use error::{self, FdbError, Result};

/// An opaque type that represents a Future in the FoundationDB C API.
pub struct FdbFuture {
    //
    f: *mut fdb::FDBFuture,
    task: Option<Box<futures::task::Task>>,
}

impl FdbFuture {
    pub(crate) fn new(f: *mut fdb::FDBFuture) -> Self {
        Self { f, task: None }
    }
}

impl futures::Future for FdbFuture {
    type Item = FdbFutureResult;
    type Error = FdbError;

    fn poll(&mut self) -> std::result::Result<Async<Self::Item>, Self::Error> {
        if self.f == std::ptr::null_mut() {
            panic!("cannot poll after resolve")
        }

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

        unsafe { error::eval(fdb::fdb_future_get_error(self.f))? };

        // The result is taking ownership of fdb::FDBFuture
        let g = FdbFutureResult::new(self.f);
        self.f = std::ptr::null_mut();

        Ok(Async::Ready(g))
    }
}

// The callback from fdb C API can be called from multiple threads. so this callback should be
// thread-safe.
extern "C" fn fdb_future_callback(
    _f: *mut fdb::FDBFuture,
    callback_parameter: *mut ::std::os::raw::c_void,
) {
    let task: *const futures::task::Task = callback_parameter as *const _;
    let task: &futures::task::Task = unsafe { std::mem::transmute(task) };
    task.notify();
}

/// The Result of an FdbFuture from which query results can be gottent, etc.
pub struct FdbFutureResult {
    f: *mut fdb::FDBFuture,
}
impl FdbFutureResult {
    pub(crate) fn new(f: *mut fdb::FDBFuture) -> Self {
        Self { f }
    }

    pub(crate) unsafe fn get_cluster(&self) -> Result<*mut fdb::FDBCluster> {
        let mut v: *mut fdb::FDBCluster = std::ptr::null_mut();
        error::eval(fdb::fdb_future_get_cluster(self.f, &mut v as *mut _))?;
        Ok(v)
    }

    pub(crate) unsafe fn get_database(&self) -> Result<*mut fdb::FDBDatabase> {
        let mut v: *mut fdb::FDBDatabase = std::ptr::null_mut();
        error::eval(fdb::fdb_future_get_database(self.f, &mut v as *mut _))?;
        Ok(v)
    }

    pub(crate) fn get_value<'a>(&'a self) -> Result<Option<&'a [u8]>> {
        let mut present = 0;
        let mut out_value = std::ptr::null();
        let mut out_len = 0;

        unsafe {
            error::eval(fdb::fdb_future_get_value(
                self.f,
                &mut present as *mut _,
                &mut out_value as *mut _,
                &mut out_len as *mut _,
            ))?
        }

        if present == 0 {
            return Ok(None);
        }

        // A value from `fdb_future_get_value` will alive until `fdb_future_destroy` is called and
        // `fdb_future_destory` is called on `Self::drop`, so a lifetime of the value matches with
        // `self`
        let slice = unsafe { std::slice::from_raw_parts(out_value, out_len as usize) };
        Ok(Some(slice))
    }
}

impl Drop for FdbFutureResult {
    fn drop(&mut self) {
        unsafe { fdb::fdb_future_destroy(self.f) }
    }
}
