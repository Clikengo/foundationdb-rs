// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
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
    f: Option<*mut fdb::FDBFuture>,
    task: Option<Box<futures::task::Task>>,
}

impl FdbFuture {
    pub(crate) fn new(f: *mut fdb::FDBFuture) -> Self {
        Self {
            f: Some(f),
            task: None,
        }
    }
}

impl Drop for FdbFuture {
    fn drop(&mut self) {
        if let Some(f) = self.f.take() {
            // `fdb_future_destory` cancels the future, so we don't need to call
            // `fdb_future_cancel` explicitly.
            unsafe { fdb::fdb_future_destroy(f) }
        }
    }
}

impl futures::Future for FdbFuture {
    type Item = FdbFutureResult;
    type Error = FdbError;

    fn poll(&mut self) -> std::result::Result<Async<Self::Item>, Self::Error> {
        let f = self.f.expect("cannot poll after resolve");

        if self.task.is_none() {
            let task = futures::task::current();
            let task = Box::new(task);
            let task_ptr = task.as_ref() as *const _;
            unsafe {
                fdb::fdb_future_set_callback(f, Some(fdb_future_callback), task_ptr as *mut _);
            }
            self.task = Some(task);

            return Ok(Async::NotReady);
        }

        let ready = unsafe { fdb::fdb_future_is_ready(f) };
        if ready == 0 {
            return Ok(Async::NotReady);
        }

        unsafe { error::eval(fdb::fdb_future_get_error(f))? };

        // The result is taking ownership of fdb::FDBFuture
        let g = FdbFutureResult::new(self.f.take().unwrap());
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

/// Represents the output of fdb_future_get_keyvalue_array().
pub struct KeyValues<'a> {
    keyvalues: &'a [KeyValue<'a>],
    more: bool,
}
impl<'a> KeyValues<'a> {
    /// Returns true if (but not necessarily only if) values remain in the key range requested
    /// (possibly beyond the limits requested).
    pub(crate) fn more(&self) -> bool {
        self.more
    }
}
//TODO: maybe `as_slice()` for just `keyvalues()`? `as_ref()` is not intuitive in this case...
impl<'a> AsRef<[KeyValue<'a>]> for KeyValues<'a> {
    fn as_ref(&self) -> &[KeyValue<'a>] {
        self.keyvalues
    }
}

/// Represents a single key-value pair in the output of fdb_future_get_keyvalue_array().
// Uses repr(packed) because c API uses 4-byte alignment for this struct
// TODO: field reordering might change a struct layout...
#[repr(packed)]
pub struct KeyValue<'a> {
    key: *const u8,
    key_len: u32,
    value: *const u8,
    value_len: u32,
    _dummy: std::marker::PhantomData<&'a u8>,
}
impl<'a> KeyValue<'a> {
    /// key
    pub fn key(&'a self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.key, self.key_len as usize) }
    }
    /// value
    pub fn value(&'a self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.value, self.value_len as usize) }
    }
}

/// The Result of an FdbFuture from which query results can be gottent, etc.
pub struct FdbFutureResult {
    f: *mut fdb::FDBFuture,
}

impl Drop for FdbFutureResult {
    fn drop(&mut self) {
        unsafe { fdb::fdb_future_destroy(self.f) }
    }
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

    #[allow(unused)]
    pub(crate) fn get_key<'a>(&'a self) -> Result<&'a [u8]> {
        let mut out_value = std::ptr::null();
        let mut out_len = 0;

        unsafe {
            error::eval(fdb::fdb_future_get_key(
                self.f,
                &mut out_value as *mut _,
                &mut out_len as *mut _,
            ))?
        }

        // A value from `fdb_future_get_value` will alive until `fdb_future_destroy` is called and
        // `fdb_future_destroy` is called on `Self::drop`, so a lifetime of the value matches with
        // `self`
        let slice = unsafe { std::slice::from_raw_parts(out_value, out_len as usize) };
        Ok(slice)
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
        // `fdb_future_destroy` is called on `Self::drop`, so a lifetime of the value matches with
        // `self`
        let slice = unsafe { std::slice::from_raw_parts(out_value, out_len as usize) };
        Ok(Some(slice))
    }

    #[allow(unused)]
    pub(crate) fn get_string_array(&self) -> Result<Vec<&[u8]>> {
        use std::os::raw::c_char;

        let mut out_strings: *mut *const c_char = std::ptr::null_mut();
        let mut out_len = 0;

        unsafe {
            error::eval(fdb::fdb_future_get_string_array(
                self.f,
                &mut out_strings as *mut _,
                &mut out_len as *mut _,
            ))?
        }

        let out_len = out_len as usize;
        let out_strings: &[*const c_char] =
            unsafe { std::slice::from_raw_parts(out_strings, out_len) };

        let mut v = Vec::with_capacity(out_len);
        for i in 0..out_len {
            let cstr = unsafe { std::ffi::CStr::from_ptr(out_strings[i]) };
            v.push(cstr.to_bytes());
        }
        Ok(v)
    }

    #[allow(unused)]
    pub(crate) fn get_keyvalue_array<'a>(&'a self) -> Result<KeyValues<'a>> {
        let mut out_keyvalues = std::ptr::null();
        let mut out_len = 0;
        let mut more = 0;

        unsafe {
            error::eval(fdb::fdb_future_get_keyvalue_array(
                self.f,
                &mut out_keyvalues as *mut _,
                &mut out_len as *mut _,
                &mut more as *mut _,
            ))?
        }

        let out_len = out_len as usize;
        let out_keyvalues: &[fdb::keyvalue] =
            unsafe { std::slice::from_raw_parts(out_keyvalues, out_len) };
        let out_keyvalues: &[KeyValue] = unsafe { std::mem::transmute(out_keyvalues) };
        Ok(KeyValues {
            keyvalues: out_keyvalues,
            more: (more != 0),
        })
    }
}

#[allow(unused)]
fn test_keyvalue_size() {
    unsafe {
        // compile-time test for struct size
        std::mem::transmute::<KeyValue, fdb::keyvalue>(std::mem::uninitialized());
    }
}
