// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Most functions in the FoundationDB API are asynchronous, meaning that they
//! may return to the caller before actually delivering their result.
//!
//! These functions always return FDBFuture*. An FDBFuture object represents a
//! result value or error to be delivered at some future time. You can wait for
//! a Future to be “ready” – to have a value or error delivered – by setting a
//! callback function, or by blocking a thread, or by polling. Once a Future is
//! ready, you can extract either an error code or a value of the appropriate
//! type (the documentation for the original function will tell you which
//! fdb_future_get_*() function you should call).
//!
//! Futures make it easy to do multiple operations in parallel, by calling several
//! asynchronous functions before waiting for any of the results. This can be
//! important for reducing the latency of transactions.
//!

use std;
use std::convert::TryFrom;
use std::ffi::CStr;
use std::ops::Deref;
use std::os::raw::c_char;
use std::pin::Pin;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::Arc;

use foundationdb_sys as fdb_sys;
use futures::prelude::*;
use futures::task::{AtomicWaker, Context, Poll};

use crate::error::{self, Error, Result};

pub struct FdbFutureHandle(NonNull<fdb_sys::FDBFuture>);

impl FdbFutureHandle {
    pub const fn as_ptr(&self) -> *mut fdb_sys::FDBFuture {
        self.0.as_ptr()
    }
}
impl Drop for FdbFutureHandle {
    fn drop(&mut self) {
        // `fdb_future_destroy` cancels the future, so we don't need to call
        // `fdb_future_cancel` explicitly.
        unsafe { fdb_sys::fdb_future_destroy(self.as_ptr()) }
    }
}

/// An opaque type that represents a Future in the FoundationDB C API.
pub struct FdbFuture<T> {
    f: Option<FdbFutureHandle>,
    waker: Option<Arc<AtomicWaker>>,
    phantom: std::marker::PhantomData<T>,
}
unsafe impl<T> Send for FdbFuture<T> {}

impl<T> FdbFuture<T>
where
    T: TryFrom<FdbFutureHandle, Error = Error> + Unpin,
{
    pub(crate) fn new(f: *mut fdb_sys::FDBFuture) -> Self {
        Self {
            f: Some(FdbFutureHandle(
                NonNull::new(f).expect("FDBFuture to not be null"),
            )),
            waker: None,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<T> Future for FdbFuture<T>
where
    T: TryFrom<FdbFutureHandle, Error = Error> + Unpin,
{
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<T>> {
        let f = self.f.as_ref().expect("cannot poll after resolve");
        let ready = unsafe { fdb_sys::fdb_future_is_ready(f.as_ptr()) };
        if ready == 0 {
            let f_ptr = f.as_ptr();
            let mut register = false;
            let waker = self.waker.get_or_insert_with(|| {
                register = true;
                Arc::new(AtomicWaker::new())
            });
            waker.register(cx.waker());
            if register {
                let network_waker: Arc<AtomicWaker> = waker.clone();
                let network_waker_ptr = Arc::into_raw(network_waker);
                unsafe {
                    fdb_sys::fdb_future_set_callback(
                        f_ptr,
                        Some(fdb_future_callback),
                        network_waker_ptr as *mut _,
                    );
                }
            }
            Poll::Pending
        } else {
            Poll::Ready(
                error::eval(unsafe { fdb_sys::fdb_future_get_error(f.as_ptr()) })
                    .and_then(|()| T::try_from(self.f.take().expect("self.f.is_some()"))),
            )
        }
    }
}

// The callback from fdb C API can be called from multiple threads. so this callback should be
// thread-safe.
extern "C" fn fdb_future_callback(
    _f: *mut fdb_sys::FDBFuture,
    callback_parameter: *mut ::std::os::raw::c_void,
) {
    let network_waker: Arc<AtomicWaker> = unsafe { Arc::from_raw(callback_parameter as *const _) };
    network_waker.wake();
}

pub struct FdbFutureSlice {
    _f: FdbFutureHandle,
    value: *const u8,
    len: i32,
}

impl Deref for FdbFutureSlice {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.value, self.len as usize) }
    }
}

impl TryFrom<FdbFutureHandle> for FdbFutureSlice {
    type Error = Error;

    fn try_from(f: FdbFutureHandle) -> Result<Self> {
        let mut value = std::ptr::null();
        let mut len = 0;

        error::eval(unsafe { fdb_sys::fdb_future_get_key(f.as_ptr(), &mut value, &mut len) })?;

        Ok(FdbFutureSlice { _f: f, value, len })
    }
}

impl TryFrom<FdbFutureHandle> for Option<FdbFutureSlice> {
    type Error = Error;

    fn try_from(f: FdbFutureHandle) -> Result<Self> {
        let mut present = 0;
        let mut value = std::ptr::null();
        let mut len = 0;

        error::eval(unsafe {
            fdb_sys::fdb_future_get_value(f.as_ptr(), &mut present, &mut value, &mut len)
        })?;

        Ok(if present == 0 {
            None
        } else {
            Some(FdbFutureSlice { _f: f, value, len })
        })
    }
}

pub struct FdbFutureAddresses {
    _f: FdbFutureHandle,
    strings: *const *const c_char,
    len: i32,
}

impl TryFrom<FdbFutureHandle> for FdbFutureAddresses {
    type Error = Error;

    fn try_from(f: FdbFutureHandle) -> Result<Self> {
        let mut strings: *mut *const c_char = std::ptr::null_mut();
        let mut len = 0;

        error::eval(unsafe {
            fdb_sys::fdb_future_get_string_array(f.as_ptr(), &mut strings, &mut len)
        })?;

        Ok(FdbFutureAddresses {
            _f: f,
            strings,
            len,
        })
    }
}

impl Deref for FdbFutureAddresses {
    type Target = [FdbFutureAddress];

    fn deref(&self) -> &Self::Target {
        assert_eq_size!(FdbFutureAddress, *const c_char);
        assert_eq_align!(FdbFutureAddress, *const c_char);
        unsafe { std::mem::transmute(std::slice::from_raw_parts(self.strings, self.len as usize)) }
    }
}

pub struct FdbFutureAddress {
    c_str: *const c_char,
}

impl Deref for FdbFutureAddress {
    type Target = CStr;

    fn deref(&self) -> &CStr {
        unsafe { std::ffi::CStr::from_ptr(self.c_str) }
    }
}

pub struct FdbFutureValues {
    _f: FdbFutureHandle,
    keyvalues: *const fdb_sys::FDBKeyValue,
    len: i32,
    pub(crate) more: bool,
}

impl TryFrom<FdbFutureHandle> for FdbFutureValues {
    type Error = Error;
    fn try_from(f: FdbFutureHandle) -> Result<Self> {
        let mut keyvalues = std::ptr::null();
        let mut len = 0;
        let mut more = 0;

        unsafe {
            error::eval(fdb_sys::fdb_future_get_keyvalue_array(
                f.as_ptr(),
                &mut keyvalues,
                &mut len,
                &mut more,
            ))?
        }

        Ok(FdbFutureValues {
            _f: f,
            keyvalues,
            len,
            more: more != 0,
        })
    }
}

impl Deref for FdbFutureValues {
    type Target = [KeyValue];
    fn deref(&self) -> &Self::Target {
        assert_eq_size!(KeyValue, fdb_sys::FDBKeyValue);
        assert_eq_align!(KeyValue, fdb_sys::FDBKeyValue);
        unsafe {
            std::mem::transmute(std::slice::from_raw_parts(
                self.keyvalues,
                self.len as usize,
            ))
        }
    }
}

impl<'a> IntoIterator for &'a FdbFutureValues {
    type Item = &'a KeyValue;
    type IntoIter = std::slice::Iter<'a, KeyValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.deref().iter()
    }
}
impl IntoIterator for FdbFutureValues {
    type Item = FdbFutureValue;
    type IntoIter = FdbFutureValuesIter;

    fn into_iter(self) -> Self::IntoIter {
        FdbFutureValuesIter {
            f: Rc::new(self._f),
            keyvalues: self.keyvalues,
            len: self.len,
            pos: 0,
        }
    }
}

pub struct FdbFutureValuesIter {
    f: Rc<FdbFutureHandle>,
    keyvalues: *const fdb_sys::FDBKeyValue,
    len: i32,
    pos: i32,
}
impl Iterator for FdbFutureValuesIter {
    type Item = FdbFutureValue;
    fn next(&mut self) -> Option<Self::Item> {
        self.nth(0)
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let pos = (self.pos as usize).checked_add(n);
        match pos {
            Some(pos) if pos < self.len as usize => {
                // safe because pos < self.len
                let keyvalue = unsafe { self.keyvalues.add(pos) };
                self.pos = pos as i32 + 1;

                Some(FdbFutureValue {
                    _f: self.f.clone(),
                    keyvalue,
                })
            }
            _ => {
                self.pos = self.len;
                None
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = (self.len - self.pos) as usize;
        (rem, Some(rem))
    }
}
impl ExactSizeIterator for FdbFutureValuesIter {}
impl DoubleEndedIterator for FdbFutureValuesIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.nth_back(0)
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        if n < self.pos as usize {
            // safe because n < self.pos
            self.pos -= 1 + n as i32;
            let keyvalue = unsafe { self.keyvalues.add(self.pos as usize) };

            Some(FdbFutureValue {
                _f: self.f.clone(),
                keyvalue,
            })
        } else {
            self.pos = 0;
            None
        }
    }
}

pub struct FdbFutureValue {
    _f: Rc<FdbFutureHandle>,
    keyvalue: *const fdb_sys::FDBKeyValue,
}
impl Deref for FdbFutureValue {
    type Target = KeyValue;
    fn deref(&self) -> &Self::Target {
        assert_eq_size!(KeyValue, fdb_sys::FDBKeyValue);
        assert_eq_align!(KeyValue, fdb_sys::FDBKeyValue);
        unsafe { std::mem::transmute(self.keyvalue) }
    }
}

/// Represents a single key-value pair in the output of fdb_future_get_keyvalue_array().
///
/// Internal info:
///
/// Uses repr(C, packed(4)) because c API uses 4-byte alignment for this struct
///
/// Because the data it represent is owned by the future in FdbFutureValues, you
/// can never own a KeyValue directly, you can only have references to it.
/// This way, you can never obtain a lifetime greater than the lifetime of the
/// slice that gave you access to it.
#[repr(C, packed(4))]
pub struct KeyValue {
    key: *const u8,
    key_len: i32,
    value: *const u8,
    value_len: i32,
}
impl KeyValue {
    /// key
    pub fn key(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.key, self.key_len as usize) }
    }

    /// value
    pub fn value(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.value, self.value_len as usize) }
    }
}

impl TryFrom<FdbFutureHandle> for i64 {
    type Error = Error;

    fn try_from(f: FdbFutureHandle) -> Result<Self> {
        let mut version: i64 = 0;
        error::eval(unsafe { fdb_sys::fdb_future_get_version(f.as_ptr(), &mut version) })?;
        Ok(version)
    }
}

impl TryFrom<FdbFutureHandle> for () {
    type Error = Error;
    fn try_from(_f: FdbFutureHandle) -> Result<Self> {
        Ok(())
    }
}
