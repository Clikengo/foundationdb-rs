use std;

use foundationdb_sys as fdb;
use futures;
use futures::Async;

use error::{FdbError, Result};

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

        let err = unsafe { fdb::fdb_future_get_error(self.f) };
        if err != 0 {
            return Err(FdbError::from(err));
        }

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

pub struct FdbFutureResult {
    f: *mut fdb::FDBFuture,
}
impl FdbFutureResult {
    pub(crate) fn new(f: *mut fdb::FDBFuture) -> Self {
        Self { f }
    }

    pub(crate) unsafe fn get_cluster(&self) -> Result<*mut fdb::FDBCluster> {
        let mut v: *mut fdb::FDBCluster = std::ptr::null_mut();
        let err = fdb::fdb_future_get_cluster(self.f, &mut v as *mut _);
        if err != 0 {
            return Err(FdbError::from(err));
        }
        Ok(v)
    }

    pub(crate) unsafe fn get_database(&self) -> Result<*mut fdb::FDBDatabase> {
        let mut v: *mut fdb::FDBDatabase = std::ptr::null_mut();
        let err = fdb::fdb_future_get_database(self.f, &mut v as *mut _);
        if err != 0 {
            return Err(FdbError::from(err));
        }
        Ok(v)
    }

    pub(crate) fn get_value<'a>(&'a self) -> Result<Option<&'a [u8]>> {
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
