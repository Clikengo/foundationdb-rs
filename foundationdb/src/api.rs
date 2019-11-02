// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Implementations of the Fdb API versioning C API
//!
//! https://apple.github.io/foundationdb/api-c.html#api-versioning

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use crate::error::{self, Result};
use crate::options::NetworkOption;
use foundationdb_sys as fdb_sys;

/// Returns the max api version of the underlying Fdb C API Client
pub fn get_max_api_version() -> i32 {
    unsafe { fdb_sys::fdb_get_max_api_version() }
}

static VERSION_SELECTED: AtomicBool = AtomicBool::new(false);

/// A type which represents the successful innitialization of an Fdb API Version
pub struct FdbApi {
    _private: (),
}
/// A Builder with which different versions of the Fdb C API can be initialized
pub struct FdbApiBuilder {
    runtime_version: i32,
}

impl FdbApiBuilder {
    /// The version of run-time behavior the API is requested to provide.
    ///
    /// Must be less than or equal to header_version, `foundationdb_sys::FDB_API_VERSION`, and should almost always be equal.
    /// Language bindings which themselves expose API versioning will usually pass the version requested by the application.
    pub fn set_runtime_version(mut self, version: i32) -> Self {
        self.runtime_version = version;
        self
    }

    /// The API version can only be initialized once in the lifetime of a process
    pub fn build(self) -> Result<NetworkBuilder> {
        if VERSION_SELECTED.compare_and_swap(false, true, Ordering::AcqRel) {
            panic!("the fdb select api version can only be run once per process");
        }

        error::eval(unsafe {
            fdb_sys::fdb_select_api_version_impl(
                self.runtime_version,
                fdb_sys::FDB_API_VERSION as i32,
            )
        })?;
        Ok(NetworkBuilder { _private: () })
    }
}

impl Default for FdbApiBuilder {
    fn default() -> Self {
        FdbApiBuilder {
            runtime_version: fdb_sys::FDB_API_VERSION as i32,
        }
    }
}

/// Allow `NetworkOption`s to be associated with the Fdb Network
pub struct NetworkBuilder {
    _private: (),
}

impl NetworkBuilder {
    /// Called to set network options.
    pub fn set_option(self, option: NetworkOption) -> Result<Self> {
        unsafe { option.apply()? };
        Ok(self)
    }

    /// Finalizes the construction of the Network
    pub fn build(self) -> Result<(NetworkRunner, NetworkWait)> {
        unsafe { error::eval(fdb_sys::fdb_setup_network())? }

        let cond = Arc::new((Mutex::new(false), Condvar::new()));
        Ok((NetworkRunner { cond: cond.clone() }, NetworkWait { cond }))
    }

    pub fn boot(self) -> Result<NetworkAutoStop> {
        let (runner, cond) = self.build()?;

        let net_thread = thread::spawn(move || {
            runner.run().expect("failed to run");
        });

        Ok(NetworkAutoStop {
            network: Some(cond.wait()),
            handle: Some(net_thread),
        })
    }
}

pub struct NetworkRunner {
    cond: Arc<(Mutex<bool>, Condvar)>,
}

impl NetworkRunner {
    pub fn run(self) -> Result<()> {
        {
            let (lock, cvar) = &*self.cond;
            let mut started = lock.lock().unwrap();
            *started = true;
            // We notify the condvar that the value has changed.
            cvar.notify_one();
        }

        error::eval(unsafe { fdb_sys::fdb_run_network() })
    }
}

pub struct NetworkWait {
    cond: Arc<(Mutex<bool>, Condvar)>,
}

impl NetworkWait {
    pub fn wait(self) -> NetworkStop {
        // Wait for the thread to start up.
        {
            let (lock, cvar) = &*self.cond;
            let mut started = lock.lock().unwrap();
            while !*started {
                started = cvar.wait(started).unwrap();
            }
        }

        NetworkStop { _private: () }
    }
}

pub struct NetworkStop {
    _private: (),
}

impl NetworkStop {
    /// Signals the event loop invoked by `Network::run` to terminate.
    ///
    /// You must call this function and wait for fdb_run_network() to return before allowing your program to exit, or else the behavior is undefined.
    ///
    /// # Example
    ///
    /// ```rust
    /// let (runner, cond) = foundationdb::api::FdbApiBuilder::default()
    ///     .build()
    ///     .expect("failed to init api")
    ///     .build()
    ///     .expect("failed to init network");
    ///
    /// let network_thread = std::thread::spawn(move || {
    ///     runner.run().expect("failed to run");
    /// });
    ///
    /// // wait for the network to be running
    /// let network_stop = cond.wait();
    ///
    /// // do whatever you want with foundationdb
    ///
    /// network_stop.stop().expect("failed to stop network");
    /// network_thread.join().expect("failed to join fdb thread");
    /// ```
    pub fn stop(self) -> Result<()> {
        error::eval(unsafe { fdb_sys::fdb_stop_network() })
    }
}

pub struct NetworkAutoStop {
    network: Option<NetworkStop>,
    handle: Option<std::thread::JoinHandle<()>>,
}
impl Drop for NetworkAutoStop {
    fn drop(&mut self) {
        self.network
            .take()
            .unwrap()
            .stop()
            .expect("failed to stop network");
        self.handle
            .take()
            .unwrap()
            .join()
            .expect("failed to join fdb thread");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_api() {
        assert!(get_max_api_version() > 0);
    }
}
