// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Implementations of the Network related functions for FoundationDB
//!
//! see https://apple.github.io/foundationdb/api-c.html#network

use std;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use failure;

use error::{self, Result};
use fdb_api::FdbApi;
use foundationdb_sys as fdb_sys;
use options::NetworkOption;

// The Fdb states that setting the Client version should happen only once
//   and is not thread-safe, thus the choice of a lazy static enforcing a single
//   init.
static HAS_BEEN_RUN: AtomicBool = AtomicBool::new(false);

/// The FoundationDB client library performs most tasks on a singleton thread (which usually will be a different thread than your application runs on).
///
/// These functions are used to configure, start and stop the FoundationDB event loop on this thread.
///
/// *NOTE* Networks may only be constructed from an initalized `fdb_api::FdbApi`
#[derive(Clone, Copy)]
pub struct Network(private::PrivateNetwork);

// forces the construction to be private to this module
mod private {
    #[derive(Clone, Copy)]
    pub(super) struct PrivateNetwork;
}

impl Network {
    /// Must be called before any asynchronous functions in this API can be expected to complete.
    ///
    /// Unless your program is entirely event-driven based on results of asynchronous functions in this API and has no event loop of its own, you will want to invoke this function on an auxiliary thread (which it is your responsibility to create).
    ///
    /// This function will not return until `Network::stop` is called by you or a serious error occurs. You must not invoke `run` concurrently or reentrantly while it is already running.
    pub fn run(&self) -> std::result::Result<(), failure::Error> {
        if HAS_BEEN_RUN.compare_and_swap(false, true, Ordering::AcqRel) {
            return Err(format_err!("the network can only be run once per process"));
        }

        // TODO: before running, we may want to register a thread destroyed notification, not sure
        //   what we'd need that for ATM, see: https://apple.github.io/foundationdb/api-c.html#network
        //   and fdb_add_network_thread_completion_hook

        unsafe { error::eval(fdb_sys::fdb_run_network())? }
        Ok(())
    }

    /// Waits for run to have started
    pub fn wait(&self) {
        // TODO: rather than a hot loop, consider a condvar here...
        while !HAS_BEEN_RUN.load(Ordering::Acquire) {
            thread::yield_now();
        }
    }

    /// Signals the event loop invoked by `Network::run` to terminate.
    ///
    /// You must call this function and wait for fdb_run_network() to return before allowing your program to exit, or else the behavior is undefined.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::thread;
    /// use foundationdb;
    ///
    /// let network = foundationdb::init().expect("failed to initialize Fdb");
    ///
    /// let handle = std::thread::spawn(move || {
    ///     let error = network.run();
    ///
    ///     if let Err(error) = error {
    ///         panic!("fdb_run_network: {}", error);
    ///     }
    /// });
    ///
    /// network.wait();
    ///
    /// // do some interesting things with the API...
    ///
    /// network.stop().expect("failed to stop network");
    /// handle.join().expect("failed to join fdb thread");
    /// ```
    pub fn stop(&self) -> std::result::Result<(), failure::Error> {
        if !HAS_BEEN_RUN.load(Ordering::Acquire) {
            return Err(format_err!(
                "the network must be runn before trying to stop"
            ));
        }

        unsafe { error::eval(fdb_sys::fdb_stop_network())? }
        Ok(())
    }
}

/// Allow `NetworkOption`s to be associated with the Fdb Network
pub struct NetworkBuilder(private::PrivateNetwork);

impl NetworkBuilder {
    /// Called to set network options.
    pub fn set_option(self, option: NetworkOption) -> Result<Self> {
        unsafe { option.apply()? };
        Ok(self)
    }

    /// Finalizes the construction of the Network
    pub fn build(self) -> Result<Network> {
        unsafe { error::eval(fdb_sys::fdb_setup_network())? }

        Ok(Network(private::PrivateNetwork))
    }
}

impl From<FdbApi> for NetworkBuilder {
    fn from(_api: FdbApi) -> Self {
        NetworkBuilder(private::PrivateNetwork)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use fdb_api::*;

    use super::*;

    // TODO: this test will break other integration tests...
    #[test]
    fn test_run() {
        let api = FdbApiBuilder::default()
            .build()
            .expect("could not initialize api");
        let network = NetworkBuilder::from(api)
            .build()
            .expect("could not initialize network");

        let network = Arc::new(network);
        let runner = Arc::clone(&network);
        let net_thread = thread::spawn(move || {
            runner.run().expect("failed to run");
        });

        println!("stop!");
        network.wait();
        network.stop().expect("failed to stop");
        net_thread.join().expect("failed to join net thread");
        println!("stopped!");

        // this should fail:
        assert!(network.run().is_err());
    }
}
