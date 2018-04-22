// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

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

// lazy_static! {
//     // TODO: how do we configure the network?
//     static ref NETWORK: Network = Network::new().build().expect("error initializing FoundationDB");
// }

#[derive(Clone, Copy)]
pub struct Network {}

impl Network {
    /// This will block the current thread
    ///
    /// It must be run from a separate thread
    pub fn run(&self) -> std::result::Result<(), failure::Error> {
        if HAS_BEEN_RUN.compare_and_swap(false, true, Ordering::AcqRel) {
            return Err(format_err!("the network can only be run once per process"));
        }

        unsafe { error::eval(fdb_sys::fdb_run_network())? }
        Ok(())
    }

    /// Wait for run to have started
    pub fn wait(&self) {
        // TODO: rather than a hot loop, consider a condvar here...
        while !HAS_BEEN_RUN.load(Ordering::Acquire) {
            thread::yield_now();
        }
    }

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

pub struct NetworkBuilder {}

impl NetworkBuilder {
    pub fn new(_api: FdbApi) -> Self {
        NetworkBuilder {}
    }

    pub fn set_option(self, option: NetworkOption) -> Result<Self> {
        unsafe { option.apply()? };
        Ok(self)
    }

    pub fn build(self) -> Result<Network> {
        unsafe { error::eval(fdb_sys::fdb_setup_network())? }

        Ok(Network {})
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use fdb_api::*;

    use super::*;

    #[test]
    fn test_run() {
        let api = FdbApiBuilder::default()
            .build()
            .expect("could not initialize api");
        let network = NetworkBuilder::new(api)
            .build()
            .expect("could not initialize network");

        let network = Arc::new(network);
        let runner = Arc::clone(&network);
        thread::spawn(move || {
            runner.run().expect("failed to run");
        });

        println!("stop!");
        network.wait();
        network.stop().expect("failed to stop");
        println!("stopped!");

        // this should fail:
        assert!(network.run().is_err());
    }
}
