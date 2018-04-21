use failure::Error;
use foundationdb_sys as fdb;

use context::Context;
use error::{self, FdbError};

// The Fdb states that setting the Client version should happen only once
//   and is not thread-safe, thus the choice of a lazy static enforcing a single
//   init.
lazy_static! {
    // TODO: how do we configure the network?
    static ref NETWORK: Network = Network::new().build().expect("error initializing FoundationDB");
}

pub struct Network {
    _context: &'static Context,
}

impl Network {
    /// Get the singleton context, initializes FoundationDB version.
    pub fn get() -> &'static Network {
        &NETWORK
    }

    fn new() -> NetworkBuilder {
        NetworkBuilder {}
    }

    /// This will block the current thread
    ///
    /// It must be run from a separate thread
    pub fn run(&self) -> Result<(), FdbError> {
        unsafe { error::eval(fdb::fdb_run_network())? }
        Ok(())
    }

    pub fn stop(&self) -> Result<(), FdbError> {
        unsafe { error::eval(fdb::fdb_stop_network())? }
        Ok(())
    }
}

pub struct NetworkBuilder {}

impl NetworkBuilder {
    fn build(self) -> Result<Network, FdbError> {
        // context must be established before setting up the network
        let context = Context::get();

        unsafe { error::eval(fdb::fdb_setup_network())? }

        Ok(Network { _context: context })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_builder() {
        Network::get();
    }

    #[test]
    fn test_run() {
        let network = Network::get();

        let network = Arc::new(network);
        let runner = Arc::clone(&network);
        thread::spawn(move || {
            runner.run().expect("failed to run");
        });

        println!("stop!");
        thread::sleep(Duration::from_millis(100));
        network.stop().expect("failed to stop");
        println!("stopped!");
    }
}
