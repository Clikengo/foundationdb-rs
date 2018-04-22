use error::{self, Result};
use fdb_api::FdbApi;
use foundationdb_sys as fdb_sys;
use options::NetworkOption;

// The Fdb states that setting the Client version should happen only once
//   and is not thread-safe, thus the choice of a lazy static enforcing a single
//   init.
// lazy_static! {
//     // TODO: how do we configure the network?
//     static ref NETWORK: Network = Network::new().build().expect("error initializing FoundationDB");
// }

pub struct Network {}

impl Network {
    /// This will block the current thread
    ///
    /// It must be run from a separate thread
    pub fn run(&self) -> Result<()> {
        unsafe { error::eval(fdb_sys::fdb_run_network())? }
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        unsafe { error::eval(fdb_sys::fdb_stop_network())? }
        Ok(())
    }
}

pub struct NetworkBuilder {}

impl NetworkBuilder {
    pub fn new(api: FdbApi) -> Self {
        NetworkBuilder {}
    }

    pub fn set_option(self, option: NetworkOption) -> Result<Self> {
        use self::NetworkOption::*;

        let fdb_option = match option {
            LocalAddress(s) => (),
            ClusterFile(s) => (),
            TraceEnable(s) => (),
            TraceRollSize(size) => (),
            TraceMaxLogsSize(size) => (),
            TraceLogGroup(s) => (),
            Knob(s) => (),
            TlsPlugin(s) => (),
            TlsCertByte(bytes) => (),
            TlsCertPath(s) => (),
            TlsKeyByte(bytes) => (),
            TlsKeyPath(s) => (),
            TlsVerifyPeer(bytes) => (),
            BuggifyEnable => (),
            BuggifyDisable => (),
            BuggifySectionActivatedProbability(probability) => (),
            BuggifySectionFiredProbability(probability) => (),
            DisableMultiVersionClientApi => (),
            CallbacksOnExternalThread => (),
            ExternalClientLibrary(s) => (),
            ExternalClientDirectory(s) => (),
            DisableLocalClient => (),
            DisableClientStatisticsLogging => (),
            EnableSlowTaskProfiling => (),
        };

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
    use std::time::Duration;

    use fdb_api::*;

    use super::*;

    #[test]
    fn test_builder() {
        // Network::get();
    }

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
        thread::sleep(Duration::from_millis(100));
        network.stop().expect("failed to stop");
        println!("stopped!");
    }
}
