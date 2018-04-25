extern crate rand;
extern crate std;

use foundationdb::*;

/// generate random string. Foundationdb watch only fires when value changed, so updating with same
/// value twice will not fire watches. To make examples work over multiple run, we use random
/// string as a value.
#[allow(unused)]
pub fn random_str(len: usize) -> String {
    use self::rand::Rng;
    let mut rng = rand::thread_rng();
    rng.gen_ascii_chars().take(len).collect::<String>()
}

#[allow(unused)]
pub fn setup_static() {
    let _env = &*ENV;
}

lazy_static! {
    static ref ENV: TestEnv = { TestEnv::new() };
}

pub struct TestEnv {
    network: network::Network,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl TestEnv {
    pub fn new() -> Self {
        let network = fdb_api::FdbApiBuilder::default()
            .build()
            .expect("failed to init api")
            .network()
            .build()
            .expect("failed to init network");

        let handle = std::thread::spawn(move || {
            let error = network.run();

            if let Err(error) = error {
                panic!("fdb_run_network: {}", error);
            }
        });

        network.wait();

        Self {
            network,
            handle: Some(handle),
        }
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        self.network.stop().expect("failed to stop network");
        self.handle
            .take()
            .expect("cannot dropped twice")
            .join()
            .expect("failed to join fdb thread");
    }
}
