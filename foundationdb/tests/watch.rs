// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

extern crate foundationdb;
extern crate foundationdb_sys;
extern crate futures;
extern crate rand;

use foundationdb::*;

use futures::future::*;

use error::FdbError;

/// generate random string. Foundationdb watch only fires when value changed, so updating with same
/// value twice will not fire watches. To make examples work over multiple run, we use random
/// string as a value.
fn random_str(len: usize) -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    rng.gen_ascii_chars().take(len).collect::<String>()
}

fn example_watch() -> Box<Future<Item = (), Error = FdbError>> {
    const KEY: &'static [u8] = b"test-watch";

    let fut = Cluster::new(foundationdb::default_config_path())
        .and_then(|cluster| cluster.create_database())
        .and_then(|db| {
            let watch = result(db.create_trx()).and_then(|trx| {
                eprintln!("setting watch");
                let watch = trx.watch(KEY);
                trx.commit().map(|_| {
                    eprintln!("watch committed");
                    watch
                })
            });

            let write = result(db.create_trx()).and_then(|trx| {
                eprintln!("writing value");

                let value = random_str(10);
                trx.set(KEY, value.as_bytes());
                trx.commit().map(|_| {
                    eprintln!("write committed");
                })
            });

            // 1. Setup a watch with a key
            watch.and_then(move |watch| {
                // 2. After the watch is installed, try to update the key.
                write
                    .and_then(move |_| {
                        // 3. After updating the key, waiting for the watch
                        watch
                    })
                    .map(|_| {
                        // 4. watch fired as expected
                        eprintln!("watch fired");
                    })
            })
        });

    Box::new(fut)
}

fn example_watch_without_commit() -> Box<Future<Item = (), Error = FdbError>> {
    const KEY: &'static [u8] = b"test-watch-2";

    let fut = Cluster::new(foundationdb::default_config_path())
        .and_then(|cluster| cluster.create_database())
        .and_then(|db| result(db.create_trx()))
        .and_then(|trx| {
            eprintln!("setting watch");

            // trx will be dropped without `commit`, so a watch will be canceled
            trx.watch(KEY)
        })
        .or_else(|e| {
            // should return error_code=1025, `Operation aborted because the transaction was
            // canceled`
            eprintln!("error as expected: {:?}", e);
            Ok(())
        });

    Box::new(fut)
}

#[test]
fn watch() {
    use fdb_api::FdbApiBuilder;

    let network = FdbApiBuilder::default()
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

    example_watch().wait().expect("failed to run");
    example_watch_without_commit()
        .wait()
        .expect("failed to run");

    network.stop().expect("failed to stop network");
    handle.join().expect("failed to join fdb thread");
}
