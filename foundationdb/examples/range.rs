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

use foundationdb::keyselector::*;
use foundationdb::*;

use futures::future::*;
use futures::stream::*;

use error::FdbError;

fn random_str(len: usize) -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    rng.gen_ascii_chars().take(len).collect::<String>()
}

fn example_get_range() -> Box<Future<Item = (), Error = FdbError>> {
    const N: usize = 10000;

    let fut = Cluster::new(foundationdb::default_config_path())
        .and_then(|cluster| cluster.create_database())
        .and_then(|db| result(db.create_trx()))
        .and_then(|trx| {
            let key_begin = "test-range-";
            let key_end = "test-range.";

            trx.clear_range(key_begin.as_bytes(), key_end.as_bytes());

            for _ in 0..N {
                let key = format!("{}-{}", key_begin, random_str(10));
                let value = random_str(10);
                trx.set(key.as_bytes(), value.as_bytes());
            }

            let begin = KeySelector::first_greater_or_equal(key_begin.as_bytes());
            let end = KeySelector::first_greater_than(key_end.as_bytes());
            let opt = transaction::RangeOptionBuilder::new(begin, end).build();

            trx.get_ranges(opt)
                .map_err(|(_opt, e)| e)
                .fold(0, |count, item| {
                    let kvs = item.keyvalues();
                    Ok(count + kvs.as_ref().len())
                })
                .map(|count| {
                    if count != N {
                        panic!("count expected={}, found={}", N, count);
                    }
                    eprintln!("count: {:?}", count);
                })
        });

    Box::new(fut)
}

fn main() {
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

    example_get_range().wait().expect("failed to run");

    network.stop().expect("failed to stop network");
    handle.join().expect("failed to join fdb thread");
}
