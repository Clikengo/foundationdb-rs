// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

extern crate foundationdb;
extern crate foundationdb_sys;
extern crate futures;

use foundationdb::*;

use futures::future::*;

use error::FdbError;

//TODO: impl Future
fn example_set_get() -> Box<Future<Item = (), Error = FdbError>> {
    let fut = Cluster::new(foundationdb::default_config_path())
        .and_then(|cluster| cluster.create_database())
        .and_then(|db| result(db.create_trx()))
        .and_then(|trx| {
            trx.set(b"hello", b"world");
            trx.commit()
        })
        .and_then(|trx| result(trx.database().create_trx()))
        .and_then(|trx| trx.get(b"hello"))
        .and_then(|res| {
            let val = res.value();
            eprintln!("value: {:?}", val);

            let trx = res.transaction();
            trx.clear(b"hello");
            trx.commit()
        })
        .and_then(|trx| result(trx.database().create_trx()))
        .and_then(|trx| trx.get(b"hello"))
        .and_then(|res| {
            eprintln!("value: {:?}", res.value());
            Ok(())
        });

    Box::new(fut)
}

fn example_get_multi() -> Box<Future<Item = (), Error = FdbError>> {
    let fut = Cluster::new(foundationdb::default_config_path())
        .and_then(|cluster| cluster.create_database())
        .and_then(|db| result(db.create_trx()))
        .and_then(|trx| {
            let keys: &[&[u8]] = &[b"hello", b"world", b"foo", b"bar"];

            let futs = keys.iter().map(|k| trx.get(k)).collect::<Vec<_>>();
            join_all(futs)
        })
        .and_then(|results| {
            for (i, res) in results.into_iter().enumerate() {
                eprintln!("res[{}]: {:?}", i, res.value());
            }
            Ok(())
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

    example_set_get().wait().expect("failed to run");
    example_get_multi().wait().expect("failed to run");

    network.stop().expect("failed to stop network");
    handle.join().expect("failed to join fdb thread");
}
