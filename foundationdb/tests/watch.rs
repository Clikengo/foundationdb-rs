// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

extern crate foundationdb;
extern crate futures;
#[macro_use]
extern crate lazy_static;

use foundationdb::*;
use futures::future::*;

mod common;

#[test]
fn test_watch() {
    common::setup_static();
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

                let value = common::random_str(10);
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

    fut.wait().expect("failed to run")
}

#[test]
fn test_watch_without_commit() {
    common::setup_static();
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
            Ok::<(), error::Error>(())
        });

    fut.wait().expect("failed to run")
}
