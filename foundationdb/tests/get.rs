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
fn test_set_get() {
    common::setup_static();
    let fut = Cluster::new(foundationdb::default_config_path())
        .and_then(|cluster| cluster.create_database())
        .and_then(|db| result(db.create_trx()))
        .and_then(|trx| {
            trx.set(b"hello", b"world");
            trx.commit()
        })
        .and_then(|trx| result(trx.database().create_trx()))
        .and_then(|trx| trx.get(b"hello", false))
        .and_then(|res| {
            let val = res.value();
            eprintln!("value: {:?}", val);

            let trx = res.transaction();
            trx.clear(b"hello");
            trx.commit()
        })
        .and_then(|trx| result(trx.database().create_trx()))
        .and_then(|trx| trx.get(b"hello", false))
        .and_then(|res| {
            eprintln!("value: {:?}", res.value());
            Ok(())
        });

    fut.wait().expect("failed to run")
}

#[test]
fn test_get_multi() {
    common::setup_static();
    let fut = Cluster::new(foundationdb::default_config_path())
        .and_then(|cluster| cluster.create_database())
        .and_then(|db| result(db.create_trx()))
        .and_then(|trx| {
            let keys: &[&[u8]] = &[b"hello", b"world", b"foo", b"bar"];

            let futs = keys.iter().map(|k| trx.get(k, false)).collect::<Vec<_>>();
            join_all(futs)
        })
        .and_then(|results| {
            for (i, res) in results.into_iter().enumerate() {
                eprintln!("res[{}]: {:?}", i, res.value());
            }
            Ok(())
        });

    fut.wait().expect("failed to run")
}
