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

#[test]
fn test_set_conflict() {
    common::setup_static();

    let key = b"test-conflict";
    let fut = Cluster::new(foundationdb::default_config_path())
        .and_then(|cluster| cluster.create_database())
        .and_then(|db| {
            // First transaction. It will be committed before second one.
            let fut_set1 = result(db.create_trx()).and_then(|trx1| {
                trx1.set(key, common::random_str(10).as_bytes());
                trx1.commit()
            });

            // Second transaction. There will be conflicted by first transaction before commit.
            result(db.create_trx())
                .and_then(|trx2| {
                    // try to read value to set conflict range
                    trx2.get(key, false)
                })
                .and_then(move |val| {
                    // commit first transaction to create conflict
                    fut_set1.map(move |_trx1| val.transaction())
                })
                .and_then(|trx2| {
                    // commit seconds transaction, which will cause conflict
                    trx2.set(key, common::random_str(10).as_bytes());
                    trx2.commit()
                })
                .map(|_v| {
                    panic!("should not be committed without conflict");
                })
                .or_else(|e| {
                    eprintln!("error as expected: {:?}", e);
                    Ok(())
                })
        });

    fut.wait().expect("failed to run")
}

#[test]
fn test_set_conflict_snapshot() {
    common::setup_static();

    let key = b"test-conflict-snapshot";
    let fut = Cluster::new(foundationdb::default_config_path())
        .and_then(|cluster| cluster.create_database())
        .and_then(|db| {
            // First transaction. It will be committed before second one.
            let fut_set1 = result(db.create_trx()).and_then(|trx1| {
                trx1.set(key, common::random_str(10).as_bytes());
                trx1.commit()
            });

            // Second transaction.
            result(db.create_trx())
                .and_then(|trx2| {
                    // snapshot read does not set conflict range, so both transaction will be
                    // committed.
                    trx2.get(key, true)
                })
                .and_then(move |val| {
                    // commit first transaction
                    fut_set1.map(move |_trx1| val.transaction())
                })
                .and_then(|trx2| {
                    // commit seconds transaction, which will *not* cause conflict because of
                    // snapshot read
                    trx2.set(key, common::random_str(10).as_bytes());
                    trx2.commit()
                })
                .map(|_v| ())
        });

    fut.wait().expect("failed to run")
}
