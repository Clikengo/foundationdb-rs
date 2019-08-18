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

use crate::error::Result;
use foundationdb::*;
use futures::executor::block_on;
use futures::future::*;

mod common;

#[test]
fn test_set_get() {
    common::setup_static();
    let db = Database::new(foundationdb::default_config_path()).unwrap();
    let fut = ready(db.create_trx())
        .and_then(|trx| {
            trx.set(b"hello", b"world");
            trx.commit()
        })
        .and_then(|trx| ready(trx.database().create_trx()))
        .and_then(|trx| trx.get(b"hello", false))
        .and_then(|res| {
            let val = res.value();
            eprintln!("value: {:?}", val);

            let trx = res.transaction();
            trx.clear(b"hello");
            trx.commit()
        })
        .and_then(|trx| ready(trx.database().create_trx()))
        .and_then(|trx| trx.get(b"hello", false))
        .and_then(|res| {
            eprintln!("value: {:?}", res.value());
            ok(())
        });

    block_on(fut).expect("failed to run")
}

#[test]
fn test_get_multi() {
    common::setup_static();
    let db = Database::new(foundationdb::default_config_path()).unwrap();
    let fut = ready(db.create_trx())
        .and_then(|trx| {
            let keys: &[&[u8]] = &[b"hello", b"world", b"foo", b"bar"];

            let futs = keys.iter().map(|k| trx.get(k, false)).collect::<Vec<_>>();
            try_join_all(futs)
        })
        .and_then(|results| {
            for (i, res) in results.into_iter().enumerate() {
                eprintln!("res[{}]: {:?}", i, res.value());
            }
            ok(())
        });

    block_on(fut).expect("failed to run")
}

#[test]
fn test_set_conflict() {
    common::setup_static();

    let key = b"test-conflict";
    let db = Database::new(foundationdb::default_config_path()).unwrap();

    let r: Result<()> = Ok(());
    let fut = ready(r).and_then(|_| {
        // First transaction. It will be committed before second one.
        let fut_set1 = ready(db.create_trx()).and_then(|trx1| {
            trx1.set(key, common::random_str(10).as_bytes());
            trx1.commit()
        });

        // Second transaction. There will be conflicted by first transaction before commit.
        ready(db.create_trx())
            .and_then(|trx2| {
                // try to read value to set conflict range
                trx2.get(key, false)
            })
            .and_then(move |val| {
                // commit first transaction to create conflict
                fut_set1.map(move |_trx1| Ok(val.transaction()))
            })
            .and_then(|trx2| {
                // commit seconds transaction, which will cause conflict
                trx2.set(key, common::random_str(10).as_bytes());
                trx2.commit()
            })
            .then(|r| {
                // should conflict
                assert!(r.is_err());

                ok(())
            })
    });

    block_on(fut).expect("failed to run")
}

#[test]
fn test_set_conflict_snapshot() {
    common::setup_static();

    let key = b"test-conflict-snapshot";
    let db = Database::new(foundationdb::default_config_path()).unwrap();

    let r: Result<()> = Ok(());
    let fut = ready(r).and_then(|_| {
        // First transaction. It will be committed before second one.
        let fut_set1 = ready(db.create_trx()).and_then(|trx1| {
            trx1.set(key, common::random_str(10).as_bytes());
            trx1.commit()
        });

        // Second transaction.
        ready(db.create_trx())
            .and_then(|trx2| {
                // snapshot read does not set conflict range, so both transaction will be
                // committed.
                trx2.get(key, true)
            })
            .and_then(move |val| {
                // commit first transaction
                fut_set1.map(move |_trx1| Ok(val.transaction()))
            })
            .and_then(|trx2| {
                // commit seconds transaction, which will *not* cause conflict because of
                // snapshot read
                trx2.set(key, common::random_str(10).as_bytes());
                trx2.commit()
            })
            .map(|_v| Ok(()))
    });

    block_on(fut).expect("failed to run")
}

#[test]
fn test_versionstamp() {
    const KEY: &[u8] = b"test-versionstamp";
    common::setup_static();

    let db = Database::new(foundationdb::default_config_path()).unwrap();

    let r: Result<()> = Ok(());
    let fut = ready(r)
        .and_then(|_| ready(db.create_trx()))
        .and_then(|trx| {
            trx.set(KEY, common::random_str(10).as_bytes());
            let f_version = trx.get_versionstamp();
            trx.commit().and_then(move |_trx| f_version)
        })
        .and_then(|r| {
            eprintln!("versionstamp: {:?}", r.versionstamp());

            ok(())
        });

    block_on(fut).expect("failed to run");
}

#[test]
fn test_read_version() {
    common::setup_static();

    let db = Database::new(foundationdb::default_config_path()).unwrap();

    let r: Result<()> = Ok(());
    let fut = ready(r)
        .and_then(|_| ready(db.create_trx()))
        .and_then(|trx| trx.get_read_version())
        .and_then(|v| {
            assert!(v > 0);

            ok(())
        });

    block_on(fut).expect("failed to run");
}

#[test]
fn test_set_read_version() {
    const KEY: &[u8] = b"test-versionstamp";
    common::setup_static();

    let db = Database::new(foundationdb::default_config_path()).unwrap();

    let r: Result<()> = Ok(());
    let fut = ready(r)
        .and_then(|_| ready(db.create_trx()))
        .and_then(|trx| {
            trx.set_read_version(0);
            trx.get(KEY, false)
        })
        .then(|r| {
            assert!(r.is_err());
            ready(Ok::<(), ()>(()))
        });

    block_on(fut).expect("failed to run");
}
