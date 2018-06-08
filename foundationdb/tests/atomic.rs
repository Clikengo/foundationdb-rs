// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

extern crate byteorder;
extern crate foundationdb;
extern crate futures;
#[macro_use]
extern crate lazy_static;

use byteorder::ByteOrder;
use foundationdb::*;
use futures::future::*;

mod common;

//TODO: impl Future
fn atomic_add(
    db: Database,
    key: &[u8],
    value: i64,
) -> Box<Future<Item = (), Error = error::Error>> {
    let trx = match db.create_trx() {
        Ok(trx) => trx,
        Err(e) => return Box::new(err(e)),
    };

    let val = {
        let mut buf = [0u8; 8];
        byteorder::LE::write_i64(&mut buf, value);
        buf
    };
    trx.atomic_op(key, &val, options::MutationType::Add);

    let fut = trx.commit().map(|_trx| ());
    Box::new(fut)
}

#[test]
fn test_atomic() {
    common::setup_static();
    const KEY: &[u8] = b"test-atomic";

    let fut = Cluster::new(foundationdb::default_config_path())
        .and_then(|cluster| cluster.create_database())
        .and_then(|db| {
            // clear key before run example
            result(db.create_trx())
                .and_then(|trx| {
                    trx.clear(KEY);
                    trx.commit()
                })
                .map(|trx| trx.database())
        })
        .and_then(|db| {
            let n = 1000usize;

            // Run `n` add(1) operations in parallel
            let db0 = db.clone();
            let fut_add_list = (0..n)
                .into_iter()
                .map(move |_| atomic_add(db0.clone(), KEY, 1))
                .collect::<Vec<_>>();
            let fut_add = join_all(fut_add_list);

            // Run `n` add(-1) operations in parallel
            let db0 = db.clone();
            let fut_sub_list = (0..n)
                .into_iter()
                .map(move |_| atomic_add(db0.clone(), KEY, -1))
                .collect::<Vec<_>>();
            let fut_sub = join_all(fut_sub_list);

            // Wait for all atomic operations
            fut_add.join(fut_sub).map(move |_| db)
        })
        .and_then(|db| result(db.create_trx()).and_then(|trx| trx.get(KEY, false)))
        .and_then(|res| {
            let value = res.value().expect("value should exists");

            // A value should be zero, as same number of atomic add/sub operations are done.
            let v: i64 = byteorder::LE::read_i64(&value);
            if v != 0 {
                panic!("expected 0, found {}", v);
            }

            Ok(())
        });

    fut.wait().expect("failed to run");
}
