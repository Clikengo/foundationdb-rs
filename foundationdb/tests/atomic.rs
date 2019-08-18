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

use crate::error::Result;
use byteorder::ByteOrder;
use foundationdb::*;
use futures::executor::block_on;
use futures::future::*;
use std::pin::Pin;

mod common;

fn atomic_add(db: Database, key: &[u8], value: i64) -> Pin<Box<dyn Future<Output = Result<()>>>> {
    let trx = match db.create_trx() {
        Ok(trx) => trx,
        Err(e) => return Box::pin(err(e)),
    };

    let val = {
        let mut buf = [0u8; 8];
        byteorder::LE::write_i64(&mut buf, value);
        buf
    };
    trx.atomic_op(key, &val, options::MutationType::Add);

    let fut = trx.commit().map(|_trx| Ok(()));
    Box::pin(fut)
}

#[test]
fn test_atomic() {
    common::setup_static();
    const KEY: &[u8] = b"test-atomic";

    let db = Database::new(foundationdb::default_config_path()).unwrap();
    let r: Result<()> = Ok(());
    let fut = ready(r)
        .and_then(|_| {
            let trx = db.create_trx().unwrap();
            trx.clear(KEY);
            trx.commit()
        })
        .and_then(|_| {
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
            join(fut_add, fut_sub).map(|_| Ok(()))
        })
        .and_then(|_| ready(db.create_trx()).and_then(|trx| trx.get(KEY, false)))
        .and_then(|res| {
            let value = res.value().expect("value should exists");

            // A value should be zero, as same number of atomic add/sub operations are done.
            let v: i64 = byteorder::LE::read_i64(&value);
            if v != 0 {
                panic!("expected 0, found {}", v);
            }

            ok(())
        });

    block_on(fut).expect("failed to run");
}
