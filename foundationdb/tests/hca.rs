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

use std::collections::HashSet;

use futures::future::*;
use futures::prelude::*;

use foundationdb::error::Error;
use foundationdb::hca::HighContentionAllocator;
use foundationdb::tuple::{Element, Tuple};
use foundationdb::*;

mod common;

#[test]
fn test_allocate() {
    use foundationdb::keyselector::KeySelector;

    common::setup_static();
    const N: usize = 100;
    const KEY: &[u8] = b"h";

    let fut = Cluster::new(foundationdb::default_config_path())
        .and_then(|cluster| cluster.create_database())
        //        .and_then(|db| result(db.create_trx()))
        .and_then(|db: Database| {
            let hca = HighContentionAllocator::new(Subspace::from_bytes(KEY));

            db.transact(move |tx| {
                tx.clear_range(KEY, KEY);
                futures::future::result(Ok::<(), failure::Error>(()))
            })
            .wait();

            let mut all_ints = Vec::new();

            //            let mut tx: Transaction = db.create_trx()?;
            //            tx.clear_subspace_range(Subspace::from_bytes(KEY));
            //            tx.commit().wait()?;

            for _ in 0..N {
                let mut tx: Transaction = db.create_trx()?;

                let next_int: i64 = hca.allocate(&mut tx)?;
                println!("next: {:?}", next_int);
                all_ints.push(next_int);

                tx.commit().wait();
            }

            Ok::<_, Error>(all_ints)
        });
    //        .and_then(|mut trx| {
    //            println!("starting to run");
    //            let hca = HighContentionAllocator::new(Subspace::from_bytes(KEY));
    //
    //            trx.clear_range(KEY, KEY);
    //
    //            let mut all_ints = Vec::new();
    //
    //            for _ in 0..N {
    //
    //                let next_int : i64 = hca.allocate(&mut trx)?;
    //                all_ints.push(next_int);
    ////                println!("next: {:?}", next_int);
    //            }
    //
    //            Ok::<_, Error>(all_ints)
    //        });

    println!("running test");
    let all_ints: Vec<i64> = fut.wait().expect("failed to run");
    println!("ran test {:?}", all_ints);
}
