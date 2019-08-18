// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

#![feature(async_await)]

extern crate foundationdb;
extern crate futures;
#[macro_use]
extern crate lazy_static;

use std::collections::HashSet;
use std::iter::FromIterator;

use futures::future::*;

use foundationdb::error::Error;
use foundationdb::hca::HighContentionAllocator;
use foundationdb::*;
use futures::executor::block_on;

mod common;

#[test]
fn test_hca_many_sequential_allocations() {
    common::setup_static();
    const N: usize = 6000;
    const KEY: &[u8] = b"test-hca-allocate";

    let db = Database::new(foundationdb::default_config_path()).unwrap();

    let fut = async move {
        let cleared_range = db
            .transact(move |tx| {
                tx.clear_subspace_range(Subspace::from_bytes(KEY));
                futures::future::ready(Ok::<(), failure::Error>(()))
            })
            .await;

        cleared_range.expect("unable to clear hca test range");

        let hca = HighContentionAllocator::new(Subspace::from_bytes(KEY));

        let mut all_ints = Vec::new();

        for _ in 0..N {
            let tx: Transaction = db.create_trx()?;

            let next_int: i64 = hca.allocate(tx.clone()).await?;
            all_ints.push(next_int);

            tx.commit().await?;
        }

        Ok::<_, Error>(all_ints)
    };

    let all_ints: Vec<i64> = block_on(fut).expect("failed to run");
    check_hca_result_uniqueness(&all_ints);

    eprintln!("ran test {:?}", all_ints);
}

#[test]
fn test_hca_concurrent_allocations() {
    common::setup_static();
    const N: usize = 1000;
    const KEY: &[u8] = b"test-hca-allocate-concurrent";

    let db = Database::new(foundationdb::default_config_path()).unwrap();

    let fut = async move {
            let cleared_range = db
                .transact(move |tx| {
                    tx.clear_subspace_range(Subspace::from_bytes(KEY));
                    futures::future::ready(Ok::<(), failure::Error>(()))
                }).await;

            cleared_range.expect("unable to clear hca test range");

            let mut futures = Vec::new();
            let mut all_ints: Vec<i64> = Vec::new();

            for _ in 0..N {
                let f = db.transact(move |tx| async move {
                    let ha = HighContentionAllocator::new(Subspace::from_bytes(KEY));
                    ha.allocate(tx).map_err(|e| e.into()).await
                });

                futures.push(f);
            }

            for allocation in futures {
                let i = allocation.await.expect("unable to get allocation");
                all_ints.push(i);
            }

            Ok::<_, Error>(all_ints)
    };

    let all_ints: Vec<i64> = block_on(fut).expect("failed to run");
    check_hca_result_uniqueness(&all_ints);

    eprintln!("ran test {:?}", all_ints);
}

fn check_hca_result_uniqueness(results: &Vec<i64>) {
    let result_set: HashSet<i64> = HashSet::from_iter(results.clone());

    if results.len() != result_set.len() {
        panic!(
            "Set size does not much, got duplicates from HCA. Set: {:?}, List: {:?}",
            result_set.len(),
            results.len(),
        );
    }
}
