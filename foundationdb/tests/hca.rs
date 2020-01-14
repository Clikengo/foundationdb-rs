// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use std::collections::HashSet;
use std::iter::FromIterator;

use foundationdb::tuple::{hca::HighContentionAllocator, Subspace};
use foundationdb::{FdbResult, TransactOption};
use futures::prelude::*;

mod common;

#[test]
fn test_hca_many_sequential_allocations() {
    common::boot();
    futures::executor::block_on(test_hca_many_sequential_allocations_async())
        .expect("failed to run");
}
async fn test_hca_many_sequential_allocations_async() -> FdbResult<()> {
    const N: usize = 1000;
    const KEY: &[u8] = b"test-hca-allocate";

    let db = common::database().await?;

    {
        let tx = db.create_trx()?;
        tx.clear_subspace_range(&Subspace::from_bytes(KEY));
        tx.commit().await?;
    }

    let hca = HighContentionAllocator::new(Subspace::from_bytes(KEY));

    let mut all_ints = Vec::new();

    for _ in 0..N {
        let mut tx = db.create_trx()?;

        let next_int: i64 = hca.allocate(&mut tx).await.unwrap();
        all_ints.push(next_int);

        tx.commit().await?;
    }

    check_hca_result_uniqueness(&all_ints);

    eprintln!("ran test {:?}", all_ints);

    Ok(())
}

#[test]
fn test_hca_concurrent_allocations() {
    common::boot();
    futures::executor::block_on(test_hca_concurrent_allocations_async()).expect("failed to run");
}
async fn test_hca_concurrent_allocations_async() -> FdbResult<()> {
    const N: usize = 1000;
    const KEY: &[u8] = b"test-hca-allocate-concurrent";

    let db = common::database().await?;

    {
        let tx = db.create_trx()?;
        tx.clear_subspace_range(&Subspace::from_bytes(KEY));
        tx.commit().await?;
    }

    let hca = HighContentionAllocator::new(Subspace::from_bytes(KEY));

    let all_ints: Vec<i64> = future::try_join_all((0..N).map(|_| {
        db.transact_boxed_local(
            &hca,
            move |tx, hca| hca.allocate(tx).boxed_local(),
            TransactOption::default(),
        )
    }))
    .await
    .unwrap();
    check_hca_result_uniqueness(&all_ints);

    eprintln!("ran test {:?}", all_ints);

    Ok(())
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
