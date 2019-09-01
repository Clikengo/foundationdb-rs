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

#[macro_use]
extern crate failure;

use foundationdb::*;

use futures::executor::block_on;

mod common;

#[test]
fn test_transact_error() {
    common::setup_static();

    let db = Database::new(foundationdb::default_config_path()).unwrap();
    assert!(block_on(db.transact(|_trx| {
        async {
            bail!("failed");

            #[allow(unreachable_code)]
            Ok(())
        }
    }))
    .is_err());
}

#[test]
fn test_transact_success() {
    common::setup_static();

    let db = Database::new(foundationdb::default_config_path()).unwrap();
    assert!(block_on(db.transact(|trx| {
        async move {
            trx.set(b"test", b"1");
            assert_eq!(trx.get(b"test", false).await?.value().unwrap(), b"1");

            Ok(())
        }
    }))
    .is_ok());
}

// Makes the key dirty. It will abort transactions which performs non-snapshot read on the `key`.
async fn make_dirty(db: &Database, key: &[u8]) {
    let trx = db.create_trx().unwrap();
    trx.set(key, b"");
    trx.commit().await.unwrap();
}

#[test]
fn test_transact_conflict() {
    use std::sync::{atomic::*, Arc};

    const KEY: &[u8] = b"test-transact";
    const RETRY_COUNT: usize = 5;
    common::setup_static();

    let try_count = Arc::new(AtomicUsize::new(0));
    let try_count0 = try_count.clone();

    let db = Database::new(foundationdb::default_config_path()).unwrap();

    let fut = db.transact(move |trx| {
        let try_count0 = try_count0.clone();
        async move {
            // increment try counter
            try_count0.fetch_add(1, Ordering::SeqCst);

            trx.set_option(options::TransactionOption::RetryLimit(RETRY_COUNT as u32))
                .expect("failed to set retry limit");

            let db = trx.database();

            // update conflict range
            let res = trx.get(KEY, false).await?;

            // make current transaction invalid by making conflict
            make_dirty(&db, KEY).await;

            let trx = res.transaction();
            trx.set(KEY, common::random_str(10).as_bytes());

            Ok(())
        }
    });

    block_on(fut).expect_err("commit should have failed");

    // `TransactionOption::RetryCount` does not count first try, so `try_count` should be equal to
    // `RETRY_COUNT+1`
    assert_eq!(try_count.load(Ordering::SeqCst), RETRY_COUNT + 1);
}
