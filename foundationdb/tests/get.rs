// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
use foundationdb::*;
use futures::future::*;
use std::ops::Deref;
use std::sync::{atomic::*, Arc};

mod common;

#[test]
fn test_set_get() {
    common::boot();
    futures::executor::block_on(test_set_get_async()).expect("failed to run")
}
async fn test_set_get_async() -> error::Result<()> {
    let db = Database::default()?;

    let trx = db.create_trx()?;
    trx.set(b"hello", b"world");
    trx.commit().await?;

    let trx = db.create_trx()?;
    let value = trx.get(b"hello", false).await?.unwrap();
    assert_eq!(value.deref(), b"world");

    trx.clear(b"hello");
    trx.commit().await?;

    let trx = db.create_trx()?;
    assert!(trx.get(b"hello", false).await?.is_none());

    Ok(())
}

#[test]
fn test_get_multi() {
    common::boot();
    futures::executor::block_on(test_get_multi_async()).expect("failed to run")
}
async fn test_get_multi_async() -> error::Result<()> {
    let db = Database::default()?;

    let trx = db.create_trx()?;
    let keys: &[&[u8]] = &[b"hello", b"world", b"foo", b"bar"];
    let _results = try_join_all(keys.iter().map(|k| trx.get(k, false))).await?;

    Ok(())
}

#[test]
fn test_set_conflict() {
    common::boot();
    futures::executor::block_on(test_set_conflict_async()).expect("failed to run")
}
async fn test_set_conflict_async() -> error::Result<()> {
    let key = b"test_set_conflict";
    let db = Database::default()?;

    let trx1 = db.create_trx()?;
    let trx2 = db.create_trx()?;

    // try to read value to set conflict range
    let _ = trx2.get(key, false).await?;

    // commit first transaction to create conflict
    trx1.set(key, common::random_str(10).as_bytes());
    trx1.commit().await?;

    // commit seconds transaction, which will cause conflict
    trx2.set(key, common::random_str(10).as_bytes());
    assert!(trx2.commit().await.is_err());

    Ok(())
}
#[test]
fn test_set_conflict_snapshot() {
    common::boot();
    futures::executor::block_on(test_set_conflict_snapshot_async()).expect("failed to run")
}
async fn test_set_conflict_snapshot_async() -> error::Result<()> {
    let key = b"test_set_conflict_snapshot";
    let db = Database::default()?;

    let trx1 = db.create_trx()?;
    let trx2 = db.create_trx()?;

    // snapshot read does not set conflict range, so both transaction will be
    // committed.
    let _ = trx2.get(key, true).await?;

    // commit first transaction
    trx1.set(key, common::random_str(10).as_bytes());
    trx1.commit().await?;

    // commit seconds transaction, which will *not* cause conflict because of
    // snapshot read
    trx2.set(key, common::random_str(10).as_bytes());
    trx2.commit().await?;

    Ok(())
}

// Makes the key dirty. It will abort transactions which performs non-snapshot read on the `key`.
async fn make_dirty(db: &Database, key: &[u8]) -> error::Result<()> {
    let trx = db.create_trx()?;
    trx.set(key, b"");
    trx.commit().await?;

    Ok(())
}
#[test]
fn test_transact() {
    common::boot();
    futures::executor::block_on(test_transact_async()).expect("failed to run")
}
async fn test_transact_async() -> error::Result<()> {
    const KEY: &[u8] = b"test_transact";
    const RETRY_COUNT: usize = 5;
    async fn async_body(
        db: &Database,
        trx: &Transaction,
        try_count0: Arc<AtomicUsize>,
    ) -> error::Result<()> {
        // increment try counter
        try_count0.fetch_add(1, Ordering::SeqCst);

        trx.set_option(options::TransactionOption::RetryLimit(RETRY_COUNT as i32))
            .expect("failed to set retry limit");

        // update conflict range
        trx.get(KEY, false).await?;

        // make current transaction invalid by making conflict
        make_dirty(&db, KEY).await?;

        trx.set(KEY, common::random_str(10).as_bytes());

        // `Database::transact` will handle commit by itself, so returns without commit
        Ok(())
    }

    let try_count = Arc::new(AtomicUsize::new(0));
    let db = Database::default()?;
    let res = db
        .transact(
            &db,
            |trx, db| async_body(db, trx, try_count.clone()).boxed(),
            database::TransactOption::default(),
        )
        .await;
    assert!(res.is_err(), "should not be able to commit");

    // `TransactionOption::RetryCount` does not count first try, so `try_count` should be equal to
    // `RETRY_COUNT+1`
    assert_eq!(try_count.load(Ordering::SeqCst), RETRY_COUNT + 1);

    Ok(())
}
#[test]
fn test_versionstamp() {
    common::boot();
    futures::executor::block_on(test_versionstamp_async()).expect("failed to run")
}
async fn test_versionstamp_async() -> error::Result<()> {
    const KEY: &[u8] = b"test_versionstamp";
    let db = Database::default()?;

    let trx = db.create_trx()?;
    trx.set(KEY, common::random_str(10).as_bytes());
    let f_version = trx.get_versionstamp();
    trx.commit().await?;
    f_version.await?;

    Ok(())
}

#[test]
fn test_read_version() {
    common::boot();
    futures::executor::block_on(test_read_version_async()).expect("failed to run")
}
async fn test_read_version_async() -> error::Result<()> {
    let db = Database::default()?;

    let trx = db.create_trx()?;
    trx.get_read_version().await?;

    Ok(())
}

#[test]
fn test_set_read_version() {
    common::boot();
    futures::executor::block_on(test_set_read_version_async()).expect("failed to run")
}
async fn test_set_read_version_async() -> error::Result<()> {
    const KEY: &[u8] = b"test_set_read_version";
    let db = Database::default()?;

    let trx = db.create_trx()?;
    trx.set_read_version(0);
    assert!(trx.get(KEY, false).await.is_err());

    Ok(())
}
