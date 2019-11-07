// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use foundationdb::keyselector::KeySelector;
use foundationdb::*;
use futures::future;
use futures::prelude::*;
use std::borrow::Cow;

mod common;

async fn test_get_range_async() -> FdbResult<()> {
    const N: usize = 10000;

    let db = common::database().await?;

    {
        let trx = db.create_trx()?;
        let key_begin = "test-range-";
        let key_end = "test-range.";

        eprintln!("clearing...");
        trx.clear_range(key_begin.as_bytes(), key_end.as_bytes());

        eprintln!("inserting...");
        for _ in 0..N {
            let key = format!("{}-{}", key_begin, common::random_str(10));
            let value = common::random_str(10);
            trx.set(key.as_bytes(), value.as_bytes());
        }

        eprintln!("counting...");
        let begin = KeySelector::first_greater_or_equal(Cow::Borrowed(key_begin.as_bytes()));
        let end = KeySelector::first_greater_than(Cow::Borrowed(key_end.as_bytes()));
        let opt = transaction::RangeOptionBuilder::new(begin, end).build();

        let count = trx
            .get_ranges(opt, false)
            .try_fold(0usize, |count, kvs| future::ok(count + kvs.as_ref().len()))
            .await?;

        assert_eq!(count, N);
        eprintln!("count: {:?}", count);
    }

    Ok(())
}
#[test]
fn test_get_range() {
    common::boot();
    futures::executor::block_on(test_get_range_async()).expect("failed to run");
}
