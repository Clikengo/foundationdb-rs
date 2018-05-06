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

use foundationdb::error::Error;
use foundationdb::*;
use futures::future::*;
use futures::prelude::*;

mod common;

#[test]
fn test_get_range() {
    use foundationdb::keyselector::KeySelector;

    common::setup_static();
    const N: usize = 10000;

    let fut = Cluster::new(foundationdb::default_config_path())
        .and_then(|cluster| cluster.create_database())
        .and_then(|db| result(db.create_trx()))
        .and_then(|trx| {
            let key_begin = "test-range-";
            let key_end = "test-range.";

            trx.clear_range(key_begin.as_bytes(), key_end.as_bytes());

            for _ in 0..N {
                let key = format!("{}-{}", key_begin, common::random_str(10));
                let value = common::random_str(10);
                trx.set(key.as_bytes(), value.as_bytes());
            }

            let begin = KeySelector::first_greater_or_equal(key_begin.as_bytes());
            let end = KeySelector::first_greater_than(key_end.as_bytes());
            let opt = transaction::RangeOptionBuilder::new(begin, end).build();

            trx.get_ranges(opt)
                .map_err(|(_opt, e)| e)
                .fold(0, |count, item| {
                    let kvs = item.keyvalues();
                    Ok::<_, Error>(count + kvs.as_ref().len())
                })
                .map(|count| {
                    if count != N {
                        panic!("count expected={}, found={}", N, count);
                    }
                    eprintln!("count: {:?}", count);
                })
        });

    fut.wait().expect("failed to run")
}
