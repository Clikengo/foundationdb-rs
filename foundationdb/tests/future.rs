// Copyright 2019 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
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
use foundationdb::transaction::TrxGet;
use foundationdb::*;
use futures::prelude::*;

mod common;

struct AbortingFuture {
    inner: TrxGet,
    polled: bool,
}

impl Future for AbortingFuture {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Self::Item>> {
        // poll once only
        if !self.polled {
            self.polled = true;
            let _ = self.inner.poll();
        }

        Ok(Async::Ready(()))
    }
}

#[test]
// dropping a future while it's in the pending state should not crash
fn test_future_discard() {
    common::setup_static();

    let db = Cluster::new(foundationdb::default_config_path())
        .and_then(|cluster| cluster.create_database())
        .wait()
        .unwrap();

    for _i in 0..=1000 {
        db.transact(|trx| AbortingFuture {
            inner: trx.get(b"key", false),
            polled: false,
        })
        .wait()
        .unwrap();
    }
}
