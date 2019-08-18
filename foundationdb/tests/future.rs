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
use futures::executor::block_on;
use futures::future::*;
use std::pin::Pin;
use std::task::{Context, Poll};

mod common;

struct AbortingFuture {
    inner: TrxGet,
    polled: bool,
}

impl Future for AbortingFuture {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // poll once only
        if !self.polled {
            self.polled = true;
            let _ = self.inner.poll_unpin(cx);
        }

        Poll::Ready(Ok(()))
    }
}

#[test]
// dropping a future while it's in the pending state should not crash
fn test_future_discard() {
    common::setup_static();

    let db = Database::new(foundationdb::default_config_path()).unwrap();

    for _i in 0..=1000 {
        block_on(db.transact(|trx| {
            AbortingFuture {
                inner: trx.get(b"key", false),
                polled: false,
            }
            .map_err(|e| e.into())
        }))
        .unwrap();
    }
}
