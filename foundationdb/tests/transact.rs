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
use futures::future::*;

mod common;

#[test]
fn test_transact_error() {
    common::setup_static();
    let fut = Cluster::new(foundationdb::default_config_path())
        .and_then(|cluster| cluster.create_database())
        .map_err(failure::Error::from)
        .and_then(|db| db.transact(|_trx| -> Result<(), failure::Error> { bail!("failed") }));

    assert!(fut.wait().is_err(), "should return error");
}
