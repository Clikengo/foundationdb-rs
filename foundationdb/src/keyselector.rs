// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

#[derive(Clone, Debug)]
pub struct KeySelector {
    //TODO: Box<[u8]>?
    //TODO: introduces BorrowedKeySelector?
    key: Vec<u8>,
    or_equal: bool,
    offset: usize,
}

impl KeySelector {
    pub fn new(key: Vec<u8>, or_equal: bool, offset: usize) -> Self {
        Self {
            key,
            or_equal,
            offset,
        }
    }

    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn or_equal(&self) -> bool {
        self.or_equal
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn last_less_than(key: &[u8]) -> Self {
        Self::new(key.to_vec(), false, 0)
    }
    pub fn last_less_or_equal(key: &[u8]) -> Self {
        Self::new(key.to_vec(), true, 0)
    }

    pub fn first_greater_than(key: &[u8]) -> Self {
        Self::new(key.to_vec(), true, 1)
    }
    pub fn first_greater_or_equal(key: &[u8]) -> Self {
        Self::new(key.to_vec(), false, 1)
    }
}
