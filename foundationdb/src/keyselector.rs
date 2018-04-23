// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

///TODO: revise
//TODO: introduces a Trait to cover both KeySelector/OwnedKeySelector?
#[derive(Clone, Debug)]
pub struct KeySelector<'a> {
    key: &'a [u8],
    or_equal: bool,
    offset: usize,
}

impl<'a> KeySelector<'a> {
    pub fn new(key: &'a [u8], or_equal: bool, offset: usize) -> Self {
        Self {
            key,
            or_equal,
            offset,
        }
    }

    pub fn key(&self) -> &[u8] {
        self.key
    }

    pub fn or_equal(&self) -> bool {
        self.or_equal
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn to_owned(&self) -> OwnedKeySelector {
        OwnedKeySelector::new(self.key.to_vec(), self.or_equal, self.offset)
    }

    pub fn last_less_than(key: &'a [u8]) -> Self {
        Self::new(key, false, 0)
    }
    pub fn last_less_or_equal(key: &'a [u8]) -> Self {
        Self::new(key, true, 0)
    }

    pub fn first_greater_than(key: &'a [u8]) -> Self {
        Self::new(key, true, 1)
    }
    pub fn first_greater_or_equal(key: &'a [u8]) -> Self {
        Self::new(key, false, 1)
    }
}

///TODO: revise
pub struct OwnedKeySelector {
    //TODO: Box<[u8]>?
    key: Vec<u8>,
    or_equal: bool,
    offset: usize,
}

impl OwnedKeySelector {
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

    //TODO: better naming
    pub(crate) fn as_selector(&self) -> KeySelector {
        KeySelector::new(self.key.as_slice(), self.or_equal, self.offset)
    }
}
