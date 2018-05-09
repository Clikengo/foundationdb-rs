// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! A `KeySelector` identifies a particular key in the database.

/// A `KeySelector` identifies a particular key in the database.
///
/// FoundationDB's lexicographically ordered data model permits finding keys based on their order (for example, finding the first key in the database greater than a given key). Key selectors represent a description of a key in the database that could be resolved to an actual key by `Transaction::get_key` or used directly as the beginning or end of a range in `Transaction::getRange`.
///
/// Note that the way the key selectors are resolved is somewhat non-intuitive, so users who wish to use a key selector other than the default ones described below should probably consult that documentation before proceeding.
///
/// Generally one of the following static methods should be used to construct a KeySelector:
///
/// - `last_less_than`
/// - `last_less_or_equal`
/// - `first_greater_than`
/// - `first_greater_or_equal`
#[derive(Clone, Debug)]
pub struct KeySelector {
    //TODO: Box<[u8]>?
    //TODO: introduces BorrowedKeySelector?
    key: Vec<u8>,
    or_equal: bool,
    offset: usize,
}

impl KeySelector {
    /// Constructs a new KeySelector from the given parameters.
    pub fn new(key: Vec<u8>, or_equal: bool, offset: usize) -> Self {
        Self {
            key,
            or_equal,
            offset,
        }
    }

    /// Returns a the key that serves as the anchor for this `KeySelector`
    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    /// True if this is an `or_equal` `KeySelector`
    pub fn or_equal(&self) -> bool {
        self.or_equal
    }

    /// Returns the key offset parameter for this `KeySelector`
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Creates a `KeySelector` that picks the last key less than the parameter
    pub fn last_less_than(key: &[u8]) -> Self {
        Self::new(key.to_vec(), false, 0)
    }

    /// Creates a `KeySelector` that picks the last key less than or equal to the parameter
    pub fn last_less_or_equal(key: &[u8]) -> Self {
        Self::new(key.to_vec(), true, 0)
    }

    /// Creates a `KeySelector` that picks the first key greater than or equal to the parameter
    pub fn first_greater_than(key: &[u8]) -> Self {
        Self::new(key.to_vec(), true, 1)
    }

    /// Creates a `KeySelector` that picks the first key greater than the parameter
    pub fn first_greater_or_equal(key: &[u8]) -> Self {
        Self::new(key.to_vec(), false, 1)
    }
}
