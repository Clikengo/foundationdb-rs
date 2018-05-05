// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! subspace provides a convenient way to use FoundationDB tuples to define namespaces for
//! different categories of data. The namespace is specified by a prefix tuple which is prepended
//! to all tuples packed by the subspace. When unpacking a key with the subspace, the prefix tuple
//! will be removed from the result.
//!
//! As a best practice, API clients should use at least one subspace for application data. For
//! general guidance on subspace usage, see the Subspaces section of the Developer Guide
//! (https://apple.github.io/foundationdb/developer-guide.html#subspaces).

use tuple::{Decode, Encode, Result, Error};

/// Subspace represents a well-defined region of keyspace in a FoundationDB database.
#[derive(Debug, Clone)]
pub struct Subspace {
    prefix: Vec<u8>,
}

impl Subspace {
    /// `all` returns the Subspace corresponding to all keys in a FoundationDB database.
    pub fn all() -> Subspace {
        Self { prefix: Vec::new() }
    }

    /// `from_bytes` returns a new Subspace from the provided bytes.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            prefix: bytes.to_vec(),
        }
    }

    /// Returns a new Subspace from the provided tuple encodable.
    pub fn new<T: Encode>(t: &T) -> Self {
        let prefix = Encode::encode_to_vec(t);
        Self { prefix }
    }

    /// Returns a new Subspace whose prefix extends this Subspace with a given tuple encodable.
    pub fn subspace<T: Encode>(&self, t: &T) -> Self {
        Self {
            prefix: self.pack(t),
        }
    }

    /// `bytes` returns the literal bytes of the prefix of this Subspace.
    pub fn bytes(&self) -> &[u8] {
        self.prefix.as_slice()
    }

    /// Returns the key encoding the specified Tuple with the prefix of this Subspace
    /// prepended.
    pub fn pack<T: Encode>(&self, t: &T) -> Vec<u8> {
        let mut packed = Encode::encode_to_vec(t);
        let mut out = Vec::with_capacity(self.prefix.len() + packed.len());
        out.extend_from_slice(&self.prefix);
        out.append(&mut packed);
        out
    }

    /// `unpack` returns the Tuple encoded by the given key with the prefix of this Subspace
    /// removed.  `unpack` will return an error if the key is not in this Subspace or does not
    /// encode a well-formed Tuple.
    pub fn unpack<T: Decode>(&self, key: &[u8]) -> Result<T> {
        if !self.is_start_of(key) {
            return Err(Error::InvalidData);
        }
        let key = &key[self.prefix.len()..];
        Decode::decode_full(&key)
    }

    /// `is_start_of` returns true if the provided key starts with the prefix of this Subspace,
    /// indicating that the Subspace logically contains the key.
    pub fn is_start_of(&self, key: &[u8]) -> bool {
        key.starts_with(&self.prefix)
    }

    /// `range` returns first and last key of given Subspace
    pub fn range(&self) -> (Vec<u8>, Vec<u8>) {
        let mut begin = self.prefix.clone();
        begin.push(0x00);

        let mut end = self.prefix.clone();
        end.push(0xff);

        (begin, end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tuple::Tuple;

    #[test]
    fn sub() {
        let ss0 = Subspace::new(&(1,));
        let ss1 = ss0.subspace(&(2,));

        let ss2 = Subspace::new(&(1, 2));

        assert_eq!(ss1.bytes(), ss2.bytes());
    }

    #[test]
    fn pack_unpack() {
        let ss0 = Subspace::new(&(1,));
        let tup = (2, 3);

        let packed = ss0.pack(&tup);
        let expected = Encode::encode_to_vec(&(1, 2, 3));
        assert_eq!(expected, packed);

        let tup_unpack: (i64, i64) = ss0.unpack(&packed).unwrap();
        assert_eq!(tup, tup_unpack);

        assert!(ss0.unpack::<(i64, i64, i64)>(&packed).is_err());
    }

    #[test]
    fn is_start_of() {
        let ss0 = Subspace::new(&(1,));
        let ss1 = Subspace::new(&(2,));
        let tup = (2, 3);

        assert!(ss0.is_start_of(&ss0.pack(&tup)));
        assert!(!ss1.is_start_of(&ss0.pack(&tup)));
    }

    #[test]
    fn unpack_malformed() {
        let ss0 = Subspace::new(&((),));

        let malformed = {
            let mut v = ss0.bytes().to_vec();
            v.push(0xff);
            v
        };

        assert!(ss0.unpack::<Tuple>(&malformed).is_err());
    }

    #[test]
    fn range() {
        let ss = Subspace::new(&(1,));
        let tup = (2, 3);
        let packed = ss.pack(&tup);

        let (begin, end) = ss.range();
        assert!(packed >= begin && packed <= end);
    }
}
