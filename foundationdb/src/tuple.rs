// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Tuple Key type like that of other FoundationDB libraries

use byteorder;
use byteorder::ByteOrder;
use uuid::Uuid;

const SIZE_LIMITS: &[u64] = &[
    1,
    1 << (1 * 8) - 1,
    1 << (2 * 8) - 1,
    1 << (3 * 8) - 1,
    1 << (4 * 8) - 1,
    1 << (5 * 8) - 1,
    1 << (6 * 8) - 1,
    1 << (7 * 8) - 1,
    1 << (8 * 8) - 1,
];

/// Various tuple types
enum SingleType {
    Bytes,
    String,
    Nested,
    IntZero,
    PosIntEnd,
    NegIntStart,
    Float,
    Double,
    False,
    True,
    Uuid,
    VersionStamp,
}

impl SingleType {
    /// Prefix code for encoded tuple singles
    fn to_code(&self) -> u8 {
        match *self {
            SingleType::Bytes => 0x01,
            SingleType::String => 0x02,
            SingleType::Nested => 0x05,
            SingleType::IntZero => 0x14,
            SingleType::PosIntEnd => 0x1d,
            SingleType::NegIntStart => 0x0b,
            SingleType::Float => 0x20,
            SingleType::Double => 0x21,
            SingleType::False => 0x26,
            SingleType::True => 0x27,
            SingleType::Uuid => 0x30,
            SingleType::VersionStamp => 0x33,
        }
    }
}

enum Single {
    Bytes(Box<[u8]>),
    String(String),
    Nested(Box<Tuple>),
    BigInteger(u64), // need real BigInteger here
    Float(f32),
    Double(f64),
    Boolean(bool),
    Uuid(Uuid),
    Versionstamp(Box<[u8; 12]>),
}

/// A Tuple is encoded data for
struct Tuple(Vec<Single>);

impl Tuple {
    fn add(&mut self, single: Single) {
        self.0.push(single);
    }
}

trait ToTupleBytes {
    fn to_bytes(self) -> Box<[u8]>;
}

impl ToTupleBytes for u64 {
    fn to_bytes(self) -> Box<[u8]> {
        let mut bytes = [0u8; 4];
        byteorder::BE::write_u64(&mut bytes, self);
        Box::new(bytes)
    }
}
