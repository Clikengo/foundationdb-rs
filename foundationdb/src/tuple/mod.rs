// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Tuple Key type like that of other FoundationDB libraries

mod element;

use std::ops::{Deref, DerefMut};
use std::{self, io::Write, string::FromUtf8Error};

#[cfg(feature = "uuid")]
use uuid;

pub use self::element::Element;
use self::element::Type;

/// Tuple encoding/decoding related errors
#[derive(Debug, Fail)]
pub enum Error {
    /// Unexpected end of the byte stream
    #[fail(display = "Unexpected end of file")]
    EOF,
    /// Invalid type specified
    #[fail(display = "Invalid type: {}", value)]
    InvalidType {
        /// the type code as defined in FoundationDB
        value: u8,
    },
    /// Data was not valid for the specified type
    #[fail(display = "Invalid data")]
    InvalidData,
    /// Utf8 Conversion error of tuple data
    #[fail(display = "UTF8 conversion error")]
    FromUtf8Error(FromUtf8Error),
    /// Uuid Conversion error of tuple data
    #[cfg(feature = "uuid")]
    #[fail(display = "UUID conversion error")]
    FromUuidError(uuid::BytesError),
}

/// Tracks the depth of a Tuple decoding chain
#[derive(Copy, Clone)]
pub struct TupleDepth(usize);

impl TupleDepth {
    fn new() -> Self {
        TupleDepth(0)
    }

    /// Increment the depth by one, this be called when calling into `Tuple::{encode, decode}` of tuple-like datastructures
    pub fn increment(&self) -> Self {
        TupleDepth(self.0 + 1)
    }

    /// Returns the current depth in any recursive tuple processing, 0 representing there having been no recursion
    pub fn depth(&self) -> usize {
        self.0
    }
}

/// A result with tuple::Error defined
pub type Result<T> = std::result::Result<T, Error>;

/// Generic Tuple of elements
#[derive(Clone, Debug, PartialEq)]
pub struct Tuple(Vec<Element>);

impl From<Vec<Element>> for Tuple {
    fn from(tuple: Vec<Element>) -> Self {
        Tuple(tuple)
    }
}

impl Deref for Tuple {
    type Target = Vec<Element>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Tuple {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// For types that are encodable as defined by the tuple definitions on FoundationDB
pub trait Encode {
    /// Encodes this tuple/element into the associated Write
    ///
    /// # Arguments
    ///
    /// * `w` - a Write to put the self as FoundationDB encoded bytes
    /// * `tuple_depth` - the current depth in recursive tuple-like datastructures, it should be incremented only when encoding a tuple inside another object, see `encode_to` for a method which can initialize the TupleDepth.
    fn encode<W: Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> std::io::Result<()>;

    /// Encodes this tuple/element into the associated Write
    ///
    /// # Arguments
    ///
    /// * `w` - a Write to put the self as FoundationDB encoded bytes
    fn encode_to<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        self.encode(w, TupleDepth::new())
    }

    /// Encodes this tuple/element into a new Vec
    fn to_vec(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.encode_to(&mut v)
            .expect("tuple encoding should never fail");
        v
    }
}

/// For types that are decodable from the Tuple definitions in FoundationDB
pub trait Decode: Sized {
    /// Decodes Self from the byte slice
    ///
    /// # Arguments
    ///
    /// * `buf` - the buffer, starting at 0, from which to read Self, where the encoding is FoundationDB encoded bytes
    /// * `tuple_depth` - the current depth in recursive tuple-like datastructures
    ///
    /// # Return
    ///
    /// Self and the offset of the next byte after Self in the byte slice
    fn decode(buf: &[u8], tuple_depth: TupleDepth) -> Result<(Self, usize)>;

    /// Decodes Self from the byte slice
    ///
    /// # Arguments
    ///
    /// * `buf` - the buffer, starting at 0, from which to read Self, where the encoding is FoundationDB encoded bytes
    ///
    /// # Return
    ///
    /// Self and the offset of the next byte after Self in the byte slice
    fn decode_from(buf: &[u8]) -> Result<(Self, usize)> {
        Self::decode(buf, TupleDepth::new())
    }

    /// Decodes returning Self only
    fn try_from(buf: &[u8]) -> Result<Self> {
        let (val, offset) = Self::decode_from(buf)?;
        if offset != buf.len() {
            return Err(Error::InvalidData);
        }
        Ok(val)
    }
}

macro_rules! tuple_impls {
    ($($len:expr => ($($n:tt $name:ident)+))+) => {
        $(
            impl<$($name),+> Encode for ($($name,)+)
            where
                $($name: Encode,)+
            {
                #[allow(non_snake_case, unused_assignments, deprecated)]
                fn encode<W: Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> std::io::Result<()> {
                    if tuple_depth.depth() > 0 {
                        element::NESTED.write(w)?;
                    }

                    $(
                        self.$n.encode(w, tuple_depth.increment())?;
                    )*

                    if tuple_depth.depth() > 0 {
                        element::NIL.write(w)?;
                    }
                    Ok(())
                }
            }

            impl<$($name),+> Decode for ($($name,)+)
            where
                $($name: Decode + Default,)+
            {
                #[allow(non_snake_case, unused_assignments, deprecated)]
                fn decode(buf: &[u8], tuple_depth: TupleDepth) -> Result<(Self, usize)> {
                    let mut buf = buf;
                    let mut out: Self = Default::default();
                    let mut offset = 0_usize;

                    if tuple_depth.depth() > 0{
                        element::NESTED.expect(buf[0])?;
                        offset += 1;
                        buf = &buf[1..];
                    }

                    $(
                        let (v0, offset0) = $name::decode(buf, tuple_depth.increment())?;
                        out.$n = v0;
                        offset += offset0;
                        buf = &buf[offset0..];
                    )*

                    if tuple_depth.depth() > 0 {
                        element::NIL.expect(buf[0])?;
                        offset += 1;
                        buf = &buf[1..];
                    }

                    // will not be empty if we're decoding as a tuple in a tuple
                    //   (the outer tuple has more...)
                    if !buf.is_empty() && tuple_depth.depth() == 0 {
                        return Err(Error::InvalidData);
                    }

                    Ok((out, offset))
                }
            }
        )+
    }
}

tuple_impls! {
    1 => (0 T0)
    2 => (0 T0 1 T1)
    3 => (0 T0 1 T1 2 T2)
    4 => (0 T0 1 T1 2 T2 3 T3)
    5 => (0 T0 1 T1 2 T2 3 T3 4 T4)
    6 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5)
    7 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6)
    8 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7)
    9 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8)
    10 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8 9 T9)
    11 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8 9 T9 10 T10)
    12 => (0 T0 1 T1 2 T2 3 T3 4 T4 5 T5 6 T6 7 T7 8 T8 9 T9 10 T10 11 T11)
}

impl Encode for Tuple {
    fn encode<W: Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> std::io::Result<()> {
        for element in self.0.iter() {
            element.encode(w, tuple_depth.increment())?;
        }
        Ok(())
    }
}

impl Decode for Tuple {
    fn decode(buf: &[u8], tuple_depth: TupleDepth) -> Result<(Self, usize)> {
        let mut data = buf;
        let mut v = Vec::new();
        let mut offset = 0_usize;
        while !data.is_empty() {
            let (s, len): (Element, _) = Element::decode(data, tuple_depth.increment())?;
            v.push(s);
            offset += len;
            data = &data[len..];
        }
        Ok((Tuple(v), offset))
    }
}

impl From<FromUtf8Error> for Error {
    fn from(error: FromUtf8Error) -> Self {
        Error::FromUtf8Error(error)
    }
}

#[cfg(feature = "uuid")]
impl From<uuid::BytesError> for Error {
    fn from(error: uuid::BytesError) -> Self {
        Error::FromUuidError(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_malformed_int() {
        assert!(Tuple::try_from(&[21, 0]).is_ok());
        assert!(Tuple::try_from(&[22, 0]).is_err());
        assert!(Tuple::try_from(&[22, 0, 0]).is_ok());

        assert!(Tuple::try_from(&[19, 0]).is_ok());
        assert!(Tuple::try_from(&[18, 0]).is_err());
        assert!(Tuple::try_from(&[18, 0, 0]).is_ok());
    }

    #[test]
    fn test_decode_tuple() {
        assert_eq!((0, ()), Decode::try_from(&[20, 0]).unwrap());
    }

    #[test]
    fn test_decode_tuple_ty() {
        let data: &[u8] = &[2, 104, 101, 108, 108, 111, 0, 1, 119, 111, 114, 108, 100, 0];

        let (v1, v2): (String, Vec<u8>) = Decode::try_from(data).unwrap();
        assert_eq!(v1, "hello");
        assert_eq!(v2, b"world");
    }

    #[test]
    fn test_encode_tuple_ty() {
        let tup = ("hello", b"world".to_vec());

        assert_eq!(
            &[2, 104, 101, 108, 108, 111, 0, 1, 119, 111, 114, 108, 100, 0],
            Encode::to_vec(&tup).as_slice()
        );
    }

    #[test]
    fn test_eq() {
        assert_eq!("string".to_vec(), "string".to_string().to_vec());

        assert_eq!(
            ("string", "string".to_string()).to_vec(),
            ("string".to_string(), "string").to_vec()
        );

        assert_eq!("string".to_vec(), ("string",).to_vec());
    }

    #[test]
    fn test_encode_recursive_tuple() {
        assert_eq!(
            &("one", ("two", 42)).to_vec(),
            &[2, 111, 110, 101, 0, 5, 2, 116, 119, 111, 0, 21, 42, 0]
        );
        assert_eq!(
            &("one", ("two", 42, ("three", 33))).to_vec(),
            &[
                2, 111, 110, 101, 0, 5, 2, 116, 119, 111, 0, 21, 42, 5, 2, 116, 104, 114, 101, 101,
                0, 21, 33, 0, 0,
            ]
        );

        //
        // from Python impl:
        //  >>> [ord(x) for x in fdb.tuple.pack( (None, (None, None)) )]
        assert_eq!(
            &(None::<i64>, (None::<i64>, None::<i64>)).to_vec(),
            &[0, 5, 0, 255, 0, 255, 0]
        )
    }

    #[test]
    fn test_decode_recursive_tuple() {
        let two_decode = <(String, (String, i64))>::try_from(&[
            2, 111, 110, 101, 0, 5, 2, 116, 119, 111, 0, 21, 42, 0,
        ]).expect("failed two");

        // TODO: can we get eq for borrows of the inner types?
        assert_eq!(("one".to_string(), ("two".to_string(), 42)), two_decode);

        let three_decode = <(String, (String, i64, (String, i64)))>::try_from(&[
            2, 111, 110, 101, 0, 5, 2, 116, 119, 111, 0, 21, 42, 5, 2, 116, 104, 114, 101, 101, 0,
            21, 33, 0, 0,
        ]).expect("failed three");

        assert_eq!(
            &(
                "one".to_string(),
                ("two".to_string(), 42, ("three".to_string(), 33))
            ),
            &three_decode
        );

        //
        // from Python impl:
        //  >>> [ord(x) for x in fdb.tuple.pack( (None, (None, None)) )]
        let option_decode =
            <(Option<i64>, (Option<i64>, Option<i64>))>::try_from(&[0, 5, 0, 255, 0, 255, 0])
                .expect("failed option");

        assert_eq!(&(None::<i64>, (None::<i64>, None::<i64>)), &option_decode)
    }

    #[test]
    fn test_single_tuple_encode() {
        // derived from python impl
        assert_eq!(&(1,).to_vec(), &1.to_vec());

        // >>> [ord(x) for x in fdb.tuple.pack((1,(1,)))]
        // [21, 1, 5, 21, 1, 0]
        assert_eq!(&(1, (1,)).to_vec(), &[21, 1, 5, 21, 1, 0]);

        // >>> [ord(x) for x in fdb.tuple.pack( (1,(1,(1,))) )]
        // [21, 1, 5, 21, 1, 5, 21, 1, 0, 0]
        assert_eq!(&(1, (1, (1,))).to_vec(), &[21, 1, 5, 21, 1, 5, 21, 1, 0, 0]);

        // >>> [ord(x) for x in fdb.tuple.pack( (1,(1,),(1,)) )]
        // [21, 1, 5, 21, 1, 0, 5, 21, 1, 0]
        assert_eq!(
            &(1, (1,), (1,)).to_vec(),
            &[21, 1, 5, 21, 1, 0, 5, 21, 1, 0]
        );
    }

    #[test]
    fn test_single_tuple_decode() {
        // derived from python impl
        assert_eq!(1_i64, Decode::try_from(&[21, 1]).expect("1"));
        assert_eq!((1_i64,), Decode::try_from(&[21, 1]).expect("(1,)"));

        // >>> [ord(x) for x in fdb.tuple.pack((1,(1,)))]
        // [21, 1, 5, 21, 1, 0]
        assert_eq!(
            (1_i64, (1_i64,)),
            Decode::try_from(&[21, 1, 5, 21, 1, 0]).expect("(1, (1,))")
        );

        // >>> [ord(x) for x in fdb.tuple.pack( (1,(1,(1,))) )]
        // [21, 1, 5, 21, 1, 5, 21, 1, 0, 0]
        assert_eq!(
            (1_i64, (1_i64, (1_i64,))),
            Decode::try_from(&[21, 1, 5, 21, 1, 5, 21, 1, 0, 0]).expect("(1, (1, (1,)))")
        );

        // >>> [ord(x) for x in fdb.tuple.pack( (1,(1,),(1,)) )]
        // [21, 1, 5, 21, 1, 0, 5, 21, 1, 0]
        assert_eq!(
            (1_i64, (1_i64,), (1_i64,)),
            Decode::try_from(&[21, 1, 5, 21, 1, 0, 5, 21, 1, 0]).expect("(1, (1,), (1,))")
        );
    }
}
