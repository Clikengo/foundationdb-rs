// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Tuple Key type like that of other FoundationDB libraries

mod element;

use std::{self, io::Write, string::FromUtf8Error};

pub use self::element::Element;
use subspace::Subspace;

#[derive(Clone, Debug, PartialEq)]
pub struct Tuple(pub Vec<Element>);

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Unexpected end of file")]
    EOF,
    #[fail(display = "Invalid type: {}", value)]
    InvalidType { value: u8 },
    #[fail(display = "Invalid data")]
    InvalidData,
    #[fail(display = "UTF8 conversion error")]
    FromUtf8Error(FromUtf8Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait Encode {
    fn encode<W: Write>(&self, _w: &mut W) -> std::io::Result<()>;
    fn encode_to_vec(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.encode(&mut v)
            .expect("tuple encoding should never fail");
        v
    }
}

impl<'a, E: Encode + ?Sized> From<&'a E> for Subspace {
    fn from(encode: &'a E) -> Self {
        Subspace::new(encode)
    }
}

pub trait Decode: Sized {
    fn decode(buf: &[u8]) -> Result<(Self, usize)>;
    fn decode_full(buf: &[u8]) -> Result<Self> {
        let (val, offset) = Self::decode(buf)?;
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
                fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
                    $(
                        self.$n.encode(w)?;
                    )*
                    Ok(())
                }
            }

            impl<$($name),+> Decode for ($($name,)+)
            where
                $($name: Decode + Default,)+
            {
                #[allow(non_snake_case, unused_assignments, deprecated)]
                fn decode(buf: &[u8]) -> Result<(Self, usize)> {
                    let mut buf = buf;
                    let mut out: Self = Default::default();
                    let mut offset = 0_usize;

                    $(
                        let (v0, offset0) = $name::decode(buf)?;
                        out.$n = v0;
                        offset += offset0;
                        buf = &buf[offset0..];
                    )*

                    if !buf.is_empty() {
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
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        for element in self.0.iter() {
            element.encode(w)?;
        }
        Ok(())
    }
}

impl Decode for Tuple {
    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        let mut data = buf;
        let mut v = Vec::new();
        let mut offset = 0_usize;
        while !data.is_empty() {
            let (s, len): (Element, _) = Element::decode(data)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_malformed_int() {
        assert!(Tuple::decode(&[21, 0]).is_ok());
        assert!(Tuple::decode(&[22, 0]).is_err());
        assert!(Tuple::decode(&[22, 0, 0]).is_ok());

        assert!(Tuple::decode(&[19, 0]).is_ok());
        assert!(Tuple::decode(&[18, 0]).is_err());
        assert!(Tuple::decode(&[18, 0, 0]).is_ok());
    }

    #[test]
    fn test_decode_tuple() {
        assert_eq!((0, ()), Decode::decode_full(&[20, 0]).unwrap());
    }

    #[test]
    fn test_decode_tuple_ty() {
        let data: &[u8] = &[2, 104, 101, 108, 108, 111, 0, 1, 119, 111, 114, 108, 100, 0];

        let (v1, v2): (String, Vec<u8>) = Decode::decode_full(data).unwrap();
        assert_eq!(v1, "hello");
        assert_eq!(v2, b"world");
    }

    #[test]
    fn test_encode_tuple_ty() {
        let tup = ("hello", b"world".to_vec());

        assert_eq!(
            &[2, 104, 101, 108, 108, 111, 0, 1, 119, 111, 114, 108, 100, 0],
            Encode::encode_to_vec(&tup).as_slice()
        );
    }

    #[test]
    fn test_eq() {
        assert_eq!(
            "string".encode_to_vec(),
            "string".to_string().encode_to_vec()
        );

        assert_eq!("string".encode_to_vec(), ("string",).encode_to_vec());
    }
}
