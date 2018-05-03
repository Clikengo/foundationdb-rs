// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Tuple Key type like that of other FoundationDB libraries

pub mod single;

use std::{self, io::Write, string::FromUtf8Error};
use self::single::Single;

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

type Result<T> = std::result::Result<T, Error>;

impl From<FromUtf8Error> for Error {
    fn from(error: FromUtf8Error) -> Self {
        Error::FromUtf8Error(error)
    }
}

pub trait Tuple: Sized {
    fn encode<W: Write>(&self, _w: &mut W) -> std::io::Result<()>;
    fn encode_to_vec(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.encode(&mut v)
            .expect("tuple encoding should never fail");
        v
    }

    fn decode(buf: &[u8]) -> Result<Self>;
}

macro_rules! tuple_impls {
    ($($len:expr => ($($n:tt $name:ident)+))+) => {
        $(
            impl<$($name),+> Tuple for ($($name,)+)
            where
                $($name: Single + Default,)+
            {
                #[allow(non_snake_case, unused_assignments, deprecated)]
                fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
                    $(
                        self.$n.encode(w)?;
                    )*
                    Ok(())
                }

                #[allow(non_snake_case, unused_assignments, deprecated)]
                fn decode(buf: &[u8]) -> Result<Self> {
                    let mut buf = buf;
                    let mut out: Self = Default::default();
                    $(
                        let (v0, offset0) = $name::decode(buf)?;
                        out.$n = v0;
                        buf = &buf[offset0..];
                    )*

                    if !buf.is_empty() {
                        return Err(Error::InvalidData);
                    }

                    Ok(out)
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

#[derive(Clone, Debug, PartialEq)]
pub struct Value(pub Vec<single::Value>);

impl Tuple for Value {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        for item in self.0.iter() {
            item.encode(w)?;
        }
        Ok(())
    }

    fn decode(buf: &[u8]) -> Result<Self> {
        let mut data = buf;
        let mut v = Vec::new();
        while !data.is_empty() {
            let (s, offset): (single::Value, _) = Single::decode(data)?;
            v.push(s);
            data = &data[offset..];
        }
        Ok(Value(v))
    }
}
