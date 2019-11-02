pub mod de;
mod element;
pub mod ser;
mod subspace;
mod versionstamp;

use std::borrow::Cow;
use std::fmt::{self, Display};
use std::io;
use std::ops::Deref;
use std::result;

pub use element::Element;
pub use subspace::Subspace;
pub use versionstamp::Versionstamp;

const NIL: u8 = 0x00;
const BYTES: u8 = 0x01;
const STRING: u8 = 0x02;
const NESTED: u8 = 0x05;
const INTZERO: u8 = 0x14;
// TODO const POSINTEND: u8 = 0x1d;
// TODO const NEGINTSTART: u8 = 0x0b;
const FLOAT: u8 = 0x20;
const DOUBLE: u8 = 0x21;
const FALSE: u8 = 0x26;
const TRUE: u8 = 0x27;
#[cfg(feature = "uuid")]
const UUID: u8 = 0x30;
// Not a single official binding is implementing 80 Bit versionstamp...
// const VERSIONSTAMP_88: u8 = 0x32;
const VERSIONSTAMP: u8 = 0x33;

const ENUM: u8 = 0x40;

const ESCAPE: u8 = 0xff;

#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    Message(String),
    NotSupported(&'static str),
    IoError,
    TrailingBytes,
    MissingBytes,
    BadStringFormat,
    BadSeqFormat,
    BadCharValue(u32),
    BadCode { found: u8, expected: Option<u8> },
    BadPrefix,
    BadVersionstamp,
}

impl From<io::Error> for Error {
    fn from(_: io::Error) -> Self {
        Error::IoError
    }
}

impl serde::ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl serde::de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(std::error::Error::description(self))
    }
}

impl std::error::Error for Error {}

pub type Result<T> = result::Result<T, Error>;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bytes<'a>(pub Cow<'a, [u8]>);

impl<'a> std::fmt::Debug for Bytes<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "b\"")?;
        for &byte in self.0.iter() {
            if byte.is_ascii_alphanumeric() || byte.is_ascii_punctuation() || byte == b' ' {
                write!(fmt, "{}", byte as char)?;
            } else {
                write!(fmt, "\\x{:02x}", byte)?;
            }
        }
        write!(fmt, "\"")
    }
}

impl<'a> Bytes<'a> {
    pub fn into_owned(self) -> Vec<u8> {
        self.0.into_owned()
    }
}

impl<'a> Deref for Bytes<'a> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> From<&'a [u8]> for Bytes<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        Self(Cow::Borrowed(bytes))
    }
}
impl From<Vec<u8>> for Bytes<'static> {
    fn from(vec: Vec<u8>) -> Self {
        Self(Cow::Owned(vec))
    }
}

impl<'a> From<&'a str> for Bytes<'a> {
    fn from(s: &'a str) -> Self {
        s.as_bytes().into()
    }
}
impl From<String> for Bytes<'static> {
    fn from(vec: String) -> Self {
        vec.into_bytes().into()
    }
}

impl<'a> serde::Serialize for Bytes<'a> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

struct BytesVisitor;

impl<'a> serde::de::Visitor<'a> for BytesVisitor {
    type Value = Bytes<'a>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a borrowed byte array")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Bytes(Cow::Owned(v.to_owned())))
    }

    fn visit_borrowed_bytes<E>(self, v: &'a [u8]) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Bytes(Cow::Borrowed(v)))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Bytes(Cow::Owned(v)))
    }
}

impl<'de: 'a, 'a> serde::Deserialize<'de> for Bytes<'a> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Bytes<'a>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(BytesVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    const NIL_VAL: Option<()> = None;

    fn test_serde<'de, T>(val: T, buf: &'de [u8])
    where
        T: Serialize + Deserialize<'de> + std::fmt::Debug + PartialEq,
    {
        assert_eq!(ser::to_bytes(&val).unwrap(), buf);
        assert_eq!(de::from_bytes::<'de, T>(buf).unwrap(), val);
    }

    #[test]
    fn test_spec() {
        test_serde(NIL_VAL, &[NIL]);
        test_serde((NIL_VAL,), &[NIL]);
        test_serde(((NIL_VAL,),), &[NESTED, NIL, ESCAPE, NIL]);
        // assert_eq!(to_bytes(b"foo\x00bar").unwrap(), b"\x01foo\x00\xffbar\x00");
        test_serde("FÔO\x00bar".to_owned(), b"\x02F\xc3\x94O\x00\xffbar\x00");
        test_serde(
            (("foo\x00bar".to_owned(), NIL_VAL, ()),),
            b"\x05\x02foo\x00\xffbar\x00\x00\xff\x05\x00\x00",
        );
        test_serde(-1, b"\x13\xfe");
        test_serde(-5551212, b"\x11\xabK\x93");
        test_serde(-42f32, b"\x20\x3d\xd7\xff\xff");
    }

    #[test]
    fn test_simple() {
        // bool
        test_serde(false, &[FALSE]);
        test_serde(true, &[TRUE]);

        // int
        test_serde(0i64, &[INTZERO]);
        test_serde(1i64, &[0x15, 1]);
        test_serde(-1i64, &[0x13, 254]);
        test_serde(100i64, &[21, 100]);

        test_serde(10000i32, &[22, 39, 16]);
        test_serde(-100i16, &[19, 155]);
        test_serde(-10000i64, &[18, 216, 239]);
        test_serde(-1000000i64, &[17, 240, 189, 191]);

        // boundary condition
        test_serde(255u16, &[0x15, 255]);
        test_serde(256i32, &[0x16, 1, 0]);
        test_serde(-255i16, &[0x13, 0]);
        test_serde(-256i64, &[0x12, 254, 255]);

        // versionstamp
        test_serde(
            Versionstamp::complete(b"\xaa\xbb\xcc\xdd\xee\xff\x00\x01\x02\x03".clone(), 0),
            b"\x33\xaa\xbb\xcc\xdd\xee\xff\x00\x01\x02\x03\x00\x00",
        );
        test_serde(
            Versionstamp::complete(b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a".clone(), 657),
            b"\x33\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x02\x91",
        );
        test_serde(
            Element::Versionstamp(Versionstamp::complete(
                b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a".clone(),
                657,
            )),
            b"\x33\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x02\x91",
        );
        test_serde(
            (Element::Versionstamp(Versionstamp::complete(
                b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a".clone(),
                657,
            )),),
            b"\x33\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x02\x91",
        );
    }

    #[test]
    fn test_bindingtester() {
        test_serde("NEW_TRANSACTION".to_string(), b"\x02NEW_TRANSACTION\x00");
        test_serde(
            vec!["NEW_TRANSACTION".to_string()],
            b"\x02NEW_TRANSACTION\x00",
        );
    }
}
