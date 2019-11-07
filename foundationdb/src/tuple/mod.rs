//! Implementation of the official tuple layer typecodes
//!
//! The official specification can be found [here](https://github.com/apple/foundationdb/blob/master/design/tuple.md).

mod element;
mod pack;
mod subspace;
mod versionstamp;

use std::borrow::Cow;
use std::fmt::{self, Display};
use std::io;
use std::ops::Deref;
use std::result;

#[cfg(feature = "uuid")]
pub use uuid::Uuid;

pub use element::Element;
pub use pack::{TuplePack, TupleUnpack};
pub use subspace::Subspace;
pub use versionstamp::Versionstamp;

const NIL: u8 = 0x00;
const BYTES: u8 = 0x01;
const STRING: u8 = 0x02;
const NESTED: u8 = 0x05;
// const NEGINTSTART: u8 = 0x0b;
const INTZERO: u8 = 0x14;
// const POSINTEND: u8 = 0x1d;
const FLOAT: u8 = 0x20;
const DOUBLE: u8 = 0x21;
const FALSE: u8 = 0x26;
const TRUE: u8 = 0x27;
#[cfg(feature = "uuid")]
const UUID: u8 = 0x30;
// Not a single official binding is implementing 80 Bit versionstamp...
// const VERSIONSTAMP_88: u8 = 0x32;
const VERSIONSTAMP: u8 = 0x33;

const ESCAPE: u8 = 0xff;

/// Tracks the depth of a Tuple decoding chain
#[derive(Copy, Clone)]
pub struct TupleDepth(usize);

impl TupleDepth {
    fn new() -> Self {
        TupleDepth(0)
    }

    /// Increment the depth by one, this be called when calling into `Tuple::{encode, decode}` of tuple-like datastructures
    pub fn increment(self) -> Self {
        TupleDepth(self.0 + 1)
    }

    /// Returns the current depth in any recursive tuple processing, 0 representing there having been no recursion
    pub fn depth(self) -> usize {
        self.0
    }
}

/// A packing/unpacking error
#[derive(Debug)]
pub enum Error {
    Message(String),
    IoError(io::Error),
    TrailingBytes,
    MissingBytes,
    BadStringFormat,
    BadCode {
        found: u8,
        expected: Option<u8>,
    },
    BadPrefix,
    #[cfg(feature = "uuid")]
    BadUuid,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::IoError(err)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Message(s) => s.fmt(f),
            Error::IoError(err) => err.fmt(f),
            Error::TrailingBytes => write!(f, "trailing bytes"),
            Error::MissingBytes => write!(f, "missing bytes"),
            Error::BadStringFormat => write!(f, "not an utf8 string"),
            Error::BadCode { found, .. } => write!(f, "bad code, found {}", found),
            Error::BadPrefix => write!(f, "bad prefix"),
            #[cfg(feature = "uuid")]
            Error::BadUuid => write!(f, "bad uuid"),
        }
    }
}

/// Alias for `Result<..., tuple::Error>`
pub type Result<T> = result::Result<T, Error>;

/// Represent a sequence of bytes (i.e. &[u8])
///
/// This sequence can be either owned or borrowed.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bytes<'a>(pub Cow<'a, [u8]>);

impl<'a> fmt::Debug for Bytes<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl<'a> fmt::Display for Bytes<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
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
impl<'a> AsRef<[u8]> for Bytes<'a> {
    fn as_ref(&self) -> &[u8] {
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

/// Pack value and returns the packed buffer
pub fn pack<T: TuplePack>(v: &T) -> Vec<u8> {
    v.pack_to_vec()
}

/// Pack value into the given buffer
pub fn pack_into<T: TuplePack>(v: &T, output: &mut Vec<u8>) {
    v.pack_root(output)
        .expect("tuple encoding should never fail");
}

/// Unpack input
pub fn unpack<'de, T: TupleUnpack<'de>>(input: &'de [u8]) -> Result<T> {
    T::unpack_root(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NIL_VAL: Option<()> = None;

    fn test_serde<'de, T>(val: T, buf: &'de [u8])
    where
        T: TuplePack + TupleUnpack<'de> + fmt::Debug + PartialEq,
    {
        assert_eq!(unpack::<'de, T>(buf).unwrap(), val);
        assert_eq!(pack(&val), buf);
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

        test_serde(0, b"\x14");
        test_serde(1, b"\x15\x01");
        test_serde(-1, b"\x13\xfe");
        test_serde(255, b"\x15\xff");
        test_serde(-255, b"\x13\x00");
        test_serde(256, b"\x16\x01\x00");
        test_serde(-256, b"\x12\xfe\xff");
        test_serde(65536, b"\x17\x01\x00\x00");
        test_serde(-65536, b"\x11\xfe\xff\xff");
        test_serde(i64::max_value(), b"\x1C\x7f\xff\xff\xff\xff\xff\xff\xff");
        test_serde(
            i64::max_value() as u64 + 1,
            b"\x1C\x80\x00\x00\x00\x00\x00\x00\x00",
        );
        test_serde(u64::max_value(), b"\x1C\xff\xff\xff\xff\xff\xff\xff\xff");
        test_serde(-4294967295i64, b"\x10\x00\x00\x00\x00");
        test_serde(
            i64::min_value() + 2,
            b"\x0C\x80\x00\x00\x00\x00\x00\x00\x01",
        );
        test_serde(
            i64::min_value() + 1,
            b"\x0C\x80\x00\x00\x00\x00\x00\x00\x00",
        );
        test_serde(i64::min_value(), b"\x0C\x7f\xff\xff\xff\xff\xff\xff\xff");
    }

    #[cfg(feature = "uuid")]
    #[test]
    fn test_uuid() {
        use uuid::Uuid;

        test_serde(
            Element::Uuid(
                Uuid::from_slice(
                    b"\xba\xff\xff\xff\xff\x5e\xba\x11\x00\x00\x00\x00\x5c\xa1\xab\x1e",
                )
                .unwrap(),
            ),
            b"\x30\xba\xff\xff\xff\xff\x5e\xba\x11\x00\x00\x00\x00\x5c\xa1\xab\x1e",
        );
    }

    #[test]
    fn test_bindingtester() {
        test_serde("NEW_TRANSACTION".to_string(), b"\x02NEW_TRANSACTION\x00");
        test_serde(
            vec!["NEW_TRANSACTION".to_string()],
            b"\x02NEW_TRANSACTION\x00",
        );
        test_serde(
            vec![
                Element::String(Cow::Borrowed("PUSH")),
                Element::Bytes(Bytes::from(
                    b"\x01tester_output\x00\x01results\x00\x14".as_ref(),
                )),
            ],
            b"\x02PUSH\x00\x01\x01tester_output\x00\xff\x01results\x00\xff\x14\x00",
        );
        test_serde(
            vec![Element::String(Cow::Borrowed("PUSH")), Element::Nil],
            b"\x02PUSH\x00\x00",
        );
        test_serde(
            vec![
                Element::String(Cow::Borrowed("PUSH")),
                Element::Tuple(vec![
                    Element::Nil,
                    Element::Float(3299069000000.0),
                    Element::Float(-0.000000000000000000000000000000000000011883096),
                ]),
            ],
            b"\x02PUSH\x00\x05\x00\xff \xd4@\x07\xf5 \x7f~\x9a\xc2\x00",
        );
        test_serde(
            vec![
                Element::String(Cow::Borrowed("PUSH")),
                Element::Int(-133525682914243904),
            ],
            b"\x02PUSH\x00\x0c\xfe%\x9f\x19M\x81J\xbf",
        );

        test_serde(
            Element::Tuple(vec![Element::Nil, Element::Nil]),
            b"\x00\x00",
        );
    }

    #[test]
    fn test_element() {
        test_serde(Element::Bool(true), &[TRUE]);
        test_serde(Element::Bool(false), &[FALSE]);
        test_serde(Element::Int(-1), &[0x13, 254]);
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
        test_serde(
            (Element::Versionstamp(Versionstamp::complete(
                b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a".clone(),
                657,
            )),),
            b"\x33\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x02\x91",
        );
        test_serde(
            vec![Element::Bool(true), Element::Bool(false)],
            &[TRUE, FALSE],
        );
        test_serde(
            vec![Element::Tuple(vec![
                Element::Bool(true),
                Element::Bool(false),
            ])],
            &[NESTED, TRUE, FALSE, NIL],
        );
        test_serde(Vec::<Element>::new(), &[]);
        test_serde(Element::Tuple(vec![]), &[]);
    }
}
