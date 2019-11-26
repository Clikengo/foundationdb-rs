use std::{self, io::Write};
#[cfg(feature = "uuid")]
use uuid::Uuid;

use byteorder::{self, ByteOrder};
use tuple::{Decode, Encode, Error, Result, Tuple, TupleDepth};

/// Various tuple types
pub(super) const NIL: u8 = 0x00;
const BYTES: u8 = 0x01;
const STRING: u8 = 0x02;
pub(super) const NESTED: u8 = 0x05;
const INTZERO: u8 = 0x14;
const POSINTEND: u8 = 0x1d;
const NEGINTSTART: u8 = 0x0b;
const FLOAT: u8 = 0x20;
const DOUBLE: u8 = 0x21;
const FALSE: u8 = 0x26;
const TRUE: u8 = 0x27;
#[cfg(feature = "uuid")]
const UUID: u8 = 0x30;
const VERSIONSTAMP: u8 = 0x33;

pub(super) const ESCAPE: u8 = 0xff;

const SIZE_LIMITS: &[u64] = &[
    0,
    (1 << (1 * 8)) - 1,
    (1 << (2 * 8)) - 1,
    (1 << (3 * 8)) - 1,
    (1 << (4 * 8)) - 1,
    (1 << (5 * 8)) - 1,
    (1 << (6 * 8)) - 1,
    (1 << (7 * 8)) - 1,
    u64::max_value(),
];

/// A single tuple element
#[derive(Clone, Debug, PartialEq)]
pub enum Element {
    /// Corresponse with nothing, ie the Nil byte
    Empty,
    /// A sequence of bytes to be written to the stream
    Bytes(Vec<u8>),
    /// A string
    String(String),
    /// A recursive Tuple
    Tuple(Tuple),
    /// An i64
    I64(i64),
    /// An f32
    F32(f32),
    /// An f64
    F64(f64),
    /// A bool
    Bool(bool),
    /// A UUID, requires the uuid feature/library
    #[cfg(feature = "uuid")]
    Uuid(Uuid),
    #[doc(hidden)]
    __Nonexhaustive,
}

pub(super) trait Type: Copy {
    /// verifies the value matches this type
    fn expect(self, value: u8) -> Result<()>;

    /// Validates this is a known type
    fn is_valid(self) -> Result<()>;

    /// writes this to w
    fn write<W: Write>(self, w: &mut W) -> std::io::Result<()>;
}

fn encode_bytes<W: Write>(w: &mut W, buf: &[u8]) -> std::io::Result<()> {
    for b in buf {
        b.write(w)?;
        if *b == 0 {
            ESCAPE.write(w)?;
        }
    }
    NIL.write(w)
}

fn decode_bytes(buf: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut out = Vec::<u8>::with_capacity(buf.len());
    let mut offset = 0;
    loop {
        if offset >= buf.len() {
            return Err(Error::EOF);
        }

        // is the null marker at the offset
        if NIL.expect(buf[offset]).is_ok() {
            if offset + 1 < buf.len() && buf[offset + 1] == ESCAPE {
                out.push(NIL);
                offset += 2;
                continue;
            } else {
                break;
            }
        }
        out.push(buf[offset]);
        offset += 1;
    }
    Ok((out, offset + 1))
}

fn adjust_float_bytes(b: &mut [u8], encode: bool) {
    if (encode && b[0] & 0x80 != 0x00) || (!encode && b[0] & 0x80 == 0x00) {
        // Negative numbers: flip all of the bytes.
        for byte in b.iter_mut() {
            *byte = *byte ^ 0xff
        }
    } else {
        // Positive number: flip just the sign bit.
        b[0] = b[0] ^ 0x80
    }
}

fn bisect_left(val: u64) -> usize {
    SIZE_LIMITS.iter().position(|v| val <= *v).unwrap_or(8)
}

impl Type for u8 {
    /// verifies the value matches this type
    fn expect(self, value: u8) -> Result<()> {
        if self == value {
            Ok(())
        } else {
            Err(Error::InvalidType { value })
        }
    }

    /// Validates this is a known type
    fn is_valid(self) -> Result<()> {
        match self {
            NIL => Ok(()),
            BYTES => Ok(()),
            STRING => Ok(()),
            NESTED => Ok(()),
            INTZERO => Ok(()),
            POSINTEND => Ok(()),
            NEGINTSTART => Ok(()),
            FLOAT => Ok(()),
            DOUBLE => Ok(()),
            FALSE => Ok(()),
            TRUE => Ok(()),
            #[cfg(feature = "uuid")]
            UUID => Ok(()),
            VERSIONSTAMP => Ok(()),
            _ => Err(Error::InvalidType { value: self }),
        }
    }

    fn write<W: Write>(self, w: &mut W) -> std::io::Result<()> {
        w.write_all(&[self])
    }
}

impl<'a, T: Encode> Encode for &'a T {
    fn encode<W: Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> std::io::Result<()> {
        T::encode(self, w, tuple_depth)
    }
}

impl Encode for bool {
    fn encode<W: Write>(&self, w: &mut W, _tuple_depth: TupleDepth) -> std::io::Result<()> {
        if *self {
            TRUE.write(w)
        } else {
            FALSE.write(w)
        }
    }
}

impl Decode for bool {
    fn decode(buf: &[u8], _tuple_depth: TupleDepth) -> Result<(Self, usize)> {
        if buf.is_empty() {
            return Err(Error::EOF);
        }

        match buf[0] {
            FALSE => Ok((false, 1)),
            TRUE => Ok((true, 1)),
            v => Err(Error::InvalidType { value: v }),
        }
    }
}

impl Encode for () {
    fn encode<W: Write>(&self, w: &mut W, _tuple_depth: TupleDepth) -> std::io::Result<()> {
        NIL.write(w)
    }
}

impl Decode for () {
    fn decode(buf: &[u8], _tuple_depth: TupleDepth) -> Result<(Self, usize)> {
        if buf.is_empty() {
            return Err(Error::EOF);
        }

        NIL.expect(buf[0])?;
        Ok(((), 1))
    }
}

#[cfg(feature = "uuid")]
impl Encode for Uuid {
    fn encode<W: Write>(&self, w: &mut W, _tuple_depth: TupleDepth) -> std::io::Result<()> {
        UUID.write(w)?;
        w.write_all(self.as_bytes())
    }
}

#[cfg(feature = "uuid")]
impl Decode for Uuid {
    fn decode(buf: &[u8], _tuple_depth: TupleDepth) -> Result<(Self, usize)> {
        if buf.len() < 17 {
            return Err(Error::EOF);
        }

        UUID.expect(buf[0])?;

        let mut uuid = [0u8; 16];
        uuid.copy_from_slice(&buf[1..17]);

        Ok((Uuid::from_slice(&uuid)?, 17))
    }
}

impl<'a> Encode for &'a str {
    fn encode<W: Write>(&self, w: &mut W, _tuple_depth: TupleDepth) -> std::io::Result<()> {
        STRING.write(w)?;
        encode_bytes(w, self.as_bytes())
    }
}

impl Encode for String {
    fn encode<W: Write>(&self, w: &mut W, _tuple_depth: TupleDepth) -> std::io::Result<()> {
        STRING.write(w)?;
        encode_bytes(w, self.as_bytes())
    }
}

impl Decode for String {
    fn decode(buf: &[u8], _tuple_depth: TupleDepth) -> Result<(Self, usize)> {
        if buf.len() < 2 {
            return Err(Error::EOF);
        }

        STRING.expect(buf[0])?;

        let (bytes, offset) = decode_bytes(&buf[1..])?;
        Ok((String::from_utf8(bytes)?, offset + 1))
    }
}

impl Encode for Vec<Element> {
    fn encode<W: Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> std::io::Result<()> {
        // TODO: should this only write the Nested markers in the case of tuple_depth?
        NESTED.write(w)?;
        for v in self {
            match v {
                &Element::Empty => {
                    // Empty value in nested tuple is encoded with [NIL, ESCAPE] to disambiguate
                    // itself with end-of-tuple marker.
                    NIL.write(w)?;
                    ESCAPE.write(w)?;
                }
                v => {
                    v.encode(w, tuple_depth.increment())?;
                }
            }
        }
        NIL.write(w)
    }
}

impl Decode for Vec<Element> {
    fn decode(mut buf: &[u8], tuple_depth: TupleDepth) -> Result<(Self, usize)> {
        if buf.len() < 2 {
            return Err(Error::EOF);
        }

        // TODO: should this only write the Nested markers in the case of tuple_depth?
        NESTED.expect(buf[0])?;
        let len = buf.len();
        buf = &buf[1..];

        let mut tuples = Vec::new();
        loop {
            if buf.is_empty() {
                // tuple must end with NIL byte
                return Err(Error::EOF);
            }

            if buf[0] == NIL {
                if buf.len() > 1 && buf[1] == ESCAPE {
                    // nested Empty value, which is encoded to [NIL, ESCAPE]
                    tuples.push(Element::Empty);
                    buf = &buf[2..];
                    continue;
                }

                buf = &buf[1..];
                break;
            }

            let (tuple, offset) = Element::decode(buf, tuple_depth.increment())?;
            tuples.push(tuple);
            buf = &buf[offset..];
        }

        // skip the final null
        Ok((tuples, len - buf.len()))
    }
}

impl<'a> Encode for &'a [u8] {
    fn encode<W: Write>(&self, w: &mut W, _tuple_depth: TupleDepth) -> std::io::Result<()> {
        BYTES.write(w)?;
        encode_bytes(w, self)
    }
}

impl Encode for Vec<u8> {
    fn encode<W: Write>(&self, w: &mut W, _tuple_depth: TupleDepth) -> std::io::Result<()> {
        BYTES.write(w)?;
        encode_bytes(w, self.as_slice())
    }
}

impl Decode for Vec<u8> {
    fn decode(buf: &[u8], _tuple_depth: TupleDepth) -> Result<(Self, usize)> {
        if buf.len() < 2 {
            return Err(Error::EOF);
        }

        BYTES.expect(buf[0])?;

        let (bytes, offset) = decode_bytes(&buf[1..])?;
        Ok((bytes, offset + 1))
    }
}

impl Encode for f32 {
    fn encode<W: Write>(&self, w: &mut W, _tuple_depth: TupleDepth) -> std::io::Result<()> {
        FLOAT.write(w)?;

        let mut buf: [u8; 4] = Default::default();
        byteorder::BE::write_f32(&mut buf, *self);
        adjust_float_bytes(&mut buf, true);

        w.write_all(&buf)
    }
}

impl Decode for f32 {
    fn decode(buf: &[u8], _tuple_depth: TupleDepth) -> Result<(Self, usize)> {
        if buf.len() < 5 {
            return Err(Error::EOF);
        }

        FLOAT.expect(buf[0])?;

        let mut data: [u8; 4] = Default::default();
        data.copy_from_slice(&buf[1..5]);
        adjust_float_bytes(&mut data, false);

        let val = byteorder::BE::read_f32(&data);
        Ok((val, 5))
    }
}

impl Encode for f64 {
    fn encode<W: Write>(&self, w: &mut W, _tuple_depth: TupleDepth) -> std::io::Result<()> {
        DOUBLE.write(w)?;

        let mut buf: [u8; 8] = Default::default();
        byteorder::BE::write_f64(&mut buf, *self);
        adjust_float_bytes(&mut buf, true);

        w.write_all(&buf)
    }
}

impl Decode for f64 {
    fn decode(buf: &[u8], _tuple_depth: TupleDepth) -> Result<(Self, usize)> {
        if buf.len() < 9 {
            return Err(Error::EOF);
        }

        DOUBLE.expect(buf[0])?;

        let mut data: [u8; 8] = Default::default();
        data.copy_from_slice(&buf[1..9]);
        adjust_float_bytes(&mut data, false);

        let val = byteorder::BE::read_f64(&data);
        Ok((val, 9))
    }
}

impl Encode for i64 {
    fn encode<W: Write>(&self, w: &mut W, _tuple_depth: TupleDepth) -> std::io::Result<()> {
        let mut code = INTZERO;
        let abs = self.wrapping_abs() as u64;
        let n = bisect_left(abs);
        let mut buf: [u8; 8] = Default::default();

        if *self > 0 {
            code += n as u8;
            byteorder::BE::write_u64(&mut buf, abs);
        } else {
            code -= n as u8;
            byteorder::BE::write_u64(&mut buf, SIZE_LIMITS[n] - abs);
        }

        w.write_all(&[code])?;
        w.write_all(&buf[(8 - n)..8])
    }
}

impl Decode for i64 {
    fn decode(buf: &[u8], _tuple_depth: TupleDepth) -> Result<(Self, usize)> {
        if buf.is_empty() {
            return Err(Error::EOF);
        }
        let header = buf[0];
        if header < 0x0c || header > 0x1c {
            return Err(Error::InvalidType { value: header });
        }

        // zero
        if INTZERO.expect(header).is_ok() {
            return Ok((0, 1));
        }

        let mut data: [u8; 8] = Default::default();
        if header > INTZERO {
            // positive number
            let n = usize::from(header - INTZERO);
            if n + 1 > buf.len() {
                return Err(Error::InvalidData);
            }

            (&mut data[(8 - n)..8]).copy_from_slice(&buf[1..(n + 1)]);
            let val = byteorder::BE::read_u64(&data);
            let max = i64::max_value() as u64;
            if val <= max {
                Ok((val as i64, n + 1))
            } else {
                Err(Error::InvalidData)
            }
        } else {
            // negative number
            let n = usize::from(INTZERO - header);
            if n + 1 > buf.len() {
                return Err(Error::InvalidData);
            }

            (&mut data[(8 - n)..8]).copy_from_slice(&buf[1..(n + 1)]);
            let shift = SIZE_LIMITS[n];

            let val = byteorder::BE::read_u64(&data);
            let val = shift - val;
            let max = i64::max_value() as u64 + 1;
            if val < max {
                Ok((-(val as i64), n + 1))
            } else if val == max {
                // val == i64::max_value()+1, (encoded value is i64::min_value())
                Ok((i64::min_value(), n + 1))
            } else {
                Err(Error::InvalidData)
            }
        }
    }
}

impl<T> Encode for Option<T>
where
    T: Encode,
{
    fn encode<W: Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> std::io::Result<()> {
        match *self {
            Some(ref t) => t.encode(w, tuple_depth),
            None => {
                // only at tuple depth greater than 1...
                if tuple_depth.depth() > 1 {
                    NIL.write(w)?;
                    ESCAPE.write(w)
                } else {
                    NIL.write(w)
                }
            }
        }
    }
}

impl<T> Decode for Option<T>
where
    T: Decode,
{
    fn decode(buf: &[u8], tuple_depth: TupleDepth) -> Result<(Self, usize)> {
        match *buf.get(0).ok_or(Error::EOF)? {
            NIL => {
                // custom escape markers are only needed in Nested tuples...
                if tuple_depth.depth() > 1 {
                    let byte = *buf.get(1).ok_or(Error::InvalidData)?;
                    ESCAPE.expect(byte)?;
                    Ok((None, 2))
                } else {
                    Ok((None, 1))
                }
            }
            _ => T::decode(buf, tuple_depth).map(|(t, offset)| (Some(t), offset)),
        }
    }
}

impl Encode for Element {
    fn encode<W: Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> std::io::Result<()> {
        use self::Element::*;

        match *self {
            Empty => Encode::encode(&(), w, tuple_depth),
            Bytes(ref v) => Encode::encode(v, w, tuple_depth),
            String(ref v) => Encode::encode(v, w, tuple_depth),
            Tuple(ref v) => Encode::encode(&v.0, w, tuple_depth),
            I64(ref v) => Encode::encode(v, w, tuple_depth),
            F32(ref v) => Encode::encode(v, w, tuple_depth),
            F64(ref v) => Encode::encode(v, w, tuple_depth),
            Bool(ref v) => Encode::encode(v, w, tuple_depth),
            #[cfg(feature = "uuid")]
            Uuid(ref v) => Encode::encode(v, w, tuple_depth),
            // Ugly hack
            // We will be able to drop this once #[non_exhaustive]
            // lands on `stable`
            __Nonexhaustive => panic!("__Nonexhaustive is private"),
        }
    }
}

impl Decode for Element {
    fn decode(buf: &[u8], tuple_depth: TupleDepth) -> Result<(Self, usize)> {
        if buf.is_empty() {
            return Err(Error::EOF);
        }

        let code = buf[0];
        match code {
            NIL => Ok((Element::Empty, 1)),
            BYTES => {
                let (v, offset) = Decode::decode(buf, tuple_depth)?;
                Ok((Element::Bytes(v), offset))
            }
            STRING => {
                let (v, offset) = Decode::decode(buf, tuple_depth)?;
                Ok((Element::String(v), offset))
            }
            FLOAT => {
                let (v, offset) = Decode::decode(buf, tuple_depth)?;
                Ok((Element::F32(v), offset))
            }
            DOUBLE => {
                let (v, offset) = Decode::decode(buf, tuple_depth)?;
                Ok((Element::F64(v), offset))
            }
            FALSE => Ok((Element::Bool(false), 1)),
            TRUE => Ok((Element::Bool(true), 1)),
            #[cfg(feature = "uuid")]
            UUID => {
                let (v, offset) = Decode::decode(buf, tuple_depth)?;
                Ok((Element::Uuid(v), offset))
            }
            NESTED => {
                let (v, offset) = Decode::decode(buf, tuple_depth)?;
                Ok((Element::Tuple(Tuple(v)), offset))
            }
            val => {
                if val >= NEGINTSTART && val <= POSINTEND {
                    let (v, offset) = Decode::decode(buf, tuple_depth)?;
                    Ok((Element::I64(v), offset))
                } else {
                    //TODO: Versionstamp, ...
                    Err(Error::InvalidData)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tuple::Tuple;

    fn test_round_trip<S>(val: S, buf: &[u8])
    where
        S: Encode + Decode + std::fmt::Debug + PartialEq,
    {
        assert_eq!(val, Decode::try_from(buf).unwrap());
        assert_eq!(buf, Encode::to_vec(&val).as_slice());
    }

    #[test]
    fn test_element() {
        // Some testcases are generated by following python script
        // [ord(v) for v in fdb.tuple.pack(tup)]

        // bool
        test_round_trip(false, &[FALSE]);
        test_round_trip(true, &[TRUE]);

        // empty
        test_round_trip((), &[NIL]);

        // int
        test_round_trip(0i64, &[INTZERO]);
        test_round_trip(1i64, &[0x15, 1]);
        test_round_trip(-1i64, &[0x13, 254]);
        test_round_trip(100i64, &[21, 100]);

        test_round_trip(10000i64, &[22, 39, 16]);
        test_round_trip(-100i64, &[19, 155]);
        test_round_trip(-10000i64, &[18, 216, 239]);
        test_round_trip(-1000000i64, &[17, 240, 189, 191]);

        // boundary condition
        test_round_trip(255i64, &[21, 255]);
        test_round_trip(256i64, &[22, 1, 0]);
        test_round_trip(-255i64, &[19, 0]);
        test_round_trip(-256i64, &[18, 254, 255]);

        // float
        test_round_trip(1.6f64, &[33, 191, 249, 153, 153, 153, 153, 153, 154]);

        // string
        test_round_trip(String::from("hello"), &[2, 104, 101, 108, 108, 111, 0]);

        // binary
        test_round_trip(b"hello".to_vec(), &[1, 104, 101, 108, 108, 111, 0]);
        test_round_trip(vec![0], &[1, 0, 0xff, 0]);
        test_round_trip(
            Element::Tuple(Tuple(vec![
                Element::String("hello".to_string()),
                Element::String("world".to_string()),
                Element::I64(42),
            ])),
            &[
                NESTED, /*hello*/ 2, 104, 101, 108, 108, 111, 0, /*world*/ 2, 119, 111,
                114, 108, 100, 0, /*42*/ 21, 42, /*end nested*/
                NIL,
            ],
        );
        
        test_round_trip(
            Element::Tuple(Tuple(vec![
                Element::Bytes(vec![0]),
                Element::Empty,
                Element::Tuple(Tuple(vec![Element::Bytes(vec![0]), Element::Empty])),
            ])),
            &[5, 1, 0, 255, 0, 0, 255, 5, 1, 0, 255, 0, 0, 255, 0, 0],
        );

        test_round_trip(
            Element::Tuple(Tuple(vec![
                Element::Bool(true),
                Element::Tuple(Tuple(vec![Element::Bool(false)])),
            ])),
            &[NESTED, 39, NESTED, 38, NIL, NIL],
        );
    }

    #[test]
    fn test_large_neg() {
        test_round_trip(
            -8617230260136600747,
            &[0x0c, 0x88, 0x69, 0x72, 0xbc, 0x04, 0xcf, 0x9b, 0x54],
        );
    }

    #[test]
    fn test_boundary() {
        test_round_trip(i64::min_value() + 1, &[0x0c, 0x80, 0, 0, 0, 0, 0, 0, 0]);

        test_round_trip(
            i64::min_value(),
            &[0x0c, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        );

        test_round_trip(
            i64::max_value(),
            &[0x1c, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        );

        test_round_trip(
            i64::max_value() - 1,
            &[0x1c, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe],
        );
    }

    #[test]
    fn test_i64_out_of_bound() {
        // fdb.tuple.pack(((1<<63),))
        let v = i64::try_from(&[0x1c, 0x80, 0, 0, 0, 0, 0, 0, 0]);
        assert!(v.is_err());

        // fdb.tuple.pack((-(1<<63)-1,))
        let v = i64::try_from(&[0x0c, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe]);
        assert!(v.is_err());
    }

    #[test]
    fn test_decode_nested() {
        use tuple::Decode;

        assert!(Tuple::try_from(&[NESTED]).is_err());
        assert!(Tuple::try_from(&[NESTED, NIL]).is_ok());
        assert!(Tuple::try_from(&[NESTED, INTZERO]).is_err());
        assert!(Tuple::try_from(&[NESTED, NIL, NESTED, NIL]).is_ok());
        assert!(Tuple::try_from(&[NESTED, NESTED, NESTED, NIL, NIL, NIL]).is_ok());
    }

    #[test]
    fn test_option() {
        assert_eq!(&Some(42_i64).to_vec(), &[21, 42]);
        assert_eq!(&None::<i64>.to_vec(), &[0]);

        assert_eq!(Some(42), Decode::try_from(&[21, 42]).expect("Some(42)"));
        assert_eq!(None::<i64>, Decode::try_from(&[0]).expect("None::<i64>"));

        assert!(<(i64, Option<i64>)>::try_from(&[0]).is_err());
        assert!(<(i64, Option<i64>)>::try_from(&[21, 42, 0]).is_ok());
        assert!(
            // one of the inner Nones, is missing the final escape byte...
            <(i64, (Option<i64>, Option<i64>))>::try_from(&[21, 42, 5, 0, 255, 0, 0]).is_err()
        );
    }
}
