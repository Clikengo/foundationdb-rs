use std::{self, io::Write};

use super::{TupleError, TupleValue, Result};
use byteorder::{self, ByteOrder};

/// Various tuple types
const NIL: u8 = 0x00;
const BYTES: u8 = 0x01;
const STRING: u8 = 0x02;
const NESTED: u8 = 0x05;
const INTZERO: u8 = 0x14;
const POSINTEND: u8 = 0x1d;
const NEGINTSTART: u8 = 0x0b;
const FLOAT: u8 = 0x20;
const DOUBLE: u8 = 0x21;
const FALSE: u8 = 0x26;
const TRUE: u8 = 0x27;
const UUID: u8 = 0x30;
const VERSIONSTAMP: u8 = 0x33;

const ESCAPE: u8 = 0xff;

const SIZE_LIMITS: &[i64] = &[
    0,
    (1 << (1 * 8)) - 1,
    (1 << (2 * 8)) - 1,
    (1 << (3 * 8)) - 1,
    (1 << (4 * 8)) - 1,
    (1 << (5 * 8)) - 1,
    (1 << (6 * 8)) - 1,
    (1 << (7 * 8)) - 1,
];

#[derive(Clone, Debug, PartialEq)]
pub struct Uuid([u8; 16]);

#[derive(Clone, Debug, PartialEq)]
pub enum SingleValue {
    Empty,
    Bytes(Vec<u8>),
    Str(String),
    Nested(TupleValue),
    Int(i64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    Uuid(Uuid),
}

trait SingleType: Copy {
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
    let mut out = Vec::<u8>::new();
    let mut offset = 0;
    loop {
        if offset >= buf.len() {
            return Err(TupleError::EOF);
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

fn bisect_left(val: i64) -> usize {
    SIZE_LIMITS.iter().position(|v| val <= *v).unwrap_or(8)
}

impl SingleType for u8 {
    /// verifies the value matches this type
    fn expect(self, value: u8) -> Result<()> {
        if self == value {
            Ok(())
        } else {
            Err(TupleError::InvalidType { value })
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
            UUID => Ok(()),
            VERSIONSTAMP => Ok(()),
            _ => Err(TupleError::InvalidType { value: self }),
        }
    }

    fn write<W: Write>(self, w: &mut W) -> std::io::Result<()> {
        w.write_all(&[self])
    }
}

pub trait Single: Sized {
    fn encode<W: Write>(&self, _w: &mut W) -> std::io::Result<()>;
    fn encode_to_vec(&self) -> Vec<u8> {
        let mut v = Vec::new();
        // `self.encode` should not fail because undering `Write` does not return error.
        self.encode(&mut v)
            .expect("single encoding should never fail");
        v
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize)>;
    fn decode_full(buf: &[u8]) -> Result<Self> {
        let (val, offset) = Self::decode(buf)?;
        if offset != buf.len() {
            return Err(TupleError::InvalidData);
        }
        Ok(val)
    }
}

impl Single for bool {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        if *self {
            TRUE.write(w)
        } else {
            FALSE.write(w)
        }
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.is_empty() {
            return Err(TupleError::EOF);
        }

        match buf[0] {
            FALSE => Ok((false, 1)),
            TRUE => Ok((true, 1)),
            v => Err(TupleError::InvalidType { value: v }),
        }
    }
}

impl Single for () {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        NIL.write(w)
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.is_empty() {
            return Err(TupleError::EOF);
        }

        NIL.expect(buf[0])?;
        Ok(((), 1))
    }
}

impl Single for Uuid {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        UUID.write(w)?;
        w.write_all(&self.0)
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 17 {
            return Err(TupleError::EOF);
        }

        UUID.expect(buf[0])?;

        let mut uuid = [0u8; 16];
        uuid.copy_from_slice(&buf[1..17]);

        Ok((Uuid(uuid), 17))
    }
}

impl Single for String {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        STRING.write(w)?;
        encode_bytes(w, self.as_bytes())
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 2 {
            return Err(TupleError::EOF);
        }

        STRING.expect(buf[0])?;

        let (bytes, offset) = decode_bytes(&buf[1..])?;
        Ok((String::from_utf8(bytes)?, offset + 1))
    }
}

impl Single for Vec<SingleValue> {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        NESTED.write(w)?;
        for v in self {
            match v {
                &SingleValue::Empty => {
                    // Empty value in nested tuple is encoded with [NIL, ESCAPE] to disambiguate
                    // itself with end-of-tuple marker.
                    NIL.write(w)?;
                    ESCAPE.write(w)?;
                }
                v => {
                    v.encode(w)?;
                }
            }
        }
        NIL.write(w)
    }

    fn decode(mut buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 2 {
            return Err(TupleError::EOF);
        }

        NESTED.expect(buf[0])?;
        let len = buf.len();
        buf = &buf[1..];

        let mut tuples = Vec::new();
        loop {
            if buf.is_empty() {
                // tuple must end with NIL byte
                return Err(TupleError::EOF);
            }

            if buf[0] == NIL {
                if buf.len() > 1 && buf[1] == ESCAPE {
                    // nested Empty value, which is encoded to [NIL, ESCAPE]
                    tuples.push(SingleValue::Empty);
                    buf = &buf[2..];
                    continue;
                }

                buf = &buf[1..];
                break;
            }

            let (tuple, offset) = SingleValue::decode(buf)?;
            tuples.push(tuple);
            buf = &buf[offset..];
        }

        // skip the final null
        Ok((tuples, len - buf.len()))
    }
}

impl Single for Vec<u8> {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        BYTES.write(w)?;
        encode_bytes(w, self.as_slice())
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 2 {
            return Err(TupleError::EOF);
        }

        BYTES.expect(buf[0])?;

        let (bytes, offset) = decode_bytes(&buf[1..])?;
        Ok((bytes, offset + 1))
    }
}

impl Single for f32 {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        FLOAT.write(w)?;

        let mut buf: [u8; 4] = Default::default();
        byteorder::BE::write_f32(&mut buf, *self);
        adjust_float_bytes(&mut buf, true);

        w.write_all(&buf)
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 5 {
            return Err(TupleError::EOF);
        }

        FLOAT.expect(buf[0])?;

        let mut data: [u8; 4] = Default::default();
        data.copy_from_slice(&buf[1..5]);
        adjust_float_bytes(&mut data, false);

        let val = byteorder::BE::read_f32(&data);
        Ok((val, 5))
    }
}

impl Single for f64 {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        DOUBLE.write(w)?;

        let mut buf: [u8; 8] = Default::default();
        byteorder::BE::write_f64(&mut buf, *self);
        adjust_float_bytes(&mut buf, true);

        w.write_all(&buf)
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 9 {
            return Err(TupleError::EOF);
        }

        DOUBLE.expect(buf[0])?;

        let mut data: [u8; 8] = Default::default();
        data.copy_from_slice(&buf[1..9]);
        adjust_float_bytes(&mut data, false);

        let val = byteorder::BE::read_f64(&data);
        Ok((val, 9))
    }
}

impl Single for i64 {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        let mut code = INTZERO;
        let n;
        let mut buf: [u8; 8] = Default::default();

        if *self > 0 {
            n = bisect_left(*self);
            code += n as u8;
            byteorder::BE::write_i64(&mut buf, *self);
        } else {
            n = bisect_left(-*self);
            code -= n as u8;
            byteorder::BE::write_i64(&mut buf, SIZE_LIMITS[n] + *self);
        }

        w.write_all(&[code])?;
        w.write_all(&buf[(8 - n)..8])
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.is_empty() {
            return Err(TupleError::EOF);
        }
        let header = buf[0];
        if header < 0x0c || header > 0x1c {
            return Err(TupleError::InvalidType { value: header });
        }

        // if it's 0
        if INTZERO.expect(header).is_ok() {
            return Ok((0, 1));
        }

        let mut data: [u8; 8] = Default::default();
        if header > INTZERO {
            let n = usize::from(header - INTZERO);
            if n + 1 > buf.len() {
                return Err(TupleError::InvalidData);
            }

            (&mut data[(8 - n)..8]).copy_from_slice(&buf[1..(n + 1)]);
            let val = byteorder::BE::read_i64(&data);
            Ok((val, n + 1))
        } else {
            let n = usize::from(INTZERO - header);
            if n + 1 > buf.len() {
                return Err(TupleError::InvalidData);
            }

            (&mut data[(8 - n)..8]).copy_from_slice(&buf[1..(n + 1)]);
            let shift = (1 << (n * 8)) - 1;
            let val = byteorder::BE::read_i64(&data);
            Ok((val - shift, n + 1))
        }
    }
}

impl Single for SingleValue {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        use self::SingleValue::*;

        match *self {
            Empty => Single::encode(&(), w),
            Bytes(ref v) => Single::encode(v, w),
            Str(ref v) => Single::encode(v, w),
            Nested(ref v) => Single::encode(&v.0, w),
            Int(ref v) => Single::encode(v, w),
            Float(ref v) => Single::encode(v, w),
            Double(ref v) => Single::encode(v, w),
            Boolean(ref v) => Single::encode(v, w),
            Uuid(ref v) => Single::encode(v, w),
        }
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.is_empty() {
            return Err(TupleError::EOF);
        }

        let code = buf[0];
        match code {
            NIL => Ok((SingleValue::Empty, 1)),
            BYTES => {
                let (v, offset) = Single::decode(buf)?;
                Ok((SingleValue::Bytes(v), offset))
            }
            STRING => {
                let (v, offset) = Single::decode(buf)?;
                Ok((SingleValue::Str(v), offset))
            }
            FLOAT => {
                let (v, offset) = Single::decode(buf)?;
                Ok((SingleValue::Float(v), offset))
            }
            DOUBLE => {
                let (v, offset) = Single::decode(buf)?;
                Ok((SingleValue::Double(v), offset))
            }
            FALSE => Ok((SingleValue::Boolean(false), 1)),
            TRUE => Ok((SingleValue::Boolean(false), 1)),
            UUID => {
                let (v, offset) = Single::decode(buf)?;
                Ok((SingleValue::Uuid(v), offset))
            }
            NESTED => {
                let (v, offset) = Single::decode(buf)?;
                Ok((SingleValue::Nested(TupleValue(v)), offset))
            }
            val => {
                if val >= NEGINTSTART && val <= POSINTEND {
                    let (v, offset) = Single::decode(buf)?;
                    Ok((SingleValue::Int(v), offset))
                } else {
                    //TODO: Versionstamp, ...
                    Err(TupleError::InvalidData)
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
        S: Single + std::fmt::Debug + PartialEq,
    {
        assert_eq!(val, Single::decode_full(buf).unwrap());
        assert_eq!(buf, Single::encode_to_vec(&val).as_slice());
    }

    #[test]
    fn test_single() {
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
            SingleValue::Nested(TupleValue(vec![
                SingleValue::Str("hello".to_string()),
                SingleValue::Str("world".to_string()),
                SingleValue::Int(42),
            ])),
            &[
                NESTED,
                /*hello*/ 2,
                104,
                101,
                108,
                108,
                111,
                0,
                /*world*/ 2,
                119,
                111,
                114,
                108,
                100,
                0,
                /*42*/ 21,
                42,
                /*end nested*/
                NIL,
            ],
        );

        test_round_trip(
            SingleValue::Nested(TupleValue(vec![
                SingleValue::Bytes(vec![0]),
                SingleValue::Empty,
                SingleValue::Nested(TupleValue(vec![
                    SingleValue::Bytes(vec![0]),
                    SingleValue::Empty,
                ])),
            ])),
            &[5, 1, 0, 255, 0, 0, 255, 5, 1, 0, 255, 0, 0, 255, 0, 0],
        );
    }

    #[test]
    fn test_malformed_int() {
        assert!(TupleValue::decode(&[21, 0]).is_ok());
        assert!(TupleValue::decode(&[22, 0]).is_err());
        assert!(TupleValue::decode(&[22, 0, 0]).is_ok());

        assert!(TupleValue::decode(&[19, 0]).is_ok());
        assert!(TupleValue::decode(&[18, 0]).is_err());
        assert!(TupleValue::decode(&[18, 0, 0]).is_ok());
    }

    #[test]
    fn test_decode_tuple() {
        assert_eq!((0, ()), Tuple::decode(&[20, 0]).unwrap());
    }

    #[test]
    fn test_decode_tuple_ty() {
        let data: &[u8] = &[2, 104, 101, 108, 108, 111, 0, 1, 119, 111, 114, 108, 100, 0];

        let (v1, v2): (String, Vec<u8>) = Tuple::decode(data).unwrap();
        assert_eq!(v1, "hello");
        assert_eq!(v2, b"world");
    }

    #[test]
    fn test_encode_tuple_ty() {
        let tup = (String::from("hello"), b"world".to_vec());

        assert_eq!(
            &[2, 104, 101, 108, 108, 111, 0, 1, 119, 111, 114, 108, 100, 0],
            Tuple::encode_to_vec(&tup).as_slice()
        );
    }

    #[test]
    fn test_decode_nested() {
        assert!(TupleValue::decode(&[NESTED]).is_err());
        assert!(TupleValue::decode(&[NESTED, NIL]).is_ok());
        assert!(TupleValue::decode(&[NESTED, INTZERO]).is_err());
        assert!(TupleValue::decode(&[NESTED, NIL, NESTED, NIL]).is_ok());
        assert!(TupleValue::decode(&[NESTED, NESTED, NESTED, NIL, NIL, NIL]).is_ok());
    }
}
