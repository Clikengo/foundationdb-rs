use std::{self, io::Write};
#[cfg(feature = "uuid")]
use uuid::Uuid;

use byteorder::{self, ByteOrder};
use tuple::{self, Decode, Encode, Error, Result};

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
#[cfg(feature = "uuid")]
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
pub enum Value {
    Empty,
    Bytes(Vec<u8>),
    Str(String),
    Nested(tuple::Value),
    Int(i64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    #[cfg(feature = "uuid")]
    Uuid(Uuid),
}

trait Type: Copy {
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

fn bisect_left(val: i64) -> usize {
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

impl Encode for bool {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        if *self {
            TRUE.write(w)
        } else {
            FALSE.write(w)
        }
    }
}

impl Decode for bool {
    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
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
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        NIL.write(w)
    }
}

impl Decode for () {
    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.is_empty() {
            return Err(Error::EOF);
        }

        NIL.expect(buf[0])?;
        Ok(((), 1))
    }
}

#[cfg(feature = "uuid")]
impl Encode for Uuid {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        UUID.write(w)?;
        w.write_all(self.as_bytes())
    }
}

#[cfg(feature = "uuid")]
impl Decode for Uuid {
    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 17 {
            return Err(Error::EOF);
        }

        UUID.expect(buf[0])?;

        let mut uuid = [0u8; 16];
        uuid.copy_from_slice(&buf[1..17]);

        Ok((Uuid::from_uuid_bytes(uuid), 17))
    }
}

impl Encode for String {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        STRING.write(w)?;
        encode_bytes(w, self.as_bytes())
    }
}

impl Decode for String {
    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 2 {
            return Err(Error::EOF);
        }

        STRING.expect(buf[0])?;

        let (bytes, offset) = decode_bytes(&buf[1..])?;
        Ok((String::from_utf8(bytes)?, offset + 1))
    }
}

impl Encode for Vec<Value> {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        NESTED.write(w)?;
        for v in self {
            match v {
                &Value::Empty => {
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
}

impl Decode for Vec<Value> {
    fn decode(mut buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 2 {
            return Err(Error::EOF);
        }

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
                    tuples.push(Value::Empty);
                    buf = &buf[2..];
                    continue;
                }

                buf = &buf[1..];
                break;
            }

            let (tuple, offset) = Value::decode(buf)?;
            tuples.push(tuple);
            buf = &buf[offset..];
        }

        // skip the final null
        Ok((tuples, len - buf.len()))
    }
}

impl Encode for Vec<u8> {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        BYTES.write(w)?;
        encode_bytes(w, self.as_slice())
    }
}

impl Decode for Vec<u8> {
    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 2 {
            return Err(Error::EOF);
        }

        BYTES.expect(buf[0])?;

        let (bytes, offset) = decode_bytes(&buf[1..])?;
        Ok((bytes, offset + 1))
    }
}

impl Encode for f32 {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        FLOAT.write(w)?;

        let mut buf: [u8; 4] = Default::default();
        byteorder::BE::write_f32(&mut buf, *self);
        adjust_float_bytes(&mut buf, true);

        w.write_all(&buf)
    }
}

impl Decode for f32 {
    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
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
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        DOUBLE.write(w)?;

        let mut buf: [u8; 8] = Default::default();
        byteorder::BE::write_f64(&mut buf, *self);
        adjust_float_bytes(&mut buf, true);

        w.write_all(&buf)
    }
}

impl Decode for f64 {
    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
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
}

impl Decode for i64 {
    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.is_empty() {
            return Err(Error::EOF);
        }
        let header = buf[0];
        if header < 0x0c || header > 0x1c {
            return Err(Error::InvalidType { value: header });
        }

        // if it's 0
        if INTZERO.expect(header).is_ok() {
            return Ok((0, 1));
        }

        let mut data: [u8; 8] = Default::default();
        if header > INTZERO {
            let n = usize::from(header - INTZERO);
            if n + 1 > buf.len() {
                return Err(Error::InvalidData);
            }

            (&mut data[(8 - n)..8]).copy_from_slice(&buf[1..(n + 1)]);
            let val = byteorder::BE::read_i64(&data);
            Ok((val, n + 1))
        } else {
            let n = usize::from(INTZERO - header);
            if n + 1 > buf.len() {
                return Err(Error::InvalidData);
            }

            (&mut data[(8 - n)..8]).copy_from_slice(&buf[1..(n + 1)]);
            let shift = (1 << (n * 8)) - 1;
            let val = byteorder::BE::read_i64(&data);
            Ok((val - shift, n + 1))
        }
    }
}

impl Encode for Value {
    fn encode<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        use self::Value::*;

        match *self {
            Empty => Encode::encode(&(), w),
            Bytes(ref v) => Encode::encode(v, w),
            Str(ref v) => Encode::encode(v, w),
            Nested(ref v) => Encode::encode(&v.0, w),
            Int(ref v) => Encode::encode(v, w),
            Float(ref v) => Encode::encode(v, w),
            Double(ref v) => Encode::encode(v, w),
            Boolean(ref v) => Encode::encode(v, w),
            #[cfg(feature = "uuid")]
            Uuid(ref v) => Encode::encode(v, w),
        }
    }
}

impl Decode for Value {
    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.is_empty() {
            return Err(Error::EOF);
        }

        let code = buf[0];
        match code {
            NIL => Ok((Value::Empty, 1)),
            BYTES => {
                let (v, offset) = Decode::decode(buf)?;
                Ok((Value::Bytes(v), offset))
            }
            STRING => {
                let (v, offset) = Decode::decode(buf)?;
                Ok((Value::Str(v), offset))
            }
            FLOAT => {
                let (v, offset) = Decode::decode(buf)?;
                Ok((Value::Float(v), offset))
            }
            DOUBLE => {
                let (v, offset) = Decode::decode(buf)?;
                Ok((Value::Double(v), offset))
            }
            FALSE => Ok((Value::Boolean(false), 1)),
            TRUE => Ok((Value::Boolean(false), 1)),
            #[cfg(feature = "uuid")]
            UUID => {
                let (v, offset) = Decode::decode(buf)?;
                Ok((Value::Uuid(v), offset))
            }
            NESTED => {
                let (v, offset) = Decode::decode(buf)?;
                Ok((Value::Nested(tuple::Value(v)), offset))
            }
            val => {
                if val >= NEGINTSTART && val <= POSINTEND {
                    let (v, offset) = Decode::decode(buf)?;
                    Ok((Value::Int(v), offset))
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
    use tuple::Value as TupleValue;

    fn test_round_trip<S>(val: S, buf: &[u8])
    where
        S: Encode + Decode + std::fmt::Debug + PartialEq,
    {
        assert_eq!(val, Decode::decode_full(buf).unwrap());
        assert_eq!(buf, Encode::encode_to_vec(&val).as_slice());
    }

    #[test]
    fn test_item() {
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
            Value::Nested(TupleValue(vec![
                Value::Str("hello".to_string()),
                Value::Str("world".to_string()),
                Value::Int(42),
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
            Value::Nested(TupleValue(vec![
                Value::Bytes(vec![0]),
                Value::Empty,
                Value::Nested(TupleValue(vec![Value::Bytes(vec![0]), Value::Empty])),
            ])),
            &[5, 1, 0, 255, 0, 0, 255, 5, 1, 0, 255, 0, 0, 255, 0, 0],
        );
    }

    #[test]
    fn test_decode_nested() {
        use tuple::Decode;

        assert!(TupleValue::decode(&[NESTED]).is_err());
        assert!(TupleValue::decode(&[NESTED, NIL]).is_ok());
        assert!(TupleValue::decode(&[NESTED, INTZERO]).is_err());
        assert!(TupleValue::decode(&[NESTED, NIL, NESTED, NIL]).is_ok());
        assert!(TupleValue::decode(&[NESTED, NESTED, NESTED, NIL, NIL, NIL]).is_ok());
    }
}
