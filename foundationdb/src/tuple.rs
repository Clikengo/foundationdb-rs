// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Tuple Key type like that of other FoundationDB libraries

use byteorder::{self, ByteOrder};
use std;
use std::io::Write;

#[derive(Debug, Clone, Copy)]
pub enum TupleError {
    //
    EOF,
    InvalidType,
    InvalidData,
}

type Result<T> = std::result::Result<T, TupleError>;

trait Single: Sized {
    fn encode<W: Write>(&self, _w: W) -> std::io::Result<()>;
    fn encode_to_vec(&self) -> std::io::Result<Vec<u8>> {
        let mut v = Vec::new();
        self.encode(&mut v)?;
        Ok(v)
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
    fn encode<W: Write>(&self, mut w: W) -> std::io::Result<()> {
        if *self {
            w.write_all(&[0x27])
        } else {
            w.write_all(&[0x26])
        }
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.is_empty() {
            return Err(TupleError::EOF);
        }

        match buf[0] {
            0x26 => Ok((false, 1)),
            0x27 => Ok((true, 1)),
            _ => Err(TupleError::InvalidType),
        }
    }
}

impl Single for () {
    fn encode<W: Write>(&self, mut w: W) -> std::io::Result<()> {
        w.write_all(&[0])
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.is_empty() {
            return Err(TupleError::EOF);
        }

        if buf[0] == 0x00 {
            return Ok(((), 1));
        }
        Err(TupleError::InvalidType)
    }
}

struct UUID([u8; 16]);

impl Single for UUID {
    fn encode<W: Write>(&self, mut w: W) -> std::io::Result<()> {
        w.write_all(&[0x30])?;
        w.write_all(&self.0)
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 17 {
            return Err(TupleError::EOF);
        }

        if buf[0] != 0x30 {
            return Err(TupleError::InvalidType);
        }

        let mut uuid = [0u8; 16];
        uuid.copy_from_slice(&buf[1..17]);

        Ok((UUID(uuid), 17))
    }
}

fn encode_bytes<W: Write>(mut w: W, buf: &[u8]) -> std::io::Result<()> {
    for b in buf {
        w.write_all(&[*b])?;
        if *b == 0 {
            w.write_all(&[0xff])?;
        }
    }
    w.write_all(&[0x00])
}

fn decode_bytes(buf: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut out = Vec::new();
    let mut offset = 0;
    loop {
        if offset >= buf.len() {
            return Err(TupleError::EOF);
        }

        if buf[offset] == 0x00 {
            if offset + 1 < buf.len() && buf[offset + 1] == 0xff {
                out.push(0x00);
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

impl Single for String {
    fn encode<W: Write>(&self, mut w: W) -> std::io::Result<()> {
        w.write_all(&[0x02])?;
        encode_bytes(w, self.as_bytes())
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 2 {
            return Err(TupleError::EOF);
        }

        if buf[0] != 0x02 {
            return Err(TupleError::InvalidType);
        }

        let (bytes, offset) = decode_bytes(&buf[1..])?;
        Ok((String::from_utf8(bytes).unwrap(), offset + 1))
    }
}

impl Single for Vec<u8> {
    fn encode<W: Write>(&self, mut w: W) -> std::io::Result<()> {
        w.write_all(&[0x01])?;
        encode_bytes(w, self.as_slice())
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 2 {
            return Err(TupleError::EOF);
        }

        if buf[0] != 0x01 {
            return Err(TupleError::InvalidType);
        }

        let (bytes, offset) = decode_bytes(&buf[1..])?;
        Ok((bytes, offset + 1))
    }
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

impl Single for f32 {
    fn encode<W: Write>(&self, mut w: W) -> std::io::Result<()> {
        w.write_all(&[0x020])?;

        let mut buf: [u8; 4] = Default::default();
        byteorder::BE::write_f32(&mut buf, *self);
        adjust_float_bytes(&mut buf, true);

        w.write_all(&buf)
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 5 {
            return Err(TupleError::EOF);
        }

        if buf[0] != 0x20 {
            return Err(TupleError::InvalidType);
        }

        let mut data: [u8; 4] = Default::default();
        data.copy_from_slice(&buf[1..5]);
        adjust_float_bytes(&mut data, false);

        let val = byteorder::BE::read_f32(&data);
        Ok((val, 5))
    }
}

impl Single for f64 {
    fn encode<W: Write>(&self, mut w: W) -> std::io::Result<()> {
        w.write_all(&[0x021])?;

        let mut buf: [u8; 8] = Default::default();
        byteorder::BE::write_f64(&mut buf, *self);
        adjust_float_bytes(&mut buf, true);

        w.write_all(&buf)
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < 9 {
            return Err(TupleError::EOF);
        }

        if buf[0] != 0x21 {
            return Err(TupleError::InvalidType);
        }

        let mut data: [u8; 8] = Default::default();
        data.copy_from_slice(&buf[1..9]);
        adjust_float_bytes(&mut data, false);

        let val = byteorder::BE::read_f64(&data);
        Ok((val, 9))
    }
}

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

fn bisect_left(val: i64) -> usize {
    SIZE_LIMITS.iter().position(|v| val <= *v).unwrap_or(8)
}

impl Single for i64 {
    fn encode<W: Write>(&self, mut w: W) -> std::io::Result<()> {
        let mut code = 0x14u8;
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
        let header = buf[0];
        if header < 0x0c || header > 0x1c {
            return Err(TupleError::InvalidType);
        }

        if header == 0x14 {
            return Ok((0, 1));
        }

        let mut data: [u8; 8] = Default::default();
        if header > 0x14 {
            let n = usize::from(header - 0x14);
            (&mut data[(8 - n)..8]).copy_from_slice(&buf[1..(n + 1)]);
            let val = byteorder::BE::read_i64(&data);
            Ok((val, n + 1))
        } else {
            let n = usize::from(0x14 - header);
            (&mut data[(8 - n)..8]).copy_from_slice(&buf[1..(n + 1)]);
            let shift = (1 << (n * 8)) - 1;
            let val = byteorder::BE::read_i64(&data);
            Ok((val - shift, n + 1))
        }
    }
}

trait Tuple: Sized {
    fn encode<W: Write>(&self, _w: W) -> std::io::Result<()>;
    fn encode_to_vec(&self) -> std::io::Result<Vec<u8>> {
        let mut v = Vec::new();
        self.encode(&mut v)?;
        Ok(v)
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
                fn encode<W: Write>(&self, mut w: W) -> std::io::Result<()> {
                    $(
                        self.$n.encode(&mut w)?;
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
                        return Err(TupleError::InvalidData);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_round_trip<S>(val: S, buf: &[u8])
    where
        S: Single + std::fmt::Debug + PartialEq,
    {
        assert_eq!(val, Single::decode_full(buf).unwrap());
        assert_eq!(buf, Single::encode_to_vec(&val).unwrap().as_slice());
    }

    #[test]
    fn test_single() {
        // Some testcases are generated by following python script
        // [ord(v) for v in fdb.tuple.pack(tup)]

        // bool
        test_round_trip(false, &[0x26]);
        test_round_trip(true, &[0x27]);

        // empty
        test_round_trip((), &[0x00]);

        // int
        test_round_trip(0i64, &[0x14]);
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
            Tuple::encode_to_vec(&tup).unwrap().as_slice()
        );
    }
}
