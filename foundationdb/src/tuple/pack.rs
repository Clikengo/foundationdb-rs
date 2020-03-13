use super::*;
use memchr::memchr_iter;
use std::io;
use std::mem;

/// A type that can be packed
pub trait TuplePack {
    fn pack<W: io::Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> io::Result<()>;

    fn pack_root<W: io::Write>(&self, w: &mut W) -> io::Result<()> {
        self.pack(w, TupleDepth::new())
    }

    fn pack_to_vec(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.pack_root(&mut v)
            .expect("tuple encoding should never fail");
        v
    }
}

/// A type that can be unpacked
pub trait TupleUnpack<'de>: Sized {
    fn unpack(input: &'de [u8], tuple_depth: TupleDepth) -> PackResult<(&'de [u8], Self)>;

    fn unpack_root(input: &'de [u8]) -> PackResult<Self> {
        let (input, this) = Self::unpack(input, TupleDepth::new())?;
        if !input.is_empty() {
            return Err(PackError::TrailingBytes);
        }
        Ok(this)
    }
}

impl<'a, T> TuplePack for &'a T
where
    T: TuplePack,
{
    fn pack<W: io::Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> std::io::Result<()> {
        (*self).pack(w, tuple_depth)
    }
}

fn parse_bytes(input: &[u8], num: usize) -> PackResult<(&[u8], &[u8])> {
    if input.len() < num {
        Err(PackError::MissingBytes)
    } else {
        Ok((&input[num..], &input[..num]))
    }
}

fn parse_byte(input: &[u8]) -> PackResult<(&[u8], u8)> {
    if input.is_empty() {
        Err(PackError::MissingBytes)
    } else {
        Ok((&input[1..], input[0]))
    }
}

fn parse_code(input: &[u8], expected: u8) -> PackResult<&[u8]> {
    let (input, found) = parse_byte(input)?;
    if found == expected {
        Ok(input)
    } else {
        Err(PackError::BadCode {
            found,
            expected: Some(expected),
        })
    }
}

fn write_bytes<W: io::Write>(w: &mut W, v: &[u8]) -> io::Result<()> {
    let mut pos = 0;
    for idx in memchr_iter(NIL, v) {
        let next_idx = idx + 1;
        w.write_all(&v[pos..next_idx])?;
        w.write_all(&[ESCAPE])?;
        pos = next_idx;
    }
    w.write_all(&v[pos..])?;
    w.write_all(&[NIL])?;
    Ok(())
}

fn parse_slice<'de>(input: &'de [u8]) -> PackResult<(&'de [u8], Cow<'de, [u8]>)> {
    let mut bytes = Vec::new();
    let mut pos = 0;
    for idx in memchr_iter(NIL, input) {
        let next_idx = idx + 1;
        if input.get(next_idx) == Some(&ESCAPE) {
            bytes.extend_from_slice(&input[pos..next_idx]);
            pos = next_idx + 1;
        } else {
            let slice = &input[pos..idx];
            return Ok((
                &input[next_idx..],
                (if pos == 0 {
                    Cow::Borrowed(slice)
                } else {
                    bytes.extend_from_slice(slice);
                    Cow::Owned(bytes)
                }),
            ));
        }
    }
    Err(PackError::MissingBytes)
}

fn parse_string<'de>(input: &'de [u8]) -> PackResult<(&'de [u8], Cow<'de, str>)> {
    let (input, slice) = parse_slice(input)?;
    Ok((
        input,
        match slice {
            Cow::Borrowed(slice) => {
                Cow::Borrowed(std::str::from_utf8(slice).map_err(|_| PackError::BadStringFormat)?)
            }
            Cow::Owned(vec) => {
                Cow::Owned(String::from_utf8(vec).map_err(|_| PackError::BadStringFormat)?)
            }
        },
    ))
}

impl TuplePack for () {
    fn pack<W: io::Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> std::io::Result<()> {
        if tuple_depth.depth() > 0 {
            w.write_all(&[NESTED, NIL])?;
        }
        Ok(())
    }
}

impl<'de> TupleUnpack<'de> for () {
    fn unpack(mut input: &[u8], tuple_depth: TupleDepth) -> PackResult<(&[u8], Self)> {
        if tuple_depth.depth() > 0 {
            input = parse_code(input, NESTED)?;
            input = parse_code(input, NIL)?;
        }
        Ok((input, ()))
    }
}

macro_rules! tuple_impls {
    ($(($($n:tt $name:ident $v:ident)+))+) => {
        $(
            impl<$($name),+> TuplePack for ($($name,)+)
            where
                $($name: TuplePack,)+
            {
                fn pack<W: io::Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> std::io::Result<()> {
                    if tuple_depth.depth() > 0 {
                        w.write_all(&[NESTED])?;
                    }

                    $(
                        self.$n.pack(w, tuple_depth.increment())?;
                    )*

                    if tuple_depth.depth() > 0 {
                        w.write_all(&[NIL])?;
                    }
                    Ok(())
                }
            }

            impl<'de, $($name),+> TupleUnpack<'de> for ($($name,)+)
            where
                $($name: TupleUnpack<'de>,)+
            {
                fn unpack(input: &'de [u8], tuple_depth: TupleDepth) -> PackResult<(&'de [u8], Self)> {
                    let input = if tuple_depth.depth() > 0 { parse_code(input, NESTED)? } else { input };

                    $(
                        let (input, $v) = $name::unpack(input, tuple_depth.increment())?;
                    )*

                    let input = if tuple_depth.depth() > 0 { parse_code(input, NIL)? } else { input };

                    let tuple = ( $($v,)* );
                    Ok((input, tuple))
                }
            }
        )+
    }
}

tuple_impls! {
    (0 T0 t0)
    (0 T0 t0 1 T1 t1)
    (0 T0 t0 1 T1 t1 2 T2 t2)
    (0 T0 t0 1 T1 t1 2 T2 t2 3 T3 t3)
    (0 T0 t0 1 T1 t1 2 T2 t2 3 T3 t3 4 T4 t4)
    (0 T0 t0 1 T1 t1 2 T2 t2 3 T3 t3 4 T4 t4 5 T5 t5)
    (0 T0 t0 1 T1 t1 2 T2 t2 3 T3 t3 4 T4 t4 5 T5 t5 6 T6 t6)
    (0 T0 t0 1 T1 t1 2 T2 t2 3 T3 t3 4 T4 t4 5 T5 t5 6 T6 t6 7 T7 t7)
    (0 T0 t0 1 T1 t1 2 T2 t2 3 T3 t3 4 T4 t4 5 T5 t5 6 T6 t6 7 T7 t7 8 T8 t8)
    (0 T0 t0 1 T1 t1 2 T2 t2 3 T3 t3 4 T4 t4 5 T5 t5 6 T6 t6 7 T7 t7 8 T8 t8 9 T9 t9)
    (0 T0 t0 1 T1 t1 2 T2 t2 3 T3 t3 4 T4 t4 5 T5 t5 6 T6 t6 7 T7 t7 8 T8 t8 9 T9 t9 10 T10 t10)
    (0 T0 t0 1 T1 t1 2 T2 t2 3 T3 t3 4 T4 t4 5 T5 t5 6 T6 t6 7 T7 t7 8 T8 t8 9 T9 t9 10 T10 t10 11 T11 t11)
}

const MAX_SZ: usize = 8;

macro_rules! sign_bit {
    ($type:ident) => {
        (1 << (mem::size_of::<$type>() * 8 - 1))
    };
}

macro_rules! unpack_px {
    ($ux: ident, $input: expr, $n: expr) => {{
        let (input, bytes) = parse_bytes($input, $n)?;
        let mut arr = [0u8; ::std::mem::size_of::<$ux>()];
        (&mut arr[(::std::mem::size_of::<$ux>() - $n)..]).copy_from_slice(bytes);
        (input, $ux::from_be_bytes(arr))
    }};
}
macro_rules! unpack_nx {
    ($ix: ident, $input: expr, $n: expr) => {{
        let (input, bytes) = parse_bytes($input, $n)?;
        let mut arr = [0xffu8; ::std::mem::size_of::<$ix>()];
        (&mut arr[(::std::mem::size_of::<$ix>() - $n)..]).copy_from_slice(bytes);
        (input, $ix::from_be_bytes(arr).wrapping_add(1))
    }};
}

macro_rules! impl_ux {
    ($ux: ident) => {
        impl_ux!($ux, mem::size_of::<$ux>());
    };
    ($ux: ident, $max_sz:expr) => {
        impl TuplePack for $ux {
            fn pack<W: io::Write>(
                &self,
                w: &mut W,
                _tuple_depth: TupleDepth,
            ) -> std::io::Result<()> {
                const SZ: usize = mem::size_of::<$ux>();
                let u = *self;
                let n = SZ - (u.leading_zeros() as usize) / 8;
                if SZ <= MAX_SZ || n <= MAX_SZ {
                    w.write_all(&[INTZERO + n as u8])?;
                } else {
                    w.write_all(&[POSINTEND, n as u8])?;
                }
                w.write_all(&u.to_be_bytes()[SZ - n..])?;
                Ok(())
            }
        }

        impl<'de> TupleUnpack<'de> for $ux {
            fn unpack(input: &[u8], _tuple_depth: TupleDepth) -> PackResult<(&[u8], Self)> {
                const SZ: usize = mem::size_of::<$ux>();
                let (input, found) = parse_byte(input)?;
                if INTZERO <= found && found <= INTZERO + $max_sz as u8 {
                    let n = (found - INTZERO) as usize;
                    Ok(unpack_px!($ux, input, n))
                } else if found == POSINTEND {
                    let (input, raw_length) = parse_byte(input)?;
                    let n: usize = usize::from(raw_length);
                    if n > SZ {
                        return Err(PackError::UnsupportedIntLength);
                    }
                    Ok(unpack_px!($ux, input, n))
                } else {
                    Err(PackError::BadCode {
                        found,
                        expected: None,
                    })
                }
            }
        }
    };
}

macro_rules! impl_ix {
    ($ix: ident, $ux: ident) => {
        impl_ix!($ix, $ux, mem::size_of::<$ix>());
    };
    ($ix: ident, $ux: ident, $max_sz:expr) => {
        impl TuplePack for $ix {
            fn pack<W: io::Write>(
                &self,
                w: &mut W,
                _tuple_depth: TupleDepth,
            ) -> std::io::Result<()> {
                const SZ: usize = mem::size_of::<$ix>();
                let i = *self;
                let u = self.wrapping_abs() as $ux;
                let n = SZ - (u.leading_zeros() as usize) / 8;
                let arr = if i >= 0 {
                    if SZ <= MAX_SZ || n <= MAX_SZ {
                        w.write_all(&[INTZERO + n as u8])?;
                    } else {
                        w.write_all(&[POSINTEND, n as u8])?;
                    }
                    (u.to_be_bytes())
                } else {
                    if SZ <= MAX_SZ || n <= MAX_SZ {
                        w.write_all(&[INTZERO - n as u8])?;
                    } else {
                        w.write_all(&[NEGINTSTART, n as u8 ^ 0xff])?;
                    }
                    (i.wrapping_sub(1).to_be_bytes())
                };
                w.write_all(&arr[SZ - n..])?;

                Ok(())
            }
        }

        impl<'de> TupleUnpack<'de> for $ix {
            fn unpack(input: &[u8], _tuple_depth: TupleDepth) -> PackResult<(&[u8], Self)> {
                const SZ: usize = mem::size_of::<$ix>();
                let (input, found) = parse_byte(input)?;
                if INTZERO <= found && found <= INTZERO + $max_sz as u8 {
                    let n = (found - INTZERO) as usize;
                    Ok(unpack_px!($ix, input, n))
                } else if INTZERO - $max_sz as u8 <= found && found < INTZERO {
                    let n = (INTZERO - found) as usize;
                    Ok(unpack_nx!($ix, input, n))
                } else if found == NEGINTSTART {
                    let (input, raw_length) = parse_byte(input)?;
                    let n = usize::from(raw_length ^ 0xff);
                    if n > SZ {
                        return Err(PackError::UnsupportedIntLength);
                    }
                    Ok(unpack_nx!($ix, input, n))
                } else if found == POSINTEND {
                    let (input, raw_length) = parse_byte(input)?;
                    let n: usize = usize::from(raw_length);
                    if n > SZ {
                        return Err(PackError::UnsupportedIntLength);
                    }
                    Ok(unpack_px!($ix, input, n))
                } else {
                    Err(PackError::BadCode {
                        found,
                        expected: None,
                    })
                }
            }
        }
    };
}

macro_rules! impl_fx {
    ( $fx: ident, $parse_ux: ident, $ux: ident, $code: ident) => {
        impl TuplePack for $fx {
            fn pack<W: io::Write>(
                &self,
                w: &mut W,
                _tuple_depth: TupleDepth,
            ) -> std::io::Result<()> {
                let f = *self;
                let u = if f.is_sign_negative() {
                    f.to_bits() ^ ::std::$ux::MAX
                } else {
                    f.to_bits() ^ sign_bit!($ux)
                };
                w.write_all(&[$code])?;
                w.write_all(&u.to_be_bytes())?;
                Ok(())
            }
        }

        fn $parse_ux(input: &[u8]) -> PackResult<(&[u8], $ux)> {
            let (input, bytes) = parse_bytes(input, mem::size_of::<$ux>())?;
            let mut arr = [0u8; mem::size_of::<$ux>()];
            arr.copy_from_slice(bytes);
            Ok((input, $ux::from_be_bytes(arr)))
        }
        impl<'de> TupleUnpack<'de> for $fx {
            fn unpack(input: &[u8], _tuple_depth: TupleDepth) -> PackResult<(&[u8], Self)> {
                let input = parse_code(input, $code)?;
                let (input, u) = $parse_ux(input)?;
                Ok((
                    input,
                    $fx::from_bits(if (u & sign_bit!($ux)) == 0 {
                        u ^ ::std::$ux::MAX
                    } else {
                        u ^ sign_bit!($ux)
                    }),
                ))
            }
        }
    };
}

//impl_ux!(u8);
impl_ux!(u16);
impl_ux!(u32);
impl_ux!(u64);
impl_ux!(u128, MAX_SZ);
impl_ux!(usize);

//impl_ix!(i8, u8);
impl_ix!(i16, u16);
impl_ix!(i32, u32);
impl_ix!(i64, u64);
impl_ix!(i128, u128, MAX_SZ);
impl_ix!(isize, usize);

impl_fx!(f32, parse_u32, u32, FLOAT);
impl_fx!(f64, parse_u64, u64, DOUBLE);

#[cfg(feature = "num-bigint")]
mod bigint {
    use super::*;
    use num_bigint::{BigInt, BigUint, Sign};
    use std::convert::TryFrom;

    fn invert(bytes: &mut [u8]) {
        // The ones' complement of a binary number is defined as the value
        // obtained by inverting all the bits in the binary representation
        // of the number (swapping 0s for 1s and vice versa).
        for byte in bytes.iter_mut() {
            *byte = !*byte;
        }
    }

    fn inverted(bytes: &[u8]) -> Vec<u8> {
        // The ones' complement of a binary number is defined as the value
        // obtained by inverting all the bits in the binary representation
        // of the number (swapping 0s for 1s and vice versa).
        bytes.iter().map(|byte| !*byte).collect()
    }

    impl TuplePack for BigInt {
        fn pack<W: io::Write>(&self, w: &mut W, _tuple_depth: TupleDepth) -> std::io::Result<()> {
            if self.sign() == Sign::NoSign {
                return w.write_all(&[INTZERO]);
            }
            let (sign, mut bytes) = self.to_bytes_be();
            let n = bytes.len();
            match sign {
                Sign::Minus => {
                    if n <= MAX_SZ {
                        w.write_all(&[INTZERO - n as u8])?;
                    } else {
                        w.write_all(&[NEGINTSTART, n as u8 ^ 0xff])?;
                    }
                    invert(&mut bytes);
                    w.write_all(&bytes)?;
                }
                Sign::NoSign => unreachable!(),
                Sign::Plus => {
                    if n <= MAX_SZ {
                        w.write_all(&[INTZERO + n as u8])?;
                    } else {
                        w.write_all(&[POSINTEND, n as u8])?;
                    }
                    w.write_all(&bytes)?;
                }
            };

            Ok(())
        }
    }

    impl<'de> TupleUnpack<'de> for BigInt {
        fn unpack(input: &[u8], _tuple_depth: TupleDepth) -> PackResult<(&[u8], Self)> {
            let (input, found) = parse_byte(input)?;
            if INTZERO <= found && found <= INTZERO + MAX_SZ as u8 {
                let n = (found - INTZERO) as usize;
                let (input, bytes) = parse_bytes(input, n)?;
                Ok((input, Self::from_bytes_be(Sign::Plus, bytes)))
            } else if INTZERO - MAX_SZ as u8 <= found && found < INTZERO {
                let n = (INTZERO - found) as usize;
                let (input, bytes) = parse_bytes(input, n)?;
                Ok((input, Self::from_bytes_be(Sign::Minus, &inverted(bytes))))
            } else if found == NEGINTSTART {
                let (input, raw_length) = parse_byte(input)?;
                let n = usize::from(raw_length ^ 0xff);
                let (input, bytes) = parse_bytes(input, n)?;
                Ok((input, Self::from_bytes_be(Sign::Minus, &inverted(bytes))))
            } else if found == POSINTEND {
                let (input, raw_length) = parse_byte(input)?;
                let n: usize = usize::from(raw_length);
                let (input, bytes) = parse_bytes(input, n)?;
                Ok((input, Self::from_bytes_be(Sign::Plus, bytes)))
            } else {
                Err(PackError::BadCode {
                    found,
                    expected: None,
                })
            }
        }
    }

    impl TuplePack for BigUint {
        fn pack<W: io::Write>(&self, w: &mut W, _tuple_depth: TupleDepth) -> std::io::Result<()> {
            let n = self.bits();
            if n == 0 {
                return w.write_all(&[INTZERO]);
            }
            let bytes = self.to_bytes_be();
            let n = bytes.len();
            if n <= MAX_SZ {
                w.write_all(&[INTZERO + n as u8])?;
            } else {
                w.write_all(&[
                    POSINTEND,
                    u8::try_from(n).map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "BigUint requires more than 255 bytes to be represented",
                        )
                    })?,
                ])?;
            }
            w.write_all(&bytes)?;
            Ok(())
        }
    }

    impl<'de> TupleUnpack<'de> for BigUint {
        fn unpack(input: &[u8], _tuple_depth: TupleDepth) -> PackResult<(&[u8], Self)> {
            let (input, found) = parse_byte(input)?;
            if INTZERO <= found && found <= INTZERO + MAX_SZ as u8 {
                let n = (found - INTZERO) as usize;
                let (input, bytes) = parse_bytes(input, n)?;
                Ok((input, Self::from_bytes_be(bytes)))
            } else if found == POSINTEND {
                let (input, raw_length) = parse_byte(input)?;
                let n: usize = usize::from(raw_length);
                let (input, bytes) = parse_bytes(input, n)?;
                Ok((input, Self::from_bytes_be(bytes)))
            } else {
                Err(PackError::BadCode {
                    found,
                    expected: None,
                })
            }
        }
    }
}

impl TuplePack for bool {
    fn pack<W: io::Write>(&self, w: &mut W, _tuple_depth: TupleDepth) -> std::io::Result<()> {
        w.write_all(&[if *self { TRUE } else { FALSE }])
    }
}

impl<'de> TupleUnpack<'de> for bool {
    fn unpack(input: &[u8], _tuple_depth: TupleDepth) -> PackResult<(&[u8], Self)> {
        let (input, v) = parse_byte(input)?;
        match v {
            FALSE => Ok((input, false)),
            TRUE => Ok((input, true)),
            _ => Err(PackError::Message(
                format!("{} is not a valid bool value", v).into_boxed_str(),
            )),
        }
    }
}

impl<'a, T> TuplePack for &'a [T]
where
    T: TuplePack,
{
    fn pack<W: io::Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> io::Result<()> {
        if tuple_depth.depth() > 0 {
            w.write_all(&[NESTED])?;
        }
        for v in self.iter() {
            v.pack(w, tuple_depth.increment())?;
        }

        if tuple_depth.depth() > 0 {
            w.write_all(&[NIL])?;
        }
        Ok(())
    }
}

impl<T> TuplePack for Vec<T>
where
    T: TuplePack,
{
    fn pack<W: io::Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> std::io::Result<()> {
        self.as_slice().pack(w, tuple_depth)
    }
}

fn is_end_of_tuple(input: &[u8], nested: bool) -> bool {
    match input.first() {
        None => true,
        _ if !nested => false,
        Some(&NIL) => Some(&ESCAPE) != input.get(1),
        _ => false,
    }
}

impl<'de, T> TupleUnpack<'de> for Vec<T>
where
    T: TupleUnpack<'de>,
{
    fn unpack(mut input: &'de [u8], tuple_depth: TupleDepth) -> PackResult<(&'de [u8], Self)> {
        let nested = tuple_depth.depth() > 0;
        if nested {
            input = parse_code(input, NESTED)?;
        }

        let mut vec = Vec::new();

        while !is_end_of_tuple(input, nested) {
            let (rem, v) = T::unpack(input, tuple_depth.increment())?;
            input = rem;
            vec.push(v);
        }

        if nested {
            input = parse_code(input, NIL)?;
        }

        Ok((input, vec))
    }
}

impl<'a> TuplePack for Bytes<'a> {
    fn pack<W: io::Write>(&self, w: &mut W, _tuple_depth: TupleDepth) -> std::io::Result<()> {
        w.write_all(&[BYTES])?;
        write_bytes(w, self.as_ref())
    }
}

impl<'de> TupleUnpack<'de> for Bytes<'de> {
    fn unpack(input: &'de [u8], _tuple_depth: TupleDepth) -> PackResult<(&'de [u8], Self)> {
        let input = parse_code(input, BYTES)?;
        let (input, v) = parse_slice(input)?;
        Ok((input, Bytes(v)))
    }
}

impl<'a> TuplePack for &'a [u8] {
    fn pack<W: io::Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> io::Result<()> {
        Bytes::from(*self).pack(w, tuple_depth)
    }
}

impl TuplePack for Vec<u8> {
    fn pack<W: io::Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> std::io::Result<()> {
        Bytes::from(self.as_slice()).pack(w, tuple_depth)
    }
}

impl<'de> TupleUnpack<'de> for Vec<u8> {
    fn unpack(input: &'de [u8], tuple_depth: TupleDepth) -> PackResult<(&'de [u8], Self)> {
        let (input, bytes) = Bytes::unpack(input, tuple_depth)?;
        Ok((input, bytes.into_owned()))
    }
}

impl<'a> TuplePack for &'a str {
    fn pack<W: io::Write>(&self, w: &mut W, _tuple_depth: TupleDepth) -> std::io::Result<()> {
        w.write_all(&[STRING])?;
        write_bytes(w, self.as_bytes())
    }
}

impl TuplePack for String {
    fn pack<W: io::Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> std::io::Result<()> {
        self.as_str().pack(w, tuple_depth)
    }
}

impl<'a> TuplePack for Cow<'a, str> {
    fn pack<W: io::Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> std::io::Result<()> {
        self.as_ref().pack(w, tuple_depth)
    }
}

impl<'de> TupleUnpack<'de> for Cow<'de, str> {
    fn unpack(input: &'de [u8], _tuple_depth: TupleDepth) -> PackResult<(&'de [u8], Self)> {
        let input = parse_code(input, STRING)?;
        let (input, v) = parse_string(input)?;
        Ok((input, v))
    }
}

impl<'de> TupleUnpack<'de> for String {
    fn unpack(input: &[u8], _tuple_depth: TupleDepth) -> PackResult<(&[u8], Self)> {
        let input = parse_code(input, STRING)?;
        let (input, v) = parse_string(input)?;
        Ok((input, v.into_owned()))
    }
}

impl<T> TuplePack for Option<T>
where
    T: TuplePack,
{
    fn pack<W: io::Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> std::io::Result<()> {
        match self {
            None => Ok(if tuple_depth.depth() > 1 {
                // Empty value in nested tuple is encoded with [NIL, ESCAPE] to
                // disambiguate itself with end-of-tuple marker.
                w.write_all(&[NIL, ESCAPE])
            } else {
                w.write_all(&[NIL])
            }?),
            Some(v) => v.pack(w, tuple_depth),
        }
    }
}

impl<'de, T> TupleUnpack<'de> for Option<T>
where
    T: TupleUnpack<'de>,
{
    fn unpack(mut input: &'de [u8], tuple_depth: TupleDepth) -> PackResult<(&'de [u8], Self)> {
        if let Some(&NIL) = input.first() {
            input = parse_code(input, NIL)?;
            if tuple_depth.depth() > 1 {
                input = parse_code(input, ESCAPE)?;
            }
            Ok((input, None))
        } else {
            let (input, v) = T::unpack(input, tuple_depth)?;
            Ok((input, Some(v)))
        }
    }
}

impl<'a> TuplePack for Element<'a> {
    fn pack<W: io::Write>(&self, w: &mut W, tuple_depth: TupleDepth) -> std::io::Result<()> {
        match self {
            Element::Nil => Option::<()>::None.pack(w, tuple_depth),
            Element::Bool(b) => b.pack(w, tuple_depth),
            Element::Int(i) => i.pack(w, tuple_depth),
            Element::Float(f) => f.pack(w, tuple_depth),
            Element::Double(f) => f.pack(w, tuple_depth),
            Element::String(ref c) => c.pack(w, tuple_depth),
            Element::Bytes(ref b) => b.pack(w, tuple_depth),
            Element::Versionstamp(ref b) => b.pack(w, tuple_depth),
            Element::Tuple(ref v) => v.pack(w, tuple_depth),
            #[cfg(feature = "uuid")]
            Element::Uuid(v) => v.pack(w, tuple_depth),
        }
    }
}

impl<'de> TupleUnpack<'de> for Element<'de> {
    fn unpack(input: &'de [u8], tuple_depth: TupleDepth) -> PackResult<(&'de [u8], Self)> {
        const INTMIN: u8 = INTZERO - 8;
        const INTMAX: u8 = INTZERO + 8;

        let first = match input.first() {
            None if tuple_depth.depth() == 0 => return Ok((input, Element::Tuple(Vec::new()))),
            None => return Err(PackError::MissingBytes),
            Some(byte) => byte,
        };

        let (mut input, mut v) = match *first {
            NIL => {
                let (input, _) = Option::<()>::unpack(input, tuple_depth)?;
                (input, Element::Nil)
            }
            BYTES => {
                let (input, v) = Bytes::unpack(input, tuple_depth)?;
                (input, Element::Bytes(v))
            }
            STRING => {
                let (input, v) = Cow::<'de, str>::unpack(input, tuple_depth)?;
                (input, Element::String(v))
            }
            NESTED => {
                let (input, v) = Vec::<Self>::unpack(input, tuple_depth)?;
                (input, Element::Tuple(v))
            }
            INTMIN..=INTMAX => {
                let (input, v) = i64::unpack(input, tuple_depth)?;
                (input, Element::Int(v))
            }
            NEGINTSTART => {
                let (input, v) = i64::unpack(input, tuple_depth)?;
                (input, Element::Int(v))
            }
            POSINTEND => {
                let (input, v) = i64::unpack(input, tuple_depth)?;
                (input, Element::Int(v))
            }
            FLOAT => {
                let (input, v) = f32::unpack(input, tuple_depth)?;
                (input, Element::Float(v))
            }
            DOUBLE => {
                let (input, v) = f64::unpack(input, tuple_depth)?;
                (input, Element::Double(v))
            }
            FALSE | TRUE => {
                let (input, v) = bool::unpack(input, tuple_depth)?;
                (input, Element::Bool(v))
            }
            VERSIONSTAMP => {
                let (input, v) = Versionstamp::unpack(input, tuple_depth)?;
                (input, Element::Versionstamp(v))
            }
            #[cfg(feature = "uuid")]
            UUID => {
                let (input, v) = uuid::Uuid::unpack(input, tuple_depth)?;
                (input, Element::Uuid(v))
            }
            found => {
                return Err(PackError::BadCode {
                    found,
                    expected: None,
                })
            }
        };

        if tuple_depth.depth() == 0 && !input.is_empty() {
            let mut tuple = Vec::new();
            tuple.push(v);
            while !input.is_empty() {
                let (rem, v) = Self::unpack(input, tuple_depth.increment())?;
                tuple.push(v);
                input = rem;
            }
            v = Element::Tuple(tuple);
        }

        Ok((input, v))
    }
}

impl TuplePack for Versionstamp {
    fn pack<W: io::Write>(&self, w: &mut W, _tuple_depth: TupleDepth) -> std::io::Result<()> {
        w.write_all(&[VERSIONSTAMP])?;
        w.write_all(self.as_bytes())
    }
}

impl<'de> TupleUnpack<'de> for Versionstamp {
    fn unpack(input: &[u8], _tuple_depth: TupleDepth) -> PackResult<(&[u8], Self)> {
        let input = parse_code(input, VERSIONSTAMP)?;
        let (input, slice) = parse_bytes(input, 12)?;
        let mut bytes = [0xff; 12];
        bytes.copy_from_slice(slice);
        Ok((input, Versionstamp::from(bytes)))
    }
}

#[cfg(feature = "uuid")]
mod pack_uuid {
    use super::*;
    use uuid::Uuid;

    impl TuplePack for Uuid {
        fn pack<W: io::Write>(&self, w: &mut W, _tuple_depth: TupleDepth) -> std::io::Result<()> {
            w.write_all(&[UUID])?;
            w.write_all(self.as_bytes())
        }
    }

    impl<'de> TupleUnpack<'de> for Uuid {
        fn unpack(input: &[u8], _tuple_depth: TupleDepth) -> PackResult<(&[u8], Self)> {
            let input = parse_code(input, UUID)?;
            let (input, slice) = parse_bytes(input, 16)?;
            let uuid = Self::from_slice(slice).map_err(|_| PackError::BadUuid)?;
            Ok((input, uuid))
        }
    }
}
