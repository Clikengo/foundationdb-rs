use super::*;
use memchr::memchr_iter;
use serde::de::{self, Deserialize, IntoDeserializer, Visitor};
use std::borrow::Cow;
use std::convert::TryFrom;
use std::mem;
use std::str;

pub struct Deserializer<'de> {
    input: &'de [u8],
    nested: usize,
    is_versionstamp: bool,
}

impl<'de> Deserializer<'de> {
    pub fn new(input: &'de [u8]) -> Self {
        Deserializer {
            input,
            nested: 0,
            is_versionstamp: false,
        }
    }
}

pub fn from_bytes<'a, T>(s: &'a [u8]) -> Result<T>
where
    T: Deserialize<'a>,
{
    let mut deserializer = Deserializer::new(s);
    let t = T::deserialize(&mut deserializer)?;
    if deserializer.input.len() == 0 {
        Ok(t)
    } else {
        Err(Error::TrailingBytes)
    }
}

macro_rules! sign_bit {
    ($type:ident) => {
        (1 << (::std::mem::size_of::<$type>() * 8 - 1))
    };
}

macro_rules! impl_parse_ux {
    ($parse_ux: ident, $ux: ident) => {
        fn $parse_ux(&mut self) -> Result<$ux> {
            const SZ: usize = mem::size_of::<$ux>();
            let found = self.parse_byte()?;
            if INTZERO <= found && found <= INTZERO + SZ as u8 {
                let n = (found - INTZERO) as usize;
                let bytes = self.parse_bytes(n)?;
                let mut arr = [0u8; ::std::mem::size_of::<$ux>()];
                (&mut arr[(SZ - n)..]).copy_from_slice(bytes);
                Ok($ux::from_be_bytes(arr))
            }
            else {
                Err(Error::BadCode { found, expected: None })
            }
        }
    };
}

macro_rules! impl_parse_ix {
    ($parse_ix: ident, $ix: ident) => {
        fn $parse_ix(&mut self) -> Result<$ix> {
            const SZ: usize = mem::size_of::<$ix>();
            let found = self.parse_byte()?;
            if INTZERO <= found && found <= INTZERO + SZ as u8 {
                let n = (found - INTZERO) as usize;
                let bytes = self.parse_bytes(n)?;
                let mut arr = [0u8; ::std::mem::size_of::<$ix>()];
                (&mut arr[(SZ - n)..]).copy_from_slice(bytes);
                Ok($ix::from_be_bytes(arr))
            }
            else if INTZERO - SZ as u8 <= found && found < INTZERO {
                let n = (INTZERO - found) as usize;
                let bytes = self.parse_bytes(n)?;
                let mut arr = [0u8; ::std::mem::size_of::<$ix>()];
                (&mut arr[(SZ - n)..]).copy_from_slice(bytes);
                let offset = (1 << (n * 8)) - 1;
                Ok($ix::from_be_bytes(arr) - offset)
            }
            else {
                Err(Error::BadCode { found, expected: None })
            }
        }
    };
}

macro_rules! impl_parse_fx {
    ($parse_fx: ident, $fx: ident, $parse_u32_fixed: ident, $ux: ident, $code: ident) => {
        fn $parse_u32_fixed(&mut self) -> Result<$ux> {
            let bytes = self.parse_bytes(::std::mem::size_of::<$ux>())?;
            let mut arr = [0u8; ::std::mem::size_of::<$ux>()];
            arr.copy_from_slice(bytes);
            Ok($ux::from_be_bytes(arr))
        }

        fn $parse_fx(&mut self) -> Result<$fx> {
            self.parse_code($code)?;
            let u = self.$parse_u32_fixed()?;
            Ok($fx::from_bits(if (u & sign_bit!($ux)) == 0 {
                u ^ ::std::$ux::MAX
            }
            else {
                u ^ sign_bit!($ux)
            }))
        }
    };
}

impl<'de> Deserializer<'de> {
    #[inline]
    fn parse_bytes(&mut self, num: usize) -> Result<&[u8]> {
        if self.input.len() < num {
            Err(Error::MissingBytes)
        } else {
            let r = &self.input[..num];
            self.input = &self.input[num..];
            Ok(r)
        }
    }

    fn parse_byte(&mut self) -> Result<u8> {
        if self.input.len() < 1 {
            Err(Error::MissingBytes)
        } else {
            let r = self.input[0];
            self.input = &self.input[1..];
            Ok(r)
        }
    }

    impl_parse_ux!(parse_u8, u8);
    impl_parse_ux!(parse_u16, u16);
    impl_parse_ux!(parse_u32, u32);
    impl_parse_ux!(parse_u64, u64);

    impl_parse_ix!(parse_i8, i8);
    impl_parse_ix!(parse_i16, i16);
    impl_parse_ix!(parse_i32, i32);
    impl_parse_ix!(parse_i64, i64);

    impl_parse_fx!(parse_f32, f32, parse_u32_fixed, u32, FLOAT);
    impl_parse_fx!(parse_f64, f64, parse_u64_fixed, u64, DOUBLE);

    fn parse_code(&mut self, expected: u8) -> Result<()> {
        let found = self.parse_byte()?;
        if found == expected {
            Ok(())
        } else {
            Err(Error::BadCode {
                found,
                expected: Some(expected),
            })
        }
    }

    fn parse_slice(&mut self) -> Result<Cow<'de, [u8]>> {
        let mut bytes = Vec::new();
        let mut pos = 0;
        for idx in memchr_iter(NIL, self.input) {
            let next_idx = idx + 1;
            if self.input.get(next_idx) == Some(&ESCAPE) {
                bytes.extend_from_slice(&self.input[pos..next_idx]);
                pos = next_idx + 1;
            } else {
                let slice = &self.input[pos..idx];
                self.input = &self.input[next_idx..];
                return Ok(if pos == 0 {
                    Cow::Borrowed(slice)
                } else {
                    bytes.extend_from_slice(slice);
                    Cow::Owned(bytes)
                });
            }
        }
        Err(Error::MissingBytes)
    }

    fn parse_string(&mut self) -> Result<Cow<'de, str>> {
        Ok(match self.parse_slice()? {
            Cow::Borrowed(slice) => {
                Cow::Borrowed(std::str::from_utf8(slice).map_err(|_| Error::BadStringFormat)?)
            }
            Cow::Owned(vec) => {
                Cow::Owned(String::from_utf8(vec).map_err(|_| Error::BadStringFormat)?)
            }
        })
    }

    fn push_tuple(&mut self) -> Result<()> {
        if self.nested > 0 {
            self.parse_code(NESTED)?;
        }
        self.nested += 1;
        Ok(())
    }

    fn pop_tuple(&mut self) -> Result<()> {
        self.nested -= 1;
        if self.nested > 0 {
            self.parse_code(NIL)?;
        }
        Ok(())
    }
}

macro_rules! impl_deserialize_x {
    ($deserialize_x: ident, $visit_: ident, $parse_x: ident) => {
        fn $deserialize_x<V>(self, visitor: V) -> Result<V::Value>
        where
            V: Visitor<'de>,
        {
            visitor.$visit_(self.$parse_x()?)
        }
    };
}

impl<'de, 'a> de::VariantAccess<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        de::DeserializeSeed::deserialize(seed, self)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        de::Deserializer::deserialize_tuple(self, len, visitor)
    }

    fn struct_variant<V>(self, fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: serde::de::Visitor<'de>,
    {
        de::Deserializer::deserialize_tuple(self, fields.len(), visitor)
    }
}

impl<'de, 'a> de::EnumAccess<'de> for &'a mut Deserializer<'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: de::DeserializeSeed<'de>,
    {
        let idx: u32 = de::Deserialize::deserialize(&mut *self)?;
        let val: Result<_> = seed.deserialize(idx.into_deserializer());
        Ok((val?, self))
    }
}

impl<'de, 'a> de::SeqAccess<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: de::DeserializeSeed<'de>,
    {
        match self.input.first() {
            None => {
                self.pop_tuple()?;
                return Ok(None);
            }
            Some(&NIL) => {
                if Some(&ESCAPE) != self.input.get(1) {
                    self.pop_tuple()?;
                    return Ok(None);
                }
            }
            _ => (),
        }

        seed.deserialize(&mut **self).map(Some)
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        const INTI64: u8 = INTZERO - 8;
        const INTU64: u8 = INTZERO + 8;

        let first = self.input.first().ok_or(Error::MissingBytes)?;
        match *first {
            NIL => self.deserialize_option(visitor),
            BYTES => self.deserialize_bytes(visitor),
            STRING => self.deserialize_str(visitor),
            NESTED => self.deserialize_seq(visitor),
            INTZERO..=INTU64 => self.deserialize_u64(visitor),
            INTI64..=INTZERO => self.deserialize_i64(visitor),
            FLOAT => self.deserialize_f32(visitor),
            DOUBLE => self.deserialize_f64(visitor),
            FALSE | TRUE => self.deserialize_bool(visitor),
            VERSIONSTAMP => self.deserialize_newtype_struct("Versionstamp", visitor),
            found => Err(Error::BadCode {
                found,
                expected: None,
            }),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let v = self.parse_byte()?;
        visitor.visit_bool(match v {
            FALSE => false,
            TRUE => true,
            _ => Err(Error::Message(format!("{} is not a valid bool value", v)))?,
        })
    }

    impl_deserialize_x!(deserialize_u8, visit_u8, parse_u8);
    impl_deserialize_x!(deserialize_u16, visit_u16, parse_u16);
    impl_deserialize_x!(deserialize_u32, visit_u32, parse_u32);
    impl_deserialize_x!(deserialize_u64, visit_u64, parse_u64);

    impl_deserialize_x!(deserialize_i8, visit_i8, parse_i8);
    impl_deserialize_x!(deserialize_i16, visit_i16, parse_i16);
    impl_deserialize_x!(deserialize_i32, visit_i32, parse_i32);
    impl_deserialize_x!(deserialize_i64, visit_i64, parse_i64);

    impl_deserialize_x!(deserialize_f32, visit_f32, parse_f32);
    impl_deserialize_x!(deserialize_f64, visit_f64, parse_f64);

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let v = self.parse_u32()?;
        visitor.visit_char(char::try_from(v).map_err(|_| Error::BadCharValue(v))?)
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.parse_code(STRING)?;
        match self.parse_string()? {
            Cow::Borrowed(slice) => visitor.visit_borrowed_str(slice),
            Cow::Owned(string) => visitor.visit_string(string),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if self.is_versionstamp {
            self.parse_code(VERSIONSTAMP)?;
            let bytes = self.parse_bytes(12)?;
            visitor.visit_bytes(bytes)
        } else {
            self.parse_code(BYTES)?;
            match self.parse_slice()? {
                Cow::Borrowed(slice) => visitor.visit_borrowed_bytes(slice),
                Cow::Owned(bytes) => visitor.visit_byte_buf(bytes),
            }
        }
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if let Some(&NIL) = self.input.first() {
            self.parse_code(NIL)?;
            if self.nested > 1 {
                self.parse_code(ESCAPE)?;
            }
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    // The type of `()` in Rust. It represents an anonymous value containing no data.
    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.parse_code(NESTED)?;
        self.parse_code(NIL)?;
        visitor.visit_unit()
    }

    // For example `struct Unit` or `PhantomData<T>`. It represents a named value containing no data.
    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    // For example `struct Millimeters(u8).`
    fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.is_versionstamp = name == "Versionstamp";
        let r = visitor.visit_newtype_struct(&mut *self);
        self.is_versionstamp = false;
        r
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.push_tuple()?;
        visitor.visit_seq(self)
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        struct Access<'a, 'de: 'a> {
            de: &'a mut Deserializer<'de>,
            len: usize,
        }

        impl<'de, 'a> de::SeqAccess<'de> for Access<'a, 'de> {
            type Error = Error;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
            where
                T: de::DeserializeSeed<'de>,
            {
                seed.deserialize(&mut *self.de).map(Some)
            }

            fn size_hint(&self) -> Option<usize> {
                Some(self.len)
            }
        }

        self.push_tuple()?;
        let v = visitor.visit_seq(Access { de: self, len })?;
        self.pop_tuple()?;
        Ok(v)
    }

    // A named tuple, for example `struct Rgb(u8, u8, u8)`.
    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(Error::NotSupported("deserialize_map"))
    }

    // A statically sized heterogeneous key-value pairing in which the keys are
    // compile-time constant strings and will be known at deserialization time
    // without looking at the serialized data, for example
    // `struct S { r: u8, g: u8, b: u8 }`
    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_tuple(fields.len(), visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(Error::NotSupported("deserialize_identifier"))
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(Error::NotSupported("deserialize_ignored_any"))
    }

    #[inline]
    fn is_human_readable(&self) -> bool {
        false
    }
}
