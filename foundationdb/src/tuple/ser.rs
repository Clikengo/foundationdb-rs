use super::*;
use memchr::memchr_iter;
use serde::ser::{self, Serialize};
use std::io::Write;
use std::mem;
pub struct Serializer<W> {
    pub output: W,
    nested: usize,
    is_versionstamp: bool,
}

impl<W> Serializer<W> {
    pub fn new(output: W) -> Self {
        Serializer {
            output,
            nested: 0,
            is_versionstamp: false,
        }
    }
}

pub fn to_bytes<T>(value: &T) -> Result<Vec<u8>>
where
    T: Serialize,
{
    let mut bytes = Vec::new();
    {
        let mut serializer = Serializer::new(&mut bytes);
        value.serialize(&mut serializer)?;
    }
    Ok(bytes)
}

pub fn into_bytes<T>(value: &T, into: &mut Vec<u8>) -> Result<()>
where
    T: Serialize,
{
    let mut serializer = Serializer::new(into);
    value.serialize(&mut serializer)?;
    Ok(())
}

macro_rules! sign_bit {
    ($type:ident) => {
        (1 << (mem::size_of::<$type>() * 8 - 1))
    };
}

macro_rules! impl_serialize_ux {
    ($m: ident, $ux: ident) => {
        fn $m(self, u: $ux) -> Result<()> {
            const SZ: usize = mem::size_of::<$ux>();
            let n = SZ - (u.leading_zeros() as usize) / 8;
            self.output.write_all(&[INTZERO + n as u8])?;
            self.output.write_all(&u.to_be_bytes()[SZ-n..])?;
            Ok(())
        }
    };
}

macro_rules! impl_serialize_ix {
    ($m: ident, $ix: ident, $ux: ident) => {
        fn $m(self, i: $ix) -> Result<()> {
            const SZ: usize = mem::size_of::<$ix>();
            let u = i.wrapping_abs() as $ux;
            let n = SZ - (u.leading_zeros() as usize) / 8;
            let (code, arr) = if i >= 0 {
                (INTZERO + n as u8, u.to_be_bytes())
            }
            else {
                let i = i + ((1 << (n * 8)) - 1);
                (INTZERO - n as u8, i.to_be_bytes())
            };
            self.output.write_all(&[code])?;
            self.output.write_all(&arr[SZ-n..])?;

            Ok(())
        }
    };
}

macro_rules! impl_serialize_fx {
    ($m: ident, $fx: ident, $ux: ident, $type: ident) => {
        fn $m(self, f: $fx) -> Result<()> {
            let u = if f.is_sign_negative() {
                f.to_bits() ^ ::std::$ux::MAX
            } else {
                f.to_bits() ^ sign_bit!($ux)
            };
            self.output.write_all(&[$type])?;
            self.output.write_all(&u.to_be_bytes())?;
            Ok(())
        }
    };
}

impl<W: Write> Serializer<W> {
    fn write_bytes(&mut self, v: &[u8]) -> Result<()> {
        let mut pos = 0;
        for idx in memchr_iter(NIL, v) {
            let next_idx = idx + 1;
            self.output.write_all(&v[pos..next_idx])?;
            self.output.write_all(&[ESCAPE])?;
            pos = next_idx;
        }
        self.output.write_all(&v[pos..])?;
        self.output.write_all(&[NIL])?;
        Ok(())
    }

    fn push_tuple(&mut self) -> Result<&mut Self> {
        if self.nested > 0 {
            self.output.write_all(&[NESTED])?;
        }
        self.nested += 1;
        Ok(self)
    }

    fn pop_tuple(&mut self) -> Result<()> {
        self.nested -= 1;
        if self.nested > 0 {
            self.output.write_all(&[NIL])?;
        }
        Ok(())
    }
}

impl<'a, W: Write> ser::Serializer for &'a mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    // Here we go with the simple methods. The following 12 methods receive one
    // of the primitive types of the data model and map it to JSON by appending
    // into the output string.
    fn serialize_bool(self, v: bool) -> Result<()> {
        self.output.write_all(&[if v { TRUE } else { FALSE }])?;
        Ok(())
    }

    impl_serialize_ux!(serialize_u8, u8);
    impl_serialize_ux!(serialize_u16, u16);
    impl_serialize_ux!(serialize_u32, u32);
    impl_serialize_ux!(serialize_u64, u64);

    impl_serialize_ix!(serialize_i8, i8, u8);
    impl_serialize_ix!(serialize_i16, i16, u16);
    impl_serialize_ix!(serialize_i32, i32, u32);
    impl_serialize_ix!(serialize_i64, i64, u64);

    impl_serialize_fx!(serialize_f32, f32, u32, FLOAT);
    impl_serialize_fx!(serialize_f64, f64, u64, DOUBLE);

    fn serialize_char(self, v: char) -> Result<()> {
        self.serialize_u32(u32::from(v))
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        self.output.write_all(&[STRING])?;
        self.write_bytes(v.as_bytes())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        if self.is_versionstamp {
            match v.len() {
                12 => {
                    self.output.write_all(&[VERSIONSTAMP])?;
                    self.output.write_all(v)?;
                }
                _ => return Err(Error::BadVersionstamp),
            }
        } else {
            self.output.write_all(&[BYTES])?;
            self.write_bytes(v)?;
        }
        Ok(())
    }

    fn serialize_none(self) -> Result<()> {
        Ok(if self.nested > 1 {
            // Empty value in nested tuple is encoded with [NIL, ESCAPE] to
            // disambiguate itself with end-of-tuple marker.
            self.output.write_all(&[NIL, ESCAPE])
        } else {
            self.output.write_all(&[NIL])
        }?)
    }

    fn serialize_some<T>(self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    // The type of `()` in Rust. It represents an anonymous value containing no data.
    fn serialize_unit(self) -> Result<()> {
        Ok(self.output.write_all(&[NESTED, NIL])?)
    }

    // For example `struct Unit` or `PhantomData<T>`. It represents a named value containing no data.
    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        self.serialize_unit()
    }

    // For example the `E::A` and `E::B` in `enum E { A, B }`.
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        self.serialize_u32(variant_index)
    }

    // For example `struct Millimeters(u8).`
    fn serialize_newtype_struct<T>(self, name: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.is_versionstamp = name == "Versionstamp";
        let ret = value.serialize(&mut *self);
        self.is_versionstamp = false;
        ret
    }

    // For example the `E::N` in `enum E { N(u8) }`.
    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.output.write_all(&[ENUM])?;
        self.serialize_u32(variant_index)?;
        value.serialize(&mut *self)?;
        Ok(())
    }

    // A variably sized heterogeneous sequence of values, for example `Vec<T>`
    // or `HashSet<T>`. When serializing, the length may or may not be known
    // before iterating through all the data. When deserializing, the length is
    // determined by looking at the serialized data. Note that a homogeneous
    // Rust collection like `vec![Value::Bool(true), Value::Char('c')]` may
    // serialize as a heterogeneous Serde seq, in this case containing a Serde
    // bool followed by a Serde char.
    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        self.push_tuple()
    }

    // A statically sized heterogeneous sequence of values for which the length
    // will be known at deserialization time without looking at the serialized
    // data, for example `(u8,)` or `(String, u64, Vec<T>)` or `[u64; 10]`.
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        self.push_tuple()
    }

    // A named tuple, for example `struct Rgb(u8, u8, u8)`.
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.push_tuple()
    }

    // For example the `E::T` in `enum E { T(u8, u8) }`
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.output.write_all(&[ENUM])?;
        self.serialize_u32(variant_index)?;
        self.push_tuple()
    }

    // Maps are represented in JSON as `{ K: V, K: V, ... }`.
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Err(Error::NotSupported("serialize_map"))
    }

    // A statically sized heterogeneous key-value pairing in which the keys are
    // compile-time constant strings and will be known at deserialization time
    // without looking at the serialized data, for example
    // `struct S { r: u8, g: u8, b: u8 }`
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        self.push_tuple()
    }

    // For example the `E::S` in `enum E { S { r: u8, g: u8, b: u8 } }`
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        self.output.write_all(&[ENUM])?;
        self.serialize_u32(variant_index)?;
        self.push_tuple()
    }

    #[inline]
    fn is_human_readable(&self) -> bool {
        false
    }
}

impl<'a, W: Write> ser::SerializeSeq for &'a mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    // Serialize a single element of the sequence.
    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    // Close the sequence.
    fn end(self) -> Result<()> {
        self.pop_tuple()
    }
}

impl<'a, W: Write> ser::SerializeTuple for &'a mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.pop_tuple()
    }
}

impl<'a, W: Write> ser::SerializeTupleStruct for &'a mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.pop_tuple()
    }
}

impl<'a, W: Write> ser::SerializeTupleVariant for &'a mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.pop_tuple()
    }
}

impl<'a, W: Write> ser::SerializeStruct for &'a mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.pop_tuple()
    }
}

impl<'a, W: Write> ser::SerializeStructVariant for &'a mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.pop_tuple()
    }
}

impl<'a, W: Write> ser::SerializeMap for &'a mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T>(&mut self, _key: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        Err(Error::NotSupported("serialize_key"))
    }

    fn serialize_value<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        Err(Error::NotSupported("serialize_value"))
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}
