use super::{Bytes, Versionstamp};
use serde::de;
use serde::ser::SerializeSeq;
use serde::{Deserializer, Serializer};
use std::borrow::Cow;
use std::convert::TryFrom;
use std::fmt;
use std::result::Result;

#[derive(Clone, PartialEq, Debug)]
pub enum Element<'a> {
    Nil,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(Cow<'a, str>),
    Bytes(Bytes<'a>),
    Versionstamp(Versionstamp),
    Tuple(Vec<Element<'a>>),
}

impl<'a> Element<'a> {
    pub fn into_owned(self) -> Element<'static> {
        match self {
            Element::Nil => Element::Nil,
            Element::Bool(v) => Element::Bool(v),
            Element::Int(v) => Element::Int(v),
            Element::UInt(v) => Element::UInt(v),
            Element::Float(v) => Element::Float(v),
            Element::String(v) => Element::String(Cow::Owned(v.into_owned())),
            Element::Bytes(v) => Element::Bytes(v.into_owned().into()),
            Element::Versionstamp(v) => Element::Versionstamp(v),
            Element::Tuple(v) => Element::Tuple(v.into_iter().map(|e| e.into_owned()).collect()),
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            &Element::Bool(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            &Element::Int(v) => Some(v),
            &Element::UInt(v) => i64::try_from(v).ok(),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            &Element::Int(v) => u64::try_from(v).ok(),
            &Element::UInt(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Element::String(v) => Some(&v),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            Element::Bytes(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_tuple(&self) -> Option<&[Element<'a>]> {
        match self {
            Element::Tuple(v) => Some(v.as_slice()),
            _ => None,
        }
    }
}

impl<'a> serde::Serialize for Element<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            &Element::Nil => serializer.serialize_none(),
            &Element::Bool(b) => serializer.serialize_bool(b),
            &Element::Int(i) => serializer.serialize_i64(i),
            &Element::UInt(u) => serializer.serialize_u64(u),
            &Element::Float(f) => serializer.serialize_f64(f),
            &Element::String(ref c) => serializer.serialize_str(&c),
            &Element::Bytes(ref b) => serializer.serialize_bytes(&b),
            &Element::Versionstamp(ref b) => {
                serializer.serialize_newtype_struct("Versionstamp", &b)
            }
            &Element::Tuple(ref v) => {
                let mut seq = serializer.serialize_seq(Some(v.len()))?;
                for element in v {
                    seq.serialize_element(element)?;
                }
                seq.end()
            }
        }
    }
}

struct ElementVisitor;

impl<'a> de::Visitor<'a> for ElementVisitor {
    type Value = Element<'a>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a borrowed byte array")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Element::Bool(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Element::Int(v))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Element::UInt(v))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Element::Float(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Element::String(Cow::Owned(v.to_owned())))
    }

    fn visit_borrowed_str<E>(self, v: &'a str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Element::String(Cow::Borrowed(v)))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Element::String(Cow::Owned(v)))
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Element::Bytes(Bytes(Cow::Owned(v.to_owned()))))
    }

    fn visit_borrowed_bytes<E>(self, v: &'a [u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Element::Bytes(Bytes(Cow::Borrowed(v))))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Element::Bytes(Bytes(Cow::Owned(v))))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Element::Nil)
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::de::Deserializer<'a>,
    {
        Ok(Element::Versionstamp(deserializer.deserialize_bytes(
            super::versionstamp::VersionstampVisitor,
        )?))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'a>,
    {
        let mut tuple = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(element) = seq.next_element()? {
            tuple.push(element)
        }
        Ok(Element::Tuple(tuple))
    }
}

impl<'de: 'a, 'a> serde::Deserialize<'de> for Element<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(ElementVisitor)
    }
}
