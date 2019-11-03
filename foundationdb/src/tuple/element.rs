use super::{Bytes, Versionstamp};
use std::borrow::Cow;

#[derive(Clone, PartialEq, Debug)]
pub enum Element<'a> {
    Nil,
    Bytes(Bytes<'a>),
    String(Cow<'a, str>),
    Tuple(Vec<Element<'a>>),
    Int(i64),
    Float(f32),
    Double(f64),
    Bool(bool),
    #[cfg(feature = "uuid")]
    Uuid(uuid::Uuid),
    Versionstamp(Versionstamp),
}

impl<'a> Element<'a> {
    pub fn into_owned(self) -> Element<'static> {
        match self {
            Element::Nil => Element::Nil,
            Element::Bool(v) => Element::Bool(v),
            Element::Int(v) => Element::Int(v),
            Element::Float(v) => Element::Float(v),
            Element::Double(v) => Element::Double(v),
            Element::String(v) => Element::String(Cow::Owned(v.into_owned())),
            Element::Bytes(v) => Element::Bytes(v.into_owned().into()),
            Element::Versionstamp(v) => Element::Versionstamp(v),
            Element::Tuple(v) => Element::Tuple(v.into_iter().map(|e| e.into_owned()).collect()),
            #[cfg(feature = "uuid")]
            Element::Uuid(v) => Element::Uuid(v),
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
