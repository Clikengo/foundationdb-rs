use super::{Bytes, Element};
use std::fmt;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Versionstamp {
    bytes: [u8; 12],
}

impl<'a> fmt::Debug for Versionstamp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Bytes::from(&self.bytes[..]).fmt(f)
    }
}

impl Versionstamp {
    pub fn incomplete(user_version: u16) -> Self {
        let mut bytes = [0xff; 12];
        bytes[10..].copy_from_slice(&user_version.to_be_bytes());
        Versionstamp { bytes }
    }

    pub fn complete(tr_version: [u8; 10], user_version: u16) -> Self {
        let mut bytes = [0xff; 12];
        bytes[0..10].copy_from_slice(&tr_version);
        bytes[10..].copy_from_slice(&user_version.to_be_bytes());
        Versionstamp { bytes }
    }

    pub fn transaction_version(&self) -> &[u8] {
        &self.bytes[0..10]
    }

    pub fn user_version(&self) -> u16 {
        let mut user_version = [0; 2];
        user_version.copy_from_slice(&self.bytes[10..12]);
        u16::from_be_bytes(user_version)
    }

    pub fn is_complete(&self) -> bool {
        self.bytes[0..10] != [0xff; 10]
    }

    pub fn bytes(&self) -> &[u8; 12] {
        &self.bytes
    }
}

impl From<[u8; 12]> for Versionstamp {
    fn from(bytes: [u8; 12]) -> Self {
        Versionstamp { bytes }
    }
}
impl Into<[u8; 12]> for Versionstamp {
    fn into(self) -> [u8; 12] {
        self.bytes
    }
}

impl<'a> serde::Serialize for Versionstamp {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_newtype_struct("Versionstamp", &Bytes::from(&self.bytes()[..]))
    }
}

impl<'a> Element<'a> {
    pub fn count_incomplete_versionstamp(&self) -> usize {
        match self {
            Element::Versionstamp(v) if !v.is_complete() => 1,
            Element::Tuple(v) => v.iter().map(Element::count_incomplete_versionstamp).sum(),
            _ => 0,
        }
    }
}

pub(super) struct VersionstampVisitor;

impl<'de> serde::de::Visitor<'de> for VersionstampVisitor {
    type Value = Versionstamp;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a versionstamp")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.len() != 12 {
            return Err(E::custom(format!(
                "versionstamp bytes len is not 12: {}",
                v.len()
            )));
        }

        let mut bytes = [0xff; 12];
        bytes.copy_from_slice(v);
        Ok(Versionstamp::from(bytes))
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(self)
    }
}

impl<'de> serde::Deserialize<'de> for Versionstamp {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Versionstamp, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_newtype_struct("Versionstamp", VersionstampVisitor)
    }
}
