use crate::error::{Error, Result};

pub const MAX_KEY_SIZE: usize = 1024;

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct EntryKey(Box<[u8]>);

impl EntryKey {
    pub fn new(bytes: impl AsRef<[u8]>) -> Result<Self> {
        let bytes = bytes.as_ref();
        Self::validate(bytes)?;
        Ok(Self(bytes.into()))
    }

    pub(crate) fn validate(bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() {
            return Err(Error::EmptyKey);
        }
        if bytes.len() > MAX_KEY_SIZE {
            return Err(Error::KeyTooLarge {
                len: bytes.len(),
                maximum: MAX_KEY_SIZE,
            });
        }
        Ok(())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<[u8]> for EntryKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct KeyDigest([u8; 24]);

impl KeyDigest {
    pub(crate) fn for_key(key: &EntryKey) -> Self {
        let hash = blake3::hash(key.as_bytes());
        let mut digest = [0; 24];
        digest.copy_from_slice(&hash.as_bytes()[..24]);
        Self(digest)
    }

    #[cfg(test)]
    pub(crate) const fn new(bytes: [u8; 24]) -> Self {
        Self(bytes)
    }

    pub(crate) const fn as_bytes(&self) -> &[u8; 24] {
        &self.0
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum CachePriority {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
}

impl CachePriority {
    pub const fn to_byte(self) -> u8 {
        self as u8
    }

    pub const fn from_byte(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Low),
            1 => Some(Self::Normal),
            2 => Some(Self::High),
            _ => None,
        }
    }
}
