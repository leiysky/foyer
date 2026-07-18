use std::io::{Read, Write};

use bytes::Bytes;
use foyer::{Code, Error as FoyerError, ErrorKind as FoyerErrorKind};

use crate::{CachePriority, Error, Result, model::BlobKey};

const ENGINE_VALUE_HEADER_SIZE: usize = 9;

/// A complete logical object stored by the cache.
///
/// Cloning an entry clones its shared byte handles, not the key or value bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    key: Bytes,
    value: Bytes,
    priority: CachePriority,
}

impl Entry {
    pub fn new(key: impl Into<Bytes>, value: impl Into<Bytes>, priority: CachePriority) -> Result<Self> {
        let key = key.into();
        BlobKey::validate(&key)?;
        let value = value.into();
        if value.is_empty() {
            return Err(Error::EmptyValue);
        }
        Ok(Self { key, value, priority })
    }

    pub fn key(&self) -> &Bytes {
        &self.key
    }

    pub fn value(&self) -> &Bytes {
        &self.value
    }

    pub const fn priority(&self) -> CachePriority {
        self.priority
    }

    pub fn into_parts(self) -> (Bytes, Bytes, CachePriority) {
        (self.key, self.value, self.priority)
    }

    pub(crate) fn from_engine(key: Bytes, value: EngineValue) -> Self {
        Self {
            key,
            value: value.value,
            priority: value.priority,
        }
    }

    pub(crate) fn into_engine(self) -> (Bytes, EngineValue) {
        let Self { key, value, priority } = self;
        (key, EngineValue { value, priority })
    }
}

/// The value half used at the Foyer engine boundary.
///
/// Most callers should use [`Entry`]. This type is public so an Extent engine can be installed
/// directly in `foyer::HybridCache<Bytes, EngineValue>` without another adapter value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineValue {
    value: Bytes,
    priority: CachePriority,
}

impl EngineValue {
    pub fn new(value: impl Into<Bytes>, priority: CachePriority) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(Error::EmptyValue);
        }
        Ok(Self { value, priority })
    }

    pub fn value(&self) -> &Bytes {
        &self.value
    }

    pub const fn priority(&self) -> CachePriority {
        self.priority
    }

    pub fn into_value(self) -> Bytes {
        self.value
    }
}

impl Code for EngineValue {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        writer
            .write_all(&[self.priority.to_byte()])
            .map_err(FoyerError::io_error)?;
        writer
            .write_all(&(self.value.len() as u64).to_le_bytes())
            .map_err(FoyerError::io_error)?;
        writer.write_all(&self.value).map_err(FoyerError::io_error)
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self> {
        let mut header = [0; ENGINE_VALUE_HEADER_SIZE];
        reader.read_exact(&mut header).map_err(FoyerError::io_error)?;
        let priority = CachePriority::from_byte(header[0])
            .ok_or_else(|| FoyerError::new(FoyerErrorKind::Parse, "invalid extent cache priority"))?;
        let len = usize::try_from(u64::from_le_bytes(header[1..].try_into().unwrap()))
            .map_err(|_| FoyerError::new(FoyerErrorKind::OutOfRange, "extent value is too large"))?;
        if len == 0 {
            return Err(FoyerError::new(
                FoyerErrorKind::Parse,
                "extent cache value must not be empty",
            ));
        }
        let mut value = vec![0; len];
        reader.read_exact(&mut value).map_err(FoyerError::io_error)?;
        Ok(Self {
            value: Bytes::from(value),
            priority,
        })
    }

    fn estimated_size(&self) -> usize {
        ENGINE_VALUE_HEADER_SIZE.saturating_add(self.value.len())
    }
}
