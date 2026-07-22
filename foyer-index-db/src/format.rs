use std::{fs::File, io, path::Path};

use crc_fast::CrcAlgorithm;

#[cfg(unix)]
use crate::error::Error;
use crate::error::Result;

pub const KEY_SIZE: usize = 24;
pub const VALUE_SIZE: usize = 32;
pub const RECORD_SIZE: usize = 64;
pub const DATA_BLOCK_SIZE: usize = 8 * 1024;
pub const DATA_BLOCK_HEADER_SIZE: usize = 64;
pub const RECORDS_PER_BLOCK: usize = (DATA_BLOCK_SIZE - DATA_BLOCK_HEADER_SIZE) / RECORD_SIZE;
pub const TABLE_FOOTER_SIZE: usize = 4 * 1024;
pub const MAX_SEQUENCE: u64 = (1_u64 << 63) - 1;
const TOMBSTONE_BIT: u64 = 1_u64 << 63;

pub type Key = [u8; KEY_SIZE];
pub type Value = [u8; VALUE_SIZE];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Record {
    pub key: Key,
    pub value: Option<Value>,
    pub sequence: u64,
}

impl Record {
    pub fn put(key: Key, value: Value, sequence: u64) -> Self {
        debug_assert!((1..=MAX_SEQUENCE).contains(&sequence));
        Self {
            key,
            value: Some(value),
            sequence,
        }
    }

    pub fn delete(key: Key, sequence: u64) -> Self {
        debug_assert!((1..=MAX_SEQUENCE).contains(&sequence));
        Self {
            key,
            value: None,
            sequence,
        }
    }

    pub fn encode(self) -> [u8; RECORD_SIZE] {
        let mut output = [0; RECORD_SIZE];
        output[..KEY_SIZE].copy_from_slice(&self.key);
        if let Some(value) = self.value {
            output[KEY_SIZE..KEY_SIZE + VALUE_SIZE].copy_from_slice(&value);
        }
        let sequence = self.sequence | (u64::from(self.value.is_none()) * TOMBSTONE_BIT);
        put_u64(&mut output, KEY_SIZE + VALUE_SIZE, sequence);
        output
    }

    pub fn decode(input: &[u8]) -> Option<Self> {
        if input.len() != RECORD_SIZE {
            return None;
        }
        let sequence_word = get_u64(input, KEY_SIZE + VALUE_SIZE);
        let sequence = sequence_word & MAX_SEQUENCE;
        if sequence == 0 {
            return None;
        }
        let mut key = [0; KEY_SIZE];
        key.copy_from_slice(&input[..KEY_SIZE]);
        let value = if sequence_word & TOMBSTONE_BIT == 0 {
            let mut value = [0; VALUE_SIZE];
            value.copy_from_slice(&input[KEY_SIZE..KEY_SIZE + VALUE_SIZE]);
            Some(value)
        } else {
            None
        };
        Some(Self { key, value, sequence })
    }
}

pub fn checksum(input: &[u8]) -> u32 {
    crc_fast::checksum(CrcAlgorithm::Crc32Iscsi, input) as u32
}

pub fn put_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + size_of::<u32>()].copy_from_slice(&value.to_le_bytes());
}

pub fn put_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + size_of::<u64>()].copy_from_slice(&value.to_le_bytes());
}

pub fn get_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(input[offset..offset + size_of::<u32>()].try_into().unwrap())
}

pub fn get_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(input[offset..offset + size_of::<u64>()].try_into().unwrap())
}

pub fn read_exact_at(file: &File, mut output: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !output.is_empty() {
        let read = read_at(file, output, offset)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "file ended before the requested IndexDB range",
            ));
        }
        output = &mut output[read..];
        offset = offset
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("IndexDB read offset overflow"))?;
    }
    Ok(())
}

#[cfg(unix)]
fn read_at(file: &File, output: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, output, offset)
}

#[cfg(windows)]
fn read_at(file: &File, output: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, output, offset)
}

#[cfg(unix)]
pub fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| Error::io("sync database directory", error))
}

#[cfg(windows)]
pub fn sync_directory(_path: &Path) -> Result<()> {
    // Extent's production durability contract is Linux-only. std does not expose a portable
    // Windows directory fsync, and this cache remains non-authoritative on development hosts.
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::format::{MAX_SEQUENCE, Record};

    #[test]
    fn record_roundtrips_values_and_tombstones() {
        let key = [7; 24];
        let value = [0; 32];
        let put = Record::put(key, value, MAX_SEQUENCE);
        assert_eq!(Record::decode(&put.encode()), Some(put));

        let delete = Record::delete(key, 1);
        assert_eq!(Record::decode(&delete.encode()), Some(delete));
    }
}
