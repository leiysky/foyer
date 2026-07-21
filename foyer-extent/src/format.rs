#[cfg(test)]
use crc_fast::{CrcAlgorithm, Digest};
use twox_hash::XxHash3_128;

use crate::model::{EntryKey, MAX_KEY_SIZE};

pub const PAGE_SIZE: usize = 4 * 1024;
/// Persisted value identity used for idempotence and payload validation.
///
/// Eleven bytes fit both fixed metadata records without increasing their size. The records retain
/// a separate CRC for torn-write detection, while complete keys are still verified on every hit.
pub(crate) const CONTENT_DIGEST_SIZE: usize = 11;
pub(crate) type ContentDigest = [u8; CONTENT_DIGEST_SIZE];
const CONTENT_DIGEST_SEED: u64 = 0x4f1b_bcdd_94d0_49bb;
/// The planning charge used to size Entry-directory and index targets.
///
/// Stored Entries are packed by byte inside an extent. This charge is neither a physical
/// allocation unit nor an Entry-count limit.
pub const DEFAULT_ENTRY_CHARGE: usize = PAGE_SIZE;

const STORED_ENTRY_MAGIC: [u8; 4] = *b"SCBL";
const STORED_ENTRY_VERSION: u8 = 1;
pub(crate) const STORED_ENTRY_HEADER_SIZE: usize = 16;
/// Smallest valid Stored Entry: header, one-byte key, and one-byte value.
pub(crate) const MIN_STORED_ENTRY_SIZE: usize = STORED_ENTRY_HEADER_SIZE + 2;

pub(crate) fn stored_entry_len(key: &EntryKey, value: &[u8]) -> Option<usize> {
    u16::try_from(key.len()).ok()?;
    u32::try_from(value.len()).ok()?;
    let len = STORED_ENTRY_HEADER_SIZE
        .checked_add(key.len())?
        .checked_add(value.len())?;
    u32::try_from(len).ok()?;
    Some(len)
}

pub(crate) fn value_digest(value: &[u8]) -> ContentDigest {
    let digest = XxHash3_128::oneshot_with_seed(CONTENT_DIGEST_SEED, value).to_le_bytes();
    let mut output = [0; CONTENT_DIGEST_SIZE];
    output.copy_from_slice(&digest[..CONTENT_DIGEST_SIZE]);
    output
}

pub(crate) fn encoded_entry_value_digest(stored: &[u8]) -> Option<ContentDigest> {
    let (key_len, value_len) = decode_stored_entry_header(stored)?;
    let value_offset = STORED_ENTRY_HEADER_SIZE.checked_add(key_len)?;
    let value_end = value_offset.checked_add(value_len)?;
    Some(value_digest(stored.get(value_offset..value_end)?))
}

#[cfg(test)]
pub(crate) fn stored_entry_checksum(key: &EntryKey, value: &[u8]) -> u32 {
    let header = stored_entry_header(key, value);
    let mut digest = Digest::new(CrcAlgorithm::Crc32IsoHdlc);
    digest.update(&header);
    digest.update(key.as_bytes());
    digest.update(value);
    u32::try_from(digest.finalize()).expect("CRC-32 digest must fit u32")
}

pub(crate) fn copy_stored_entry_range(key: &EntryKey, value: &[u8], start: usize, output: &mut [u8]) -> bool {
    let Some(end) = start.checked_add(output.len()) else {
        return false;
    };
    let Some(total_len) = stored_entry_len(key, value) else {
        return false;
    };
    if end > total_len {
        return false;
    }

    let header = stored_entry_header(key, value);
    let parts = [&header[..], key.as_bytes(), value];
    let mut input_offset = start;
    let mut output_offset = 0usize;
    for part in parts {
        if input_offset >= part.len() {
            input_offset -= part.len();
            continue;
        }
        let len = (part.len() - input_offset).min(output.len() - output_offset);
        output[output_offset..output_offset + len].copy_from_slice(&part[input_offset..input_offset + len]);
        output_offset += len;
        input_offset = 0;
        if output_offset == output.len() {
            return true;
        }
    }
    output.is_empty()
}

pub(crate) fn decode_entry_value(stored: Vec<u8>, expected_key: &EntryKey) -> Option<Vec<u8>> {
    let (key_len, value_len) = decode_stored_entry_header(&stored)?;
    let value_offset = STORED_ENTRY_HEADER_SIZE.checked_add(key_len)?;
    if stored.get(STORED_ENTRY_HEADER_SIZE..value_offset)? != expected_key.as_bytes() {
        return None;
    }
    extract_value(stored, value_offset, value_len)
}

pub(crate) fn decode_stored_entry(stored: Vec<u8>) -> Option<(EntryKey, Vec<u8>)> {
    let (key_len, value_len) = decode_stored_entry_header(&stored)?;
    let value_offset = STORED_ENTRY_HEADER_SIZE.checked_add(key_len)?;
    let key = EntryKey::new(stored.get(STORED_ENTRY_HEADER_SIZE..value_offset)?).ok()?;
    let value = extract_value(stored, value_offset, value_len)?;
    Some((key, value))
}

fn stored_entry_header(key: &EntryKey, value: &[u8]) -> [u8; STORED_ENTRY_HEADER_SIZE] {
    let mut header = [0; STORED_ENTRY_HEADER_SIZE];
    header[..4].copy_from_slice(&STORED_ENTRY_MAGIC);
    header[4] = STORED_ENTRY_VERSION;
    header[6..8].copy_from_slice(
        &u16::try_from(key.len())
            .expect("validated entry key length must fit u16")
            .to_le_bytes(),
    );
    header[8..12].copy_from_slice(
        &u32::try_from(value.len())
            .expect("validated entry value length must fit u32")
            .to_le_bytes(),
    );
    header
}

fn decode_stored_entry_header(stored: &[u8]) -> Option<(usize, usize)> {
    if stored.len() < STORED_ENTRY_HEADER_SIZE
        || stored[..4] != STORED_ENTRY_MAGIC
        || stored[4] != STORED_ENTRY_VERSION
        || stored[5] != 0
        || stored[12..16] != [0; 4]
    {
        return None;
    }
    let key_len = u16::from_le_bytes(stored[6..8].try_into().ok()?) as usize;
    let value_len = u32::from_le_bytes(stored[8..12].try_into().ok()?) as usize;
    let expected_len = STORED_ENTRY_HEADER_SIZE.checked_add(key_len)?.checked_add(value_len)?;
    (key_len <= MAX_KEY_SIZE && value_len > 0 && expected_len == stored.len()).then_some((key_len, value_len))
}

fn extract_value(mut stored: Vec<u8>, value_offset: usize, value_len: usize) -> Option<Vec<u8>> {
    let value_end = value_offset.checked_add(value_len)?;
    if value_end != stored.len() {
        return None;
    }
    stored.copy_within(value_offset..value_end, 0);
    stored.truncate(value_len);
    Some(stored)
}
