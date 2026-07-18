use crc_fast::{CrcAlgorithm, Digest};

use crate::model::{BlobKey, MAX_BLOB_KEY_SIZE};

pub const PAGE_SIZE: usize = 4 * 1024;
pub const DEFAULT_SLOT_SIZE: usize = 64 * 1024;

const BLOB_MAGIC: [u8; 4] = *b"SCBL";
const BLOB_VERSION: u8 = 1;
pub(crate) const BLOB_HEADER_SIZE: usize = 16;

pub fn value_checksum(value: &[u8]) -> u32 {
    crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, value) as u32
}

pub(crate) fn stored_blob_len(key: &BlobKey, value: &[u8]) -> Option<usize> {
    u16::try_from(key.len()).ok()?;
    u32::try_from(value.len()).ok()?;
    let len = BLOB_HEADER_SIZE.checked_add(key.len())?.checked_add(value.len())?;
    u32::try_from(len).ok()?;
    Some(len)
}

pub(crate) fn blob_checksum(key: &BlobKey, value: &[u8]) -> u32 {
    let header = blob_header(key, value);
    let mut digest = Digest::new(CrcAlgorithm::Crc32IsoHdlc);
    digest.update(&header);
    digest.update(key.as_bytes());
    digest.update(value);
    u32::try_from(digest.finalize()).expect("CRC-32 digest must fit u32")
}

pub(crate) fn copy_blob_range(key: &BlobKey, value: &[u8], start: usize, output: &mut [u8]) -> bool {
    let Some(end) = start.checked_add(output.len()) else {
        return false;
    };
    let Some(total_len) = stored_blob_len(key, value) else {
        return false;
    };
    if end > total_len {
        return false;
    }

    let header = blob_header(key, value);
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

pub(crate) fn decode_blob(stored: Vec<u8>, expected_key: &BlobKey) -> Option<Vec<u8>> {
    let (key_len, value_len) = decode_blob_header(&stored)?;
    let value_offset = BLOB_HEADER_SIZE.checked_add(key_len)?;
    if stored.get(BLOB_HEADER_SIZE..value_offset)? != expected_key.as_bytes() {
        return None;
    }
    extract_value(stored, value_offset, value_len)
}

pub(crate) fn decode_stored_blob(stored: Vec<u8>) -> Option<(BlobKey, Vec<u8>)> {
    let (key_len, value_len) = decode_blob_header(&stored)?;
    let value_offset = BLOB_HEADER_SIZE.checked_add(key_len)?;
    let key = BlobKey::new(stored.get(BLOB_HEADER_SIZE..value_offset)?).ok()?;
    let value = extract_value(stored, value_offset, value_len)?;
    Some((key, value))
}

fn blob_header(key: &BlobKey, value: &[u8]) -> [u8; BLOB_HEADER_SIZE] {
    let mut header = [0; BLOB_HEADER_SIZE];
    header[..4].copy_from_slice(&BLOB_MAGIC);
    header[4] = BLOB_VERSION;
    header[6..8].copy_from_slice(
        &u16::try_from(key.len())
            .expect("validated blob key length must fit u16")
            .to_le_bytes(),
    );
    header[8..12].copy_from_slice(
        &u32::try_from(value.len())
            .expect("validated blob value length must fit u32")
            .to_le_bytes(),
    );
    header
}

fn decode_blob_header(stored: &[u8]) -> Option<(usize, usize)> {
    if stored.len() < BLOB_HEADER_SIZE
        || stored[..4] != BLOB_MAGIC
        || stored[4] != BLOB_VERSION
        || stored[5] != 0
        || stored[12..16] != [0; 4]
    {
        return None;
    }
    let key_len = u16::from_le_bytes(stored[6..8].try_into().ok()?) as usize;
    let value_len = u32::from_le_bytes(stored[8..12].try_into().ok()?) as usize;
    let expected_len = BLOB_HEADER_SIZE.checked_add(key_len)?.checked_add(value_len)?;
    (key_len <= MAX_BLOB_KEY_SIZE && value_len > 0 && expected_len == stored.len()).then_some((key_len, value_len))
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
