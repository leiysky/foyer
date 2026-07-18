use crc_fast::{CrcAlgorithm, Digest};

use crate::{
    error::{Error, Result},
    format::PAGE_SIZE,
    model::{CachePriority, KeyDigest},
    segment::config::SegmentEngineConfig,
};

pub const OWNER_RECORD_SIZE: usize = 64;
pub const LOCATION_RECORD_SIZE: usize = 32;
const STATE_ENTRY_SIZE: usize = 24;
const STATE_HEADER_SIZE: usize = 64;
const STATE_CHECKSUM_SIZE: usize = size_of::<u32>();
const STATE_MAGIC: [u8; 8] = *b"SCSEGST1";
const OWNER_MAGIC: [u8; 4] = *b"SCOW";
const LOCATION_MAGIC: [u8; 4] = *b"SCLO";
const FORMAT_VERSION: u32 = 3;
const NO_SEGMENT: u32 = u32::MAX;
const FIXED_LSM_INDEX_BYTES_PER_ENTRY: u64 = 76;
// One steady-state copy, one atomic compaction output, and one bounded WAL/L0 write tail.
const FIXED_LSM_INDEX_CAPACITY_COPIES: u64 = 3;
const FIXED_LSM_INDEX_OVERHEAD_PER_ENTRY: u64 = 16;
// Small indexes still create page-aligned WAL, manifest, and SST metadata during compaction.
const FIXED_LSM_INDEX_MINIMUM_OVERHEAD: u64 = 128 * 1024;
const FIXED_LSM_INDEX_MAXIMUM_OVERHEAD: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentLayout {
    pub slot_size: usize,
    pub segment_size: usize,
    pub slots_per_segment: u32,
    pub segment_count: u32,
    pub usable_entries: u64,
    pub index_capacity_bytes: u64,
    pub data_file_size: u64,
    pub owner_file_size: u64,
    pub state_copy_size: usize,
    pub total_file_size: u64,
}

impl SegmentLayout {
    pub fn create(config: SegmentEngineConfig) -> Result<Self> {
        let slot_size = config.slot_size;
        let segment_size = config.options.segment_size;
        if slot_size < PAGE_SIZE || !slot_size.is_multiple_of(PAGE_SIZE) {
            return Err(Error::InvalidConfig(format!(
                "segment slot_size must be a positive multiple of {PAGE_SIZE} bytes"
            )));
        }
        if segment_size < slot_size || !segment_size.is_multiple_of(slot_size) {
            return Err(Error::InvalidConfig(format!(
                "segment_size must be a positive multiple of slot_size ({slot_size})"
            )));
        }
        let slots_per_segment = segment_size / slot_size;
        let slots_per_segment = u32::try_from(slots_per_segment).map_err(|_| {
            Error::InvalidConfig("segment contains more physical slots than u32 can represent".to_string())
        })?;
        let maximum_segments = config.capacity_bytes / segment_size as u64;
        let maximum_segments = u32::try_from(maximum_segments).unwrap_or(u32::MAX);

        for segment_count in (5..=maximum_segments).rev() {
            let physical_entries = u64::from(segment_count) * u64::from(slots_per_segment);
            let usable_entries = physical_entries - u64::from(slots_per_segment);
            let index_capacity_bytes = index_capacity_for_entries(usable_entries)?;
            let data_file_size = physical_entries
                .checked_mul(slot_size as u64)
                .ok_or_else(|| invalid_layout("segment data file size overflows u64"))?;
            let owner_file_size = physical_entries
                .checked_mul(OWNER_RECORD_SIZE as u64)
                .ok_or_else(|| invalid_layout("segment owner file size overflows u64"))?;
            let state_copy_size = state_copy_size(segment_count)?;
            let total_file_size = index_capacity_bytes
                .checked_add(data_file_size)
                .and_then(|size| size.checked_add(owner_file_size))
                .and_then(|size| size.checked_add((state_copy_size * 2) as u64))
                .ok_or_else(|| invalid_layout("segment engine file size overflows u64"))?;
            if total_file_size <= config.capacity_bytes {
                return Ok(Self {
                    slot_size,
                    segment_size,
                    slots_per_segment,
                    segment_count,
                    usable_entries,
                    index_capacity_bytes,
                    data_file_size,
                    owner_file_size,
                    state_copy_size,
                    total_file_size,
                });
            }
        }

        Err(Error::InvalidConfig(format!(
            "segment engine capacity must fit at least five {segment_size}-byte segments and their persistent index"
        )))
    }

    pub fn physical_slot(self, segment: u32, slot: u32) -> Option<u64> {
        (segment < self.segment_count && slot < self.slots_per_segment)
            .then(|| u64::from(segment) * u64::from(self.slots_per_segment) + u64::from(slot))
    }

    pub fn segment_for_slot(self, physical_slot: u64) -> Option<(u32, u32)> {
        let segment = physical_slot / u64::from(self.slots_per_segment);
        let slot = physical_slot % u64::from(self.slots_per_segment);
        (segment < u64::from(self.segment_count)).then_some((segment as u32, slot as u32))
    }

    pub fn discover(
        state_copy: &[u8],
        data_file_size: u64,
        owner_file_size: u64,
        state_file_size: u64,
    ) -> Option<Self> {
        if state_copy.len() < STATE_HEADER_SIZE
            || state_copy[..8] != STATE_MAGIC
            || get_u32(state_copy, 8) != FORMAT_VERSION
        {
            return None;
        }
        let segment_count = get_u32(state_copy, 12);
        let slots_per_segment = get_u32(state_copy, 16);
        let slot_size = usize::try_from(get_u32(state_copy, 20)).ok()?;
        let segment_size = usize::try_from(get_u64(state_copy, 56)).ok()?;
        if segment_count < 5
            || slots_per_segment == 0
            || slot_size < PAGE_SIZE
            || !slot_size.is_multiple_of(PAGE_SIZE)
            || segment_size != slot_size.checked_mul(slots_per_segment as usize)?
        {
            return None;
        }

        let physical_entries = u64::from(segment_count) * u64::from(slots_per_segment);
        let usable_entries = physical_entries.checked_sub(u64::from(slots_per_segment))?;
        let expected_index = index_capacity_for_entries(usable_entries).ok()?;
        let expected_data = physical_entries.checked_mul(slot_size as u64)?;
        let expected_owners = physical_entries.checked_mul(OWNER_RECORD_SIZE as u64)?;
        let state_copy_size = state_copy_size(segment_count).ok()?;
        let expected_state = u64::try_from(state_copy_size.checked_mul(2)?).ok()?;
        if data_file_size != expected_data
            || owner_file_size != expected_owners
            || state_file_size != expected_state
            || state_copy.len() != state_copy_size
        {
            return None;
        }
        let total_file_size = expected_index
            .checked_add(expected_data)?
            .checked_add(expected_owners)?
            .checked_add(expected_state)?;
        Some(Self {
            slot_size,
            segment_size,
            slots_per_segment,
            segment_count,
            usable_entries,
            index_capacity_bytes: expected_index,
            data_file_size: expected_data,
            owner_file_size: expected_owners,
            state_copy_size,
            total_file_size,
        })
    }
}

fn index_capacity_for_entries(entries: u64) -> Result<u64> {
    let overhead = entries
        .checked_mul(FIXED_LSM_INDEX_OVERHEAD_PER_ENTRY)
        .map(|bytes| bytes.clamp(FIXED_LSM_INDEX_MINIMUM_OVERHEAD, FIXED_LSM_INDEX_MAXIMUM_OVERHEAD))
        .ok_or_else(|| invalid_layout("fixed LSM index overhead overflows u64"))?;
    entries
        .checked_mul(FIXED_LSM_INDEX_BYTES_PER_ENTRY)
        .and_then(|bytes| bytes.checked_mul(FIXED_LSM_INDEX_CAPACITY_COPIES))
        .and_then(|bytes| bytes.checked_add(overhead))
        .and_then(|bytes| bytes.checked_add(PAGE_SIZE as u64 - 1))
        .map(|bytes| bytes / PAGE_SIZE as u64 * PAGE_SIZE as u64)
        .ok_or_else(|| invalid_layout("fixed LSM index capacity overflows u64"))
}

fn state_copy_size(segment_count: u32) -> Result<usize> {
    let bytes = (segment_count as usize)
        .checked_mul(STATE_ENTRY_SIZE)
        .and_then(|bytes| bytes.checked_add(STATE_HEADER_SIZE + STATE_CHECKSUM_SIZE))
        .ok_or_else(|| invalid_layout("segment state table size overflows usize"))?;
    Ok(bytes.next_multiple_of(PAGE_SIZE))
}

fn invalid_layout(message: &str) -> Error {
    Error::InvalidConfig(message.to_string())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SegmentRole {
    Free = 0,
    Reserve = 1,
    Current = 2,
    Sealed = 3,
    ReclaimSource = 4,
    ReclaimTarget = 5,
}

impl SegmentRole {
    fn from_byte(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Free),
            1 => Some(Self::Reserve),
            2 => Some(Self::Current),
            3 => Some(Self::Sealed),
            4 => Some(Self::ReclaimSource),
            5 => Some(Self::ReclaimTarget),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentState {
    pub generation: u32,
    pub used: u32,
    pub sequence: u64,
    pub priority: CachePriority,
    pub role: SegmentRole,
}

impl SegmentState {
    const fn free() -> Self {
        Self {
            generation: 1,
            used: 0,
            sequence: 0,
            priority: CachePriority::Low,
            role: SegmentRole::Free,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocatorState {
    pub generation: u64,
    pub next_sequence: u64,
    pub current: [Option<u32>; 3],
    pub reserve: u32,
    pub segments: Vec<SegmentState>,
    pub active_page: u8,
}

impl AllocatorState {
    pub fn empty(layout: SegmentLayout) -> Self {
        let mut segments = vec![SegmentState::free(); layout.segment_count as usize];
        segments[0].role = SegmentRole::Reserve;
        Self {
            generation: 1,
            next_sequence: 1,
            current: [None; 3],
            reserve: 0,
            segments,
            active_page: 0,
        }
    }

    pub fn encode(&self, layout: SegmentLayout) -> Result<Vec<u8>> {
        self.validate(layout)?;
        let mut output = vec![0; layout.state_copy_size];
        output[..8].copy_from_slice(&STATE_MAGIC);
        put_u32(&mut output, 8, FORMAT_VERSION);
        put_u32(&mut output, 12, layout.segment_count);
        put_u32(&mut output, 16, layout.slots_per_segment);
        put_u32(
            &mut output,
            20,
            u32::try_from(layout.slot_size).map_err(|_| invalid_layout("allocation slot size does not fit u32"))?,
        );
        put_u64(&mut output, 24, self.generation);
        put_u64(&mut output, 32, self.next_sequence);
        for (index, current) in self.current.iter().enumerate() {
            put_u32(
                &mut output,
                40 + index * size_of::<u32>(),
                current.unwrap_or(NO_SEGMENT),
            );
        }
        put_u32(&mut output, 52, self.reserve);
        put_u64(
            &mut output,
            56,
            u64::try_from(layout.segment_size).map_err(|_| invalid_layout("segment size does not fit u64"))?,
        );
        for (index, state) in self.segments.iter().enumerate() {
            let offset = STATE_HEADER_SIZE + index * STATE_ENTRY_SIZE;
            put_u32(&mut output, offset, state.generation);
            put_u32(&mut output, offset + 4, state.used);
            put_u64(&mut output, offset + 8, state.sequence);
            output[offset + 16] = state.priority.to_byte();
            output[offset + 17] = state.role as u8;
        }
        let checksum_offset = output.len() - STATE_CHECKSUM_SIZE;
        let checksum = checksum(&output[..checksum_offset]);
        put_u32(&mut output, checksum_offset, checksum);
        Ok(output)
    }

    pub fn decode(input: &[u8], layout: SegmentLayout, active_page: u8) -> Option<Self> {
        if input.len() != layout.state_copy_size || input[..8] != STATE_MAGIC {
            return None;
        }
        if get_u32(input, 8) != FORMAT_VERSION
            || get_u32(input, 12) != layout.segment_count
            || get_u32(input, 16) != layout.slots_per_segment
            || get_u32(input, 20) as usize != layout.slot_size
            || get_u64(input, 56) != layout.segment_size as u64
        {
            return None;
        }
        let checksum_offset = input.len() - STATE_CHECKSUM_SIZE;
        if checksum(&input[..checksum_offset]) != get_u32(input, checksum_offset) {
            return None;
        }
        let mut segments = Vec::with_capacity(layout.segment_count as usize);
        for index in 0..layout.segment_count as usize {
            let offset = STATE_HEADER_SIZE + index * STATE_ENTRY_SIZE;
            segments.push(SegmentState {
                generation: get_u32(input, offset),
                used: get_u32(input, offset + 4),
                sequence: get_u64(input, offset + 8),
                priority: CachePriority::from_byte(input[offset + 16])?,
                role: SegmentRole::from_byte(input[offset + 17])?,
            });
        }
        let state = Self {
            generation: get_u64(input, 24),
            next_sequence: get_u64(input, 32),
            current: std::array::from_fn(|index| {
                let segment = get_u32(input, 40 + index * size_of::<u32>());
                (segment != NO_SEGMENT).then_some(segment)
            }),
            reserve: get_u32(input, 52),
            segments,
            active_page,
        };
        state.validate(layout).ok().map(|()| state)
    }

    fn validate(&self, layout: SegmentLayout) -> Result<()> {
        if self.generation == 0 || self.next_sequence == 0 {
            return Err(invalid_layout("segment allocator generations must be positive"));
        }
        if self.segments.len() != layout.segment_count as usize || self.reserve >= layout.segment_count {
            return Err(invalid_layout("segment allocator reserve is invalid"));
        }
        let mut reserves = 0;
        let mut reclaim_sources = 0;
        let mut reclaim_targets = 0;
        for (segment, state) in self.segments.iter().enumerate() {
            if state.generation == 0 || state.used > layout.slots_per_segment {
                return Err(invalid_layout("segment allocator entry is invalid"));
            }
            reserves += usize::from(state.role == SegmentRole::Reserve);
            reclaim_sources += usize::from(state.role == SegmentRole::ReclaimSource);
            reclaim_targets += usize::from(state.role == SegmentRole::ReclaimTarget);
            if state.role == SegmentRole::Current
                && self.current[state.priority.to_byte() as usize] != Some(segment as u32)
            {
                return Err(invalid_layout("segment current pointer is inconsistent"));
            }
        }
        let normal = reserves == 1 && reclaim_sources == 0 && reclaim_targets == 0;
        let reclaiming = reserves == 0
            && reclaim_sources == 1
            && reclaim_targets == 1
            && self.segments[self.reserve as usize].role == SegmentRole::ReclaimTarget;
        if !normal && !reclaiming {
            return Err(invalid_layout(
                "segment allocator reserve or reclaim transaction is invalid",
            ));
        }
        if normal && self.segments[self.reserve as usize].role != SegmentRole::Reserve {
            return Err(invalid_layout("segment allocator reserve pointer is invalid"));
        }
        for (priority, current) in self.current.iter().enumerate() {
            if let Some(segment) = current {
                let Some(state) = self.segments.get(*segment as usize) else {
                    return Err(invalid_layout("segment current pointer is out of range"));
                };
                if state.role != SegmentRole::Current || state.priority.to_byte() as usize != priority {
                    return Err(invalid_layout("segment current pointer has the wrong class"));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentLocation {
    pub physical_slot: u64,
    pub segment_generation: u32,
    pub stored_len: u32,
    pub checksum: u32,
    pub priority: CachePriority,
}

impl SegmentLocation {
    pub fn encode(self) -> [u8; LOCATION_RECORD_SIZE] {
        let mut output = [0; LOCATION_RECORD_SIZE];
        output[..4].copy_from_slice(&LOCATION_MAGIC);
        output[4] = FORMAT_VERSION as u8;
        output[5] = self.priority.to_byte();
        put_u64(&mut output, 8, self.physical_slot);
        put_u32(&mut output, 16, self.segment_generation);
        put_u32(&mut output, 20, self.stored_len);
        put_u32(&mut output, 24, self.checksum);
        let checksum = checksum(&output[..28]);
        put_u32(&mut output, 28, checksum);
        output
    }

    pub fn decode(input: &[u8]) -> Option<Self> {
        if input.len() != LOCATION_RECORD_SIZE
            || input[..4] != LOCATION_MAGIC
            || input[4] != FORMAT_VERSION as u8
            || checksum(&input[..28]) != get_u32(input, 28)
        {
            return None;
        }
        Some(Self {
            physical_slot: get_u64(input, 8),
            segment_generation: get_u32(input, 16),
            stored_len: get_u32(input, 20),
            checksum: get_u32(input, 24),
            priority: CachePriority::from_byte(input[5])?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerRecord {
    pub key_digest: KeyDigest,
    pub segment_generation: u32,
    pub stored_len: u32,
    pub value_len: u32,
    pub checksum: u32,
    pub priority: CachePriority,
    pub sequence: u64,
}

impl OwnerRecord {
    pub fn encode(self) -> [u8; OWNER_RECORD_SIZE] {
        let mut output = [0; OWNER_RECORD_SIZE];
        output[..4].copy_from_slice(&OWNER_MAGIC);
        output[4] = FORMAT_VERSION as u8;
        output[5] = self.priority.to_byte();
        output[8..32].copy_from_slice(self.key_digest.as_bytes());
        put_u32(&mut output, 32, self.segment_generation);
        put_u32(&mut output, 36, self.stored_len);
        put_u32(&mut output, 40, self.checksum);
        put_u64(&mut output, 44, self.sequence);
        put_u32(&mut output, 52, self.value_len);
        let checksum = checksum(&output[..56]);
        put_u32(&mut output, 56, checksum);
        output
    }

    pub fn decode(input: &[u8]) -> Option<Self> {
        if input.len() != OWNER_RECORD_SIZE
            || input[..4] != OWNER_MAGIC
            || input[4] != FORMAT_VERSION as u8
            || checksum(&input[..56]) != get_u32(input, 56)
        {
            return None;
        }
        let mut key_digest = [0; 24];
        key_digest.copy_from_slice(&input[8..32]);
        Some(Self {
            key_digest: KeyDigest::new(key_digest),
            segment_generation: get_u32(input, 32),
            stored_len: get_u32(input, 36),
            value_len: get_u32(input, 52),
            checksum: get_u32(input, 40),
            priority: CachePriority::from_byte(input[5])?,
            sequence: get_u64(input, 44),
        })
    }
}

fn checksum(input: &[u8]) -> u32 {
    let mut digest = Digest::new(CrcAlgorithm::Crc32Iscsi);
    digest.update(input);
    u32::try_from(digest.finalize()).expect("CRC-32 digest must fit u32")
}

fn put_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + size_of::<u32>()].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + size_of::<u64>()].copy_from_slice(&value.to_le_bytes());
}

fn get_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(input[offset..offset + size_of::<u32>()].try_into().unwrap())
}

fn get_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(input[offset..offset + size_of::<u64>()].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{model::BlobKey, segment::config::SegmentEngineOptions};

    fn layout() -> SegmentLayout {
        let options = SegmentEngineOptions::default().with_segment_size(PAGE_SIZE * 64);
        SegmentLayout::create(
            SegmentEngineConfig::new(16 * 1024 * 1024)
                .with_slot_size(PAGE_SIZE)
                .with_options(options),
        )
        .unwrap()
    }

    #[test]
    fn layout_reserves_one_segment_and_fits_the_capacity() {
        let layout = layout();
        assert!(layout.segment_count >= 5);
        assert_eq!(layout.slots_per_segment, 64);
        assert_eq!(layout.usable_entries, u64::from(layout.segment_count - 1) * 64);
        assert!(layout.total_file_size <= 16 * 1024 * 1024);
        assert_eq!(
            layout.index_capacity_bytes,
            index_capacity_for_entries(layout.usable_entries).unwrap()
        );
        assert_eq!(layout.segment_for_slot(65), Some((1, 1)));
        assert_eq!(layout.physical_slot(1, 1), Some(65));
    }

    #[test]
    fn fixed_index_capacity_is_page_aligned_and_checks_rounding_overflow() {
        let capacity = index_capacity_for_entries(1).unwrap();
        assert_eq!(capacity % PAGE_SIZE as u64, 0);
        assert!(index_capacity_for_entries(u64::MAX).is_err());
    }

    #[test]
    fn allocator_state_roundtrips_and_rejects_corruption() {
        let layout = layout();
        let state = AllocatorState::empty(layout);
        let mut encoded = state.encode(layout).unwrap();
        assert_eq!(AllocatorState::decode(&encoded, layout, 0), Some(state));

        assert_eq!(
            SegmentLayout::discover(
                &encoded,
                layout.data_file_size,
                layout.owner_file_size,
                (layout.state_copy_size * 2) as u64,
            ),
            Some(layout)
        );

        encoded[STATE_HEADER_SIZE + 3] ^= 0xff;
        assert!(AllocatorState::decode(&encoded, layout, 0).is_none());
    }

    #[test]
    fn owner_and_location_records_roundtrip() {
        let mut key_bytes = [7; 24];
        key_bytes[16..].copy_from_slice(&42_u64.to_le_bytes());
        let key = BlobKey::new(key_bytes).unwrap();
        let location = SegmentLocation {
            physical_slot: 99,
            segment_generation: 3,
            stored_len: 4_096,
            checksum: 123,
            priority: CachePriority::High,
        };
        assert_eq!(SegmentLocation::decode(&location.encode()), Some(location));

        let owner = OwnerRecord {
            key_digest: KeyDigest::for_key(&key),
            segment_generation: 3,
            stored_len: 4_096,
            value_len: 4_000,
            checksum: 123,
            priority: CachePriority::High,
            sequence: 88,
        };
        assert_eq!(OwnerRecord::decode(&owner.encode()), Some(owner));
    }
}
