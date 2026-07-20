use crc_fast::{CrcAlgorithm, Digest};

use crate::{
    error::{Error, Result},
    format::PAGE_SIZE,
    model::{CachePriority, KeyDigest},
    store::config::ExtentStoreConfig,
};

pub const SLOT_OWNER_SIZE: usize = 64;
pub const LOCATION_RECORD_SIZE: usize = 32;
const STATE_ENTRY_SIZE: usize = 24;
const STATE_HEADER_SIZE: usize = 64;
const STATE_CHECKSUM_SIZE: usize = size_of::<u32>();
const STATE_MAGIC: [u8; 8] = *b"SCSEGST1";
const SLOT_OWNER_MAGIC: [u8; 4] = *b"SCOW";
const LOCATION_MAGIC: [u8; 4] = *b"SCLO";
/// Compatibility identity for every persisted Extent layout and encoding choice.
///
/// Bump this when changing the balanced slot or extent size, layout derivation, record encoding,
/// or an incompatible format in the embedded fixed-record index.
pub const EXTENT_FORMAT_VERSION: u32 = 3;
const NO_EXTENT: u32 = u32::MAX;
const FIXED_LSM_INDEX_BYTES_PER_ENTRY: u64 = 76;
// One steady-state copy, one atomic compaction output, and one bounded WAL/L0 write tail.
const FIXED_LSM_INDEX_CAPACITY_COPIES: u64 = 3;
const FIXED_LSM_INDEX_OVERHEAD_PER_ENTRY: u64 = 16;
// Small indexes still create page-aligned WAL, manifest, and SST metadata during compaction.
const FIXED_LSM_INDEX_MINIMUM_OVERHEAD: u64 = 128 * 1024;
const FIXED_LSM_INDEX_MAXIMUM_OVERHEAD: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreLayout {
    pub slot_size: usize,
    pub extent_size: usize,
    pub slots_per_extent: u32,
    pub extent_count: u32,
    pub max_entries: u64,
    pub index_capacity_bytes: u64,
    pub data_file_size: u64,
    pub slot_owner_file_size: u64,
    pub state_copy_size: usize,
    pub total_file_size: u64,
}

impl StoreLayout {
    pub fn create(config: ExtentStoreConfig) -> Result<Self> {
        let slot_size = config.slot_size;
        let extent_size = config.options.extent_size;
        if slot_size < PAGE_SIZE || !slot_size.is_multiple_of(PAGE_SIZE) {
            return Err(Error::InvalidConfig(format!(
                "extent slot_size must be a positive multiple of {PAGE_SIZE} bytes"
            )));
        }
        if extent_size < slot_size || !extent_size.is_multiple_of(slot_size) {
            return Err(Error::InvalidConfig(format!(
                "extent_size must be a positive multiple of slot_size ({slot_size})"
            )));
        }
        let slots_per_extent = extent_size / slot_size;
        let slots_per_extent = u32::try_from(slots_per_extent).map_err(|_| {
            Error::InvalidConfig("extent contains more physical slots than u32 can represent".to_string())
        })?;
        let maximum_extents = config.capacity_bytes / extent_size as u64;
        let maximum_extents = u32::try_from(maximum_extents).unwrap_or(u32::MAX);

        for extent_count in (5..=maximum_extents).rev() {
            let physical_slots = u64::from(extent_count) * u64::from(slots_per_extent);
            let max_entries = physical_slots - u64::from(slots_per_extent);
            let index_capacity_bytes = index_capacity_for_entries(max_entries)?;
            let data_file_size = physical_slots
                .checked_mul(slot_size as u64)
                .ok_or_else(|| invalid_layout("extent data file size overflows u64"))?;
            let slot_owner_file_size = physical_slots
                .checked_mul(SLOT_OWNER_SIZE as u64)
                .ok_or_else(|| invalid_layout("extent owner file size overflows u64"))?;
            let state_copy_size = state_copy_size(extent_count)?;
            let total_file_size = index_capacity_bytes
                .checked_add(data_file_size)
                .and_then(|size| size.checked_add(slot_owner_file_size))
                .and_then(|size| size.checked_add((state_copy_size * 2) as u64))
                .ok_or_else(|| invalid_layout("extent store file size overflows u64"))?;
            if total_file_size <= config.capacity_bytes {
                return Ok(Self {
                    slot_size,
                    extent_size,
                    slots_per_extent,
                    extent_count,
                    max_entries,
                    index_capacity_bytes,
                    data_file_size,
                    slot_owner_file_size,
                    state_copy_size,
                    total_file_size,
                });
            }
        }

        Err(Error::InvalidConfig(format!(
            "extent store capacity must fit at least five {extent_size}-byte extents and their persistent index"
        )))
    }

    pub fn slot_index(self, extent: u32, extent_slot: u32) -> Option<u64> {
        (extent < self.extent_count && extent_slot < self.slots_per_extent)
            .then(|| u64::from(extent) * u64::from(self.slots_per_extent) + u64::from(extent_slot))
    }

    pub fn locate_slot(self, slot_index: u64) -> Option<(u32, u32)> {
        let extent = slot_index / u64::from(self.slots_per_extent);
        let extent_slot = slot_index % u64::from(self.slots_per_extent);
        (extent < u64::from(self.extent_count)).then_some((extent as u32, extent_slot as u32))
    }

    pub fn discover(
        state_copy: &[u8],
        data_file_size: u64,
        slot_owner_file_size: u64,
        state_file_size: u64,
    ) -> Option<Self> {
        if state_copy.len() < STATE_HEADER_SIZE
            || state_copy[..8] != STATE_MAGIC
            || get_u32(state_copy, 8) != EXTENT_FORMAT_VERSION
        {
            return None;
        }
        let extent_count = get_u32(state_copy, 12);
        let slots_per_extent = get_u32(state_copy, 16);
        let slot_size = usize::try_from(get_u32(state_copy, 20)).ok()?;
        let extent_size = usize::try_from(get_u64(state_copy, 56)).ok()?;
        if extent_count < 5
            || slots_per_extent == 0
            || slot_size < PAGE_SIZE
            || !slot_size.is_multiple_of(PAGE_SIZE)
            || extent_size != slot_size.checked_mul(slots_per_extent as usize)?
        {
            return None;
        }

        let physical_slots = u64::from(extent_count) * u64::from(slots_per_extent);
        let max_entries = physical_slots.checked_sub(u64::from(slots_per_extent))?;
        let expected_index = index_capacity_for_entries(max_entries).ok()?;
        let expected_data = physical_slots.checked_mul(slot_size as u64)?;
        let expected_owners = physical_slots.checked_mul(SLOT_OWNER_SIZE as u64)?;
        let state_copy_size = state_copy_size(extent_count).ok()?;
        let expected_state = u64::try_from(state_copy_size.checked_mul(2)?).ok()?;
        if data_file_size != expected_data
            || slot_owner_file_size != expected_owners
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
            extent_size,
            slots_per_extent,
            extent_count,
            max_entries,
            index_capacity_bytes: expected_index,
            data_file_size: expected_data,
            slot_owner_file_size: expected_owners,
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

fn state_copy_size(extent_count: u32) -> Result<usize> {
    let bytes = (extent_count as usize)
        .checked_mul(STATE_ENTRY_SIZE)
        .and_then(|bytes| bytes.checked_add(STATE_HEADER_SIZE + STATE_CHECKSUM_SIZE))
        .ok_or_else(|| invalid_layout("extent state table size overflows usize"))?;
    Ok(bytes.next_multiple_of(PAGE_SIZE))
}

fn invalid_layout(message: &str) -> Error {
    Error::InvalidConfig(message.to_string())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ExtentRole {
    Free = 0,
    Reserve = 1,
    Current = 2,
    Sealed = 3,
    ReclaimSource = 4,
    ReclaimTarget = 5,
}

impl ExtentRole {
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
pub struct ExtentState {
    pub generation: u32,
    pub used: u32,
    pub sequence: u64,
    pub priority: CachePriority,
    pub role: ExtentRole,
}

impl ExtentState {
    const fn free() -> Self {
        Self {
            generation: 1,
            used: 0,
            sequence: 0,
            priority: CachePriority::Low,
            role: ExtentRole::Free,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtentPoolState {
    pub state_generation: u64,
    pub next_sequence: u64,
    pub current: [Option<u32>; 3],
    pub reserve: u32,
    pub extents: Vec<ExtentState>,
    pub active_page: u8,
}

impl ExtentPoolState {
    pub fn empty(layout: StoreLayout) -> Self {
        let mut extents = vec![ExtentState::free(); layout.extent_count as usize];
        extents[0].role = ExtentRole::Reserve;
        Self {
            state_generation: 1,
            next_sequence: 1,
            current: [None; 3],
            reserve: 0,
            extents,
            active_page: 0,
        }
    }

    pub fn encode(&self, layout: StoreLayout) -> Result<Vec<u8>> {
        self.validate(layout)?;
        let mut output = vec![0; layout.state_copy_size];
        output[..8].copy_from_slice(&STATE_MAGIC);
        put_u32(&mut output, 8, EXTENT_FORMAT_VERSION);
        put_u32(&mut output, 12, layout.extent_count);
        put_u32(&mut output, 16, layout.slots_per_extent);
        put_u32(
            &mut output,
            20,
            u32::try_from(layout.slot_size).map_err(|_| invalid_layout("allocation slot size does not fit u32"))?,
        );
        put_u64(&mut output, 24, self.state_generation);
        put_u64(&mut output, 32, self.next_sequence);
        for (index, current) in self.current.iter().enumerate() {
            put_u32(&mut output, 40 + index * size_of::<u32>(), current.unwrap_or(NO_EXTENT));
        }
        put_u32(&mut output, 52, self.reserve);
        put_u64(
            &mut output,
            56,
            u64::try_from(layout.extent_size).map_err(|_| invalid_layout("extent size does not fit u64"))?,
        );
        for (index, state) in self.extents.iter().enumerate() {
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

    pub fn decode(input: &[u8], layout: StoreLayout, active_page: u8) -> Option<Self> {
        if input.len() != layout.state_copy_size || input[..8] != STATE_MAGIC {
            return None;
        }
        if get_u32(input, 8) != EXTENT_FORMAT_VERSION
            || get_u32(input, 12) != layout.extent_count
            || get_u32(input, 16) != layout.slots_per_extent
            || get_u32(input, 20) as usize != layout.slot_size
            || get_u64(input, 56) != layout.extent_size as u64
        {
            return None;
        }
        let checksum_offset = input.len() - STATE_CHECKSUM_SIZE;
        if checksum(&input[..checksum_offset]) != get_u32(input, checksum_offset) {
            return None;
        }
        let mut extents = Vec::with_capacity(layout.extent_count as usize);
        for index in 0..layout.extent_count as usize {
            let offset = STATE_HEADER_SIZE + index * STATE_ENTRY_SIZE;
            extents.push(ExtentState {
                generation: get_u32(input, offset),
                used: get_u32(input, offset + 4),
                sequence: get_u64(input, offset + 8),
                priority: CachePriority::from_byte(input[offset + 16])?,
                role: ExtentRole::from_byte(input[offset + 17])?,
            });
        }
        let state = Self {
            state_generation: get_u64(input, 24),
            next_sequence: get_u64(input, 32),
            current: std::array::from_fn(|index| {
                let extent = get_u32(input, 40 + index * size_of::<u32>());
                (extent != NO_EXTENT).then_some(extent)
            }),
            reserve: get_u32(input, 52),
            extents,
            active_page,
        };
        state.validate(layout).ok().map(|()| state)
    }

    fn validate(&self, layout: StoreLayout) -> Result<()> {
        if self.state_generation == 0 || self.next_sequence == 0 {
            return Err(invalid_layout("extent allocator generations must be positive"));
        }
        if self.extents.len() != layout.extent_count as usize || self.reserve >= layout.extent_count {
            return Err(invalid_layout("extent allocator reserve is invalid"));
        }
        let mut reserves = 0;
        let mut reclaim_sources = 0;
        let mut reclaim_targets = 0;
        for (extent, state) in self.extents.iter().enumerate() {
            if state.generation == 0 || state.used > layout.slots_per_extent {
                return Err(invalid_layout("extent allocator entry is invalid"));
            }
            reserves += usize::from(state.role == ExtentRole::Reserve);
            reclaim_sources += usize::from(state.role == ExtentRole::ReclaimSource);
            reclaim_targets += usize::from(state.role == ExtentRole::ReclaimTarget);
            if state.role == ExtentRole::Current
                && self.current[state.priority.to_byte() as usize] != Some(extent as u32)
            {
                return Err(invalid_layout("extent current pointer is inconsistent"));
            }
        }
        let normal = reserves == 1 && reclaim_sources == 0 && reclaim_targets == 0;
        let reclaiming = reserves == 0
            && reclaim_sources == 1
            && reclaim_targets == 1
            && self.extents[self.reserve as usize].role == ExtentRole::ReclaimTarget;
        if !normal && !reclaiming {
            return Err(invalid_layout(
                "extent allocator reserve or reclaim transaction is invalid",
            ));
        }
        if normal && self.extents[self.reserve as usize].role != ExtentRole::Reserve {
            return Err(invalid_layout("extent allocator reserve pointer is invalid"));
        }
        for (priority, current) in self.current.iter().enumerate() {
            if let Some(extent) = current {
                let Some(state) = self.extents.get(*extent as usize) else {
                    return Err(invalid_layout("extent current pointer is out of range"));
                };
                if state.role != ExtentRole::Current || state.priority.to_byte() as usize != priority {
                    return Err(invalid_layout("extent current pointer has the wrong class"));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryLocation {
    pub first_slot: u64,
    pub extent_generation: u32,
    pub stored_len: u32,
    pub checksum: u32,
    pub priority: CachePriority,
}

impl EntryLocation {
    pub fn encode(self) -> [u8; LOCATION_RECORD_SIZE] {
        let mut output = [0; LOCATION_RECORD_SIZE];
        output[..4].copy_from_slice(&LOCATION_MAGIC);
        output[4] = EXTENT_FORMAT_VERSION as u8;
        output[5] = self.priority.to_byte();
        put_u64(&mut output, 8, self.first_slot);
        put_u32(&mut output, 16, self.extent_generation);
        put_u32(&mut output, 20, self.stored_len);
        put_u32(&mut output, 24, self.checksum);
        let checksum = checksum(&output[..28]);
        put_u32(&mut output, 28, checksum);
        output
    }

    pub fn decode(input: &[u8]) -> Option<Self> {
        if input.len() != LOCATION_RECORD_SIZE
            || input[..4] != LOCATION_MAGIC
            || input[4] != EXTENT_FORMAT_VERSION as u8
            || checksum(&input[..28]) != get_u32(input, 28)
        {
            return None;
        }
        Some(Self {
            first_slot: get_u64(input, 8),
            extent_generation: get_u32(input, 16),
            stored_len: get_u32(input, 20),
            checksum: get_u32(input, 24),
            priority: CachePriority::from_byte(input[5])?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotOwner {
    pub key_digest: KeyDigest,
    pub extent_generation: u32,
    pub stored_len: u32,
    pub value_len: u32,
    pub checksum: u32,
    pub priority: CachePriority,
    pub sequence: u64,
}

impl SlotOwner {
    pub fn encode(self) -> [u8; SLOT_OWNER_SIZE] {
        let mut output = [0; SLOT_OWNER_SIZE];
        output[..4].copy_from_slice(&SLOT_OWNER_MAGIC);
        output[4] = EXTENT_FORMAT_VERSION as u8;
        output[5] = self.priority.to_byte();
        output[8..32].copy_from_slice(self.key_digest.as_bytes());
        put_u32(&mut output, 32, self.extent_generation);
        put_u32(&mut output, 36, self.stored_len);
        put_u32(&mut output, 40, self.checksum);
        put_u64(&mut output, 44, self.sequence);
        put_u32(&mut output, 52, self.value_len);
        let checksum = checksum(&output[..56]);
        put_u32(&mut output, 56, checksum);
        output
    }

    pub fn decode(input: &[u8]) -> Option<Self> {
        if input.len() != SLOT_OWNER_SIZE
            || input[..4] != SLOT_OWNER_MAGIC
            || input[4] != EXTENT_FORMAT_VERSION as u8
            || checksum(&input[..56]) != get_u32(input, 56)
        {
            return None;
        }
        let mut key_digest = [0; 24];
        key_digest.copy_from_slice(&input[8..32]);
        Some(Self {
            key_digest: KeyDigest::new(key_digest),
            extent_generation: get_u32(input, 32),
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
    use crate::{model::EntryKey, store::config::ExtentStoreOptions};

    fn layout() -> StoreLayout {
        let options = ExtentStoreOptions::default().with_extent_size(PAGE_SIZE * 64);
        StoreLayout::create(
            ExtentStoreConfig::new(16 * 1024 * 1024)
                .with_slot_size(PAGE_SIZE)
                .with_options(options),
        )
        .unwrap()
    }

    #[test]
    fn layout_reserves_one_extent_and_fits_the_capacity() {
        let layout = layout();
        assert!(layout.extent_count >= 5);
        assert_eq!(layout.slots_per_extent, 64);
        assert_eq!(layout.max_entries, u64::from(layout.extent_count - 1) * 64);
        assert!(layout.total_file_size <= 16 * 1024 * 1024);
        assert_eq!(
            layout.index_capacity_bytes,
            index_capacity_for_entries(layout.max_entries).unwrap()
        );
        assert_eq!(layout.locate_slot(65), Some((1, 1)));
        assert_eq!(layout.slot_index(1, 1), Some(65));
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
        let state = ExtentPoolState::empty(layout);
        let mut encoded = state.encode(layout).unwrap();
        assert_eq!(ExtentPoolState::decode(&encoded, layout, 0), Some(state));

        assert_eq!(
            StoreLayout::discover(
                &encoded,
                layout.data_file_size,
                layout.slot_owner_file_size,
                (layout.state_copy_size * 2) as u64,
            ),
            Some(layout)
        );

        encoded[STATE_HEADER_SIZE + 3] ^= 0xff;
        assert!(ExtentPoolState::decode(&encoded, layout, 0).is_none());
    }

    #[test]
    fn owner_and_location_records_roundtrip() {
        let mut key_bytes = [7; 24];
        key_bytes[16..].copy_from_slice(&42_u64.to_le_bytes());
        let key = EntryKey::new(key_bytes).unwrap();
        let location = EntryLocation {
            first_slot: 99,
            extent_generation: 3,
            stored_len: 4_096,
            checksum: 123,
            priority: CachePriority::High,
        };
        assert_eq!(EntryLocation::decode(&location.encode()), Some(location));

        let owner = SlotOwner {
            key_digest: KeyDigest::for_key(&key),
            extent_generation: 3,
            stored_len: 4_096,
            value_len: 4_000,
            checksum: 123,
            priority: CachePriority::High,
            sequence: 88,
        };
        assert_eq!(SlotOwner::decode(&owner.encode()), Some(owner));
    }
}
