use std::{
    fs,
    fs::File,
    path::Path,
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::{
    error::{Error, Result},
    file::{
        AlignedBuffer, ensure_cache_file_reserved, open_cache_file, read_exact_at, reserve_cache_file, write_all_at,
    },
    format::{copy_stored_entry_range, decode_entry_value, decode_stored_entry, stored_entry_len, value_checksum},
    model::{CachePriority, EntryKey, KeyDigest},
    store::{
        format::{EntryLocation, ExtentPoolState, ExtentRole, SLOT_OWNER_SIZE, SlotOwner, StoreLayout},
        io::{IoSchedulerStats, PayloadIoScheduler},
        stats::{ExtentOccupancy, PhysicalWriteStats},
    },
};

pub(crate) const DATA_FILE: &str = "data";
pub(crate) const SLOT_OWNER_FILE: &str = "owners";
pub(crate) const STATE_FILE: &str = "state";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryAllocation {
    pub first_slot: u64,
    pub extent: u32,
    pub extent_slot: u32,
    pub slot_count: u32,
    pub extent_generation: u32,
    pub priority: CachePriority,
    pub sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocationResult {
    Allocated(EntryAllocation),
    ReclaimRequired,
}

#[derive(Debug, Clone, Copy)]
pub struct EntryWrite<'a> {
    pub allocation: EntryAllocation,
    pub key: &'a EntryKey,
    pub key_digest: KeyDigest,
    pub value: &'a [u8],
    pub checksum: u32,
}

impl EntryWrite<'_> {
    fn stored_len(self) -> usize {
        stored_entry_len(self.key, self.value).expect("validated stored entry length must fit usize")
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EntryWriteResult {
    pub locations: Vec<EntryLocation>,
    pub data_runs: usize,
    pub slot_owner_runs: usize,
    pub data_bytes: usize,
    pub slot_owner_bytes: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct StoredEntryRead {
    pub value: Option<Vec<u8>>,
    pub data_slots: usize,
    pub data_runs: usize,
    pub data_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtentVictim {
    pub extent: u32,
    pub generation: u32,
    pub used: u32,
    pub priority: CachePriority,
    pub sequence: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ReclaimCandidates {
    occupied_extents: [u32; 3],
    sealed: [Option<ExtentVictim>; 3],
    current: [Option<ExtentVictim>; 3],
}

impl ReclaimCandidates {
    pub const fn occupied_extents(self, priority: CachePriority) -> u32 {
        self.occupied_extents[priority as usize]
    }

    pub fn oldest(self, priority: CachePriority) -> Option<(ExtentVictim, bool)> {
        let priority = priority as usize;
        match (self.sealed[priority], self.current[priority]) {
            (Some(sealed), Some(current)) if (current.sequence, current.extent) < (sealed.sequence, sealed.extent) => {
                Some((current, true))
            }
            (Some(sealed), _) => Some((sealed, false)),
            (None, Some(current)) => Some((current, true)),
            (None, None) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimTransaction {
    pub source: ExtentVictim,
    pub target: u32,
    pub target_generation: u32,
    pub priority: CachePriority,
}

/// An immutable allocator image captured at one store publication boundary.
///
/// Allocations may continue after capture. Only the pool-state generation/page cursor is installed back into
/// the live allocator after this exact image reaches durable storage; reclaim persistence is
/// serialized separately and therefore cannot race this cursor transition.
#[derive(Debug)]
pub struct ExtentPoolCheckpoint {
    base_state_generation: u64,
    base_page: u8,
    next_state_generation: u64,
    next_page: u8,
    encoded: Vec<u8>,
}

#[derive(Debug)]
pub struct ExtentPool {
    data: File,
    owners: File,
    state_file: File,
    io: PayloadIoScheduler,
    layout: StoreLayout,
    direct_io: bool,
    write_concurrency: usize,
    read_run_slots: usize,
    write_run_slots: usize,
    allocated_size: u64,
    state: Mutex<ExtentPoolState>,
    writes: PoolWriteCounters,
}

#[derive(Debug, Default)]
struct PoolWriteCounters {
    data_runs: AtomicU64,
    data_bytes: AtomicU64,
    slot_owner_runs: AtomicU64,
    slot_owner_bytes: AtomicU64,
    allocator_runs: AtomicU64,
    allocator_bytes: AtomicU64,
}

impl PoolWriteCounters {
    fn snapshot(&self) -> PhysicalWriteStats {
        PhysicalWriteStats {
            data_runs: self.data_runs.load(Ordering::Relaxed),
            data_bytes: self.data_bytes.load(Ordering::Relaxed),
            slot_owner_runs: self.slot_owner_runs.load(Ordering::Relaxed),
            slot_owner_bytes: self.slot_owner_bytes.load(Ordering::Relaxed),
            allocator_runs: self.allocator_runs.load(Ordering::Relaxed),
            allocator_bytes: self.allocator_bytes.load(Ordering::Relaxed),
            ..Default::default()
        }
    }
}

impl ExtentPool {
    pub fn create(
        root: &Path,
        layout: StoreLayout,
        direct_io: bool,
        write_concurrency: usize,
        io_read_priority_duration: Duration,
        read_run_size: usize,
        write_run_size: usize,
    ) -> Result<Self> {
        fs::create_dir_all(root).map_err(|error| Error::io("create extent cache directory", error))?;
        let data = open_cache_file(&root.join(DATA_FILE), true, direct_io)
            .map_err(|error| Error::io("create extent data file", error))?;
        reserve_cache_file(&data, layout.data_file_size)
            .map_err(|error| Error::io("reserve extent data file", error))?;
        let owners = open_cache_file(&root.join(SLOT_OWNER_FILE), true, false)
            .map_err(|error| Error::io("create extent owner file", error))?;
        reserve_cache_file(&owners, layout.slot_owner_file_size)
            .map_err(|error| Error::io("reserve extent owner file", error))?;
        let state_file = open_cache_file(&root.join(STATE_FILE), true, false)
            .map_err(|error| Error::io("create extent state file", error))?;
        let state_file_size = state_file_size(layout)?;
        reserve_cache_file(&state_file, state_file_size)
            .map_err(|error| Error::io("reserve extent state file", error))?;

        let state = ExtentPoolState::empty(layout);
        write_all_at(&state_file, &state.encode(layout)?, 0)
            .map_err(|error| Error::io("write initial extent state", error))?;
        state_file
            .sync_data()
            .map_err(|error| Error::io("sync initial extent state", error))?;
        let allocated_size = layout_allocated_size(layout)?;
        let io = PayloadIoScheduler::new(write_concurrency, io_read_priority_duration)
            .map_err(|error| Error::io("configure extent I/O scheduler", error))?;
        Ok(Self {
            data,
            owners,
            state_file,
            io,
            layout,
            direct_io,
            write_concurrency,
            read_run_slots: read_run_size.div_ceil(layout.slot_size),
            write_run_slots: write_run_size.div_ceil(layout.slot_size),
            allocated_size,
            state: Mutex::new(state),
            writes: PoolWriteCounters::default(),
        })
    }

    pub fn open(
        root: &Path,
        direct_io: bool,
        write_concurrency: usize,
        io_read_priority_duration: Duration,
        read_run_size: usize,
        write_run_size: usize,
    ) -> Result<Self> {
        let data = open_cache_file(&root.join(DATA_FILE), false, direct_io)
            .map_err(|error| Error::io("open extent data file", error))?;
        let owners = open_cache_file(&root.join(SLOT_OWNER_FILE), false, false)
            .map_err(|error| Error::io("open extent owner file", error))?;
        let state_file = open_cache_file(&root.join(STATE_FILE), false, false)
            .map_err(|error| Error::io("open extent state file", error))?;
        let data_size = data
            .metadata()
            .map_err(|error| Error::io("read extent data file size", error))?
            .len();
        let owner_size = owners
            .metadata()
            .map_err(|error| Error::io("read extent owner file size", error))?
            .len();
        let state_size = state_file
            .metadata()
            .map_err(|error| Error::io("read extent state file size", error))?
            .len();
        if state_size == 0 || !state_size.is_multiple_of(2) {
            return Err(invalid_state("state file does not contain two equal copies"));
        }
        let copy_size =
            usize::try_from(state_size / 2).map_err(|_| invalid_state("state copy does not fit address space"))?;
        let mut copies = [vec![0; copy_size], vec![0; copy_size]];
        for (page, output) in copies.iter_mut().enumerate() {
            let offset = u64::try_from(page)
                .ok()
                .and_then(|page| page.checked_mul(state_size / 2))
                .ok_or_else(|| invalid_state("state copy offset overflows u64"))?;
            read_exact_at(&state_file, output, offset).map_err(|error| Error::io("read extent state copy", error))?;
        }

        let mut candidates = Vec::new();
        for (page, input) in copies.iter().enumerate() {
            let Some(layout) = StoreLayout::discover(input, data_size, owner_size, state_size) else {
                continue;
            };
            let Some(state) = ExtentPoolState::decode(input, layout, page as u8) else {
                continue;
            };
            candidates.push((layout, state));
        }
        if candidates.len() == 2 && candidates[0].0 != candidates[1].0 {
            return Err(invalid_state("valid state copies disagree on the layout"));
        }
        let Some((layout, state)) = candidates
            .into_iter()
            .max_by_key(|(_, state)| (state.state_generation, state.active_page))
        else {
            return Err(invalid_state("both allocator state copies are invalid"));
        };
        ensure_cache_file_reserved(&data, layout.data_file_size)
            .map_err(|error| Error::io("verify extent data reservation", error))?;
        ensure_cache_file_reserved(&owners, layout.slot_owner_file_size)
            .map_err(|error| Error::io("verify extent owner reservation", error))?;
        ensure_cache_file_reserved(&state_file, state_file_size(layout)?)
            .map_err(|error| Error::io("verify extent state reservation", error))?;
        let allocated_size = layout_allocated_size(layout)?;
        let io = PayloadIoScheduler::new(write_concurrency, io_read_priority_duration)
            .map_err(|error| Error::io("configure extent I/O scheduler", error))?;

        let pool = Self {
            data,
            owners,
            state_file,
            io,
            layout,
            direct_io,
            write_concurrency,
            read_run_slots: read_run_size.div_ceil(layout.slot_size),
            write_run_slots: write_run_size.div_ceil(layout.slot_size),
            allocated_size,
            state: Mutex::new(state),
            writes: PoolWriteCounters::default(),
        };
        pool.recover_current_tails()?;
        Ok(pool)
    }

    pub const fn layout(&self) -> StoreLayout {
        self.layout
    }

    pub fn allocated_size(&self) -> Result<u64> {
        Ok(self.allocated_size)
    }

    pub fn physical_write_stats(&self) -> PhysicalWriteStats {
        self.writes.snapshot()
    }

    pub fn io_scheduler_stats(&self) -> IoSchedulerStats {
        self.io.stats()
    }

    pub fn allocate(&self, priority: CachePriority, slots: u32) -> Result<AllocationResult> {
        if slots == 0 || slots > self.layout.slots_per_extent {
            return Err(invalid_state("extent allocation must fit completely within one extent"));
        }
        let priority_index = usize::from(priority.to_byte());
        let mut state = mutex_lock(&self.state);
        loop {
            if let Some(extent) = state.current[priority_index] {
                let entry_index = extent as usize;
                let remaining = self
                    .layout
                    .slots_per_extent
                    .saturating_sub(state.extents[entry_index].used);
                if remaining >= slots {
                    let extent_slot = state.extents[entry_index].used;
                    let extent_generation = state.extents[entry_index].generation;
                    state.extents[entry_index].used += slots;
                    let sequence = state.next_sequence;
                    state.next_sequence = state
                        .next_sequence
                        .checked_add(1)
                        .ok_or_else(|| invalid_state("extent allocation sequence is exhausted"))?;
                    let first_slot = self
                        .layout
                        .slot_index(extent, extent_slot)
                        .expect("current extent slot must be in the layout");
                    return Ok(AllocationResult::Allocated(EntryAllocation {
                        first_slot,
                        extent,
                        extent_slot,
                        slot_count: slots,
                        extent_generation,
                        priority,
                        sequence,
                    }));
                }
                state.extents[entry_index].role = ExtentRole::Sealed;
                state.current[priority_index] = None;
                continue;
            }

            let Some(extent) = state.extents.iter().position(|extent| extent.role == ExtentRole::Free) else {
                return Ok(AllocationResult::ReclaimRequired);
            };
            let extent = u32::try_from(extent).map_err(|_| invalid_state("free extent index does not fit u32"))?;
            let sequence = state.next_sequence;
            let extent_state = &mut state.extents[extent as usize];
            extent_state.used = 0;
            extent_state.sequence = sequence;
            extent_state.priority = priority;
            extent_state.role = ExtentRole::Current;
            state.current[priority_index] = Some(extent);

            // Normal allocator transitions are folded into the next coordinated checkpoint.
            // ExtentStore persists this state after payload sync and before index publication.
        }
    }

    pub fn write_batch(&self, writes: &[EntryWrite<'_>]) -> Result<EntryWriteResult> {
        if writes.is_empty() {
            return Ok(EntryWriteResult::default());
        }
        for write in writes {
            if write.value.is_empty() {
                return Err(Error::EmptyValue);
            }
            let stored_len = write.stored_len();
            if stored_len > self.layout.extent_size {
                return Err(Error::StoredEntryTooLarge {
                    len: stored_len,
                    maximum: self.layout.extent_size,
                });
            }
            let slots = stored_len.div_ceil(self.layout.slot_size);
            if usize::try_from(write.allocation.slot_count).ok() != Some(slots) {
                return Err(invalid_state("extent allocation does not match the value length"));
            }
        }

        let mut order = (0..writes.len()).collect::<Vec<_>>();
        order.sort_unstable_by_key(|index| writes[*index].allocation.first_slot);
        for pair in order.windows(2) {
            let previous = writes[pair[0]].allocation;
            let previous_end = previous
                .first_slot
                .checked_add(u64::from(previous.slot_count))
                .ok_or_else(|| invalid_state("extent allocation end overflows u64"))?;
            if previous_end > writes[pair[1]].allocation.first_slot {
                return Err(invalid_state("extent batch allocations overlap"));
            }
        }
        let slot_owner_runs = data_write_runs(&order, writes, usize::MAX);
        let data_runs = data_write_runs(&order, writes, self.write_run_slots);
        self.write_data_runs(writes, &data_runs)?;
        self.write_slot_owner_runs(writes, &slot_owner_runs)?;

        let mut locations = vec![None; writes.len()];
        for (index, write) in writes.iter().enumerate() {
            locations[index] = Some(EntryLocation {
                first_slot: write.allocation.first_slot,
                extent_generation: write.allocation.extent_generation,
                stored_len: u32::try_from(write.stored_len()).expect("validated stored entry length must fit u32"),
                checksum: write.checksum,
                priority: write.allocation.priority,
            });
        }
        let allocated_slots = writes.iter().fold(0usize, |slots, write| {
            slots.saturating_add(write.allocation.slot_count as usize)
        });
        Ok(EntryWriteResult {
            locations: locations
                .into_iter()
                .map(|location| location.expect("every write must have a location"))
                .collect(),
            data_runs: data_runs.len(),
            slot_owner_runs: slot_owner_runs.len(),
            data_bytes: allocated_slots.saturating_mul(self.layout.slot_size),
            slot_owner_bytes: allocated_slots.saturating_mul(SLOT_OWNER_SIZE),
        })
    }

    fn location_slots(&self, location: EntryLocation) -> Option<(u32, u32, u32)> {
        let len = location.stored_len as usize;
        if len == 0 || len > self.layout.extent_size {
            return None;
        }
        let slots = u32::try_from(len.div_ceil(self.layout.slot_size)).ok()?;
        let (extent, extent_slot) = self.layout.locate_slot(location.first_slot)?;
        (extent_slot.checked_add(slots)? <= self.layout.slots_per_extent).then_some((extent, extent_slot, slots))
    }

    pub fn read_entry(&self, key: &EntryKey, location: EntryLocation) -> Result<StoredEntryRead> {
        let mut result = self.read_encoded_entry(location)?;
        result.value = result.value.and_then(|stored| decode_entry_value(stored, key));
        Ok(result)
    }

    pub fn read_stored_entry(&self, location: EntryLocation) -> Result<Option<(EntryKey, Vec<u8>)>> {
        let result = self.read_encoded_entry(location)?;
        Ok(result.value.and_then(decode_stored_entry))
    }

    fn read_encoded_entry(&self, location: EntryLocation) -> Result<StoredEntryRead> {
        self.read_encoded_entry_after_validation(location, || Ok(()))
    }

    fn read_encoded_entry_after_validation<F>(
        &self,
        location: EntryLocation,
        after_validation: F,
    ) -> Result<StoredEntryRead>
    where
        F: FnOnce() -> Result<()>,
    {
        let mut result = StoredEntryRead::default();
        let Some((extent, slot, slots)) = self.location_slots(location) else {
            return Ok(result);
        };
        {
            let state = mutex_lock(&self.state);
            let state = state.extents[extent as usize];
            if state.generation != location.extent_generation || state.used < slot.saturating_add(slots) {
                return Ok(result);
            }
        }
        after_validation()?;

        let mut value = Vec::with_capacity(location.stored_len as usize);
        self.io.read(|| -> Result<()> {
            let mut slot_offset = 0usize;
            while slot_offset < slots as usize {
                let run_slots = (slots as usize - slot_offset).min(self.read_run_slots);
                let remaining = location.stored_len as usize - value.len();
                let logical_len = remaining.min(run_slots * self.layout.slot_size);
                let physical_slot = location
                    .first_slot
                    .checked_add(slot_offset as u64)
                    .ok_or_else(|| invalid_state("stored entry slot overflows u64"))?;
                let file_offset = physical_slot
                    .checked_mul(self.layout.slot_size as u64)
                    .ok_or_else(|| invalid_state("stored entry offset overflows u64"))?;
                if self.direct_io {
                    let physical_len = run_slots * self.layout.slot_size;
                    let mut input = AlignedBuffer::new(physical_len);
                    read_exact_at(&self.data, input.as_mut_slice(), file_offset)
                        .map_err(|error| Error::io("read stored entry", error))?;
                    value.extend_from_slice(&input.as_slice()[..logical_len]);
                    result.data_bytes = result.data_bytes.saturating_add(physical_len);
                } else {
                    let start = value.len();
                    value.resize(start + logical_len, 0);
                    read_exact_at(&self.data, &mut value[start..], file_offset)
                        .map_err(|error| Error::io("read stored entry", error))?;
                    result.data_bytes = result.data_bytes.saturating_add(logical_len);
                }
                result.data_runs = result.data_runs.saturating_add(1);
                result.data_slots = result.data_slots.saturating_add(run_slots);
                slot_offset += run_slots;
            }
            Ok(())
        })?;
        let generation_matches = {
            let state = mutex_lock(&self.state);
            state.extents[extent as usize].generation == location.extent_generation
        };
        if value_checksum(&value) != location.checksum || !generation_matches {
            return Ok(result);
        }
        result.value = Some(value);
        Ok(result)
    }

    pub fn reclaim_candidates(&self) -> ReclaimCandidates {
        let state = mutex_lock(&self.state);
        let mut candidates = ReclaimCandidates::default();
        for (index, extent) in state.extents.iter().enumerate() {
            let priority = extent.priority as usize;
            if matches!(
                extent.role,
                ExtentRole::Current | ExtentRole::Sealed | ExtentRole::ReclaimSource
            ) {
                candidates.occupied_extents[priority] = candidates.occupied_extents[priority].saturating_add(1);
            }
            let target = match extent.role {
                ExtentRole::Current => &mut candidates.current[priority],
                ExtentRole::Sealed => &mut candidates.sealed[priority],
                _ => continue,
            };
            let victim = ExtentVictim {
                extent: u32::try_from(index).expect("extent index must fit u32"),
                generation: extent.generation,
                used: extent.used,
                priority: extent.priority,
                sequence: extent.sequence,
            };
            if target.is_none_or(|current| (victim.sequence, victim.extent) < (current.sequence, current.extent)) {
                *target = Some(victim);
            }
        }
        candidates
    }

    pub fn extent_occupancy(&self, capacity_floor_extents: [u32; 3]) -> ExtentOccupancy {
        let state = mutex_lock(&self.state);
        let mut occupied_extents = [0u32; 3];
        let mut used_slots = [0u64; 3];
        for extent in &state.extents {
            if !matches!(
                extent.role,
                ExtentRole::Current | ExtentRole::Sealed | ExtentRole::ReclaimSource
            ) {
                continue;
            }
            let priority = extent.priority as usize;
            occupied_extents[priority] = occupied_extents[priority].saturating_add(1);
            used_slots[priority] = used_slots[priority].saturating_add(u64::from(extent.used));
        }
        ExtentOccupancy::new(
            self.layout.extent_count.saturating_sub(1),
            self.layout.slots_per_extent,
            self.layout.slot_size,
            occupied_extents,
            used_slots,
            capacity_floor_extents,
        )
    }

    pub fn seal_current(&self, victim: ExtentVictim) -> Result<ExtentVictim> {
        let mut state = mutex_lock(&self.state);
        let extent = state
            .extents
            .get(victim.extent as usize)
            .copied()
            .ok_or_else(|| invalid_state("current reclaim victim is out of range"))?;
        let priority = usize::from(victim.priority.to_byte());
        if extent.role != ExtentRole::Current
            || extent.generation != victim.generation
            || extent.used != victim.used
            || extent.priority != victim.priority
            || extent.sequence != victim.sequence
            || state.current[priority] != Some(victim.extent)
        {
            return Err(invalid_state("current reclaim victim changed before seal"));
        }
        state.extents[victim.extent as usize].role = ExtentRole::Sealed;
        state.current[priority] = None;
        Ok(victim)
    }

    pub fn begin_reclaim(&self, victim: ExtentVictim) -> Result<ReclaimTransaction> {
        let mut state = mutex_lock(&self.state);
        let source = state
            .extents
            .get(victim.extent as usize)
            .copied()
            .ok_or_else(|| invalid_state("reclaim source is out of range"))?;
        if source.role != ExtentRole::Sealed
            || source.generation != victim.generation
            || source.used != victim.used
            || source.priority != victim.priority
            || state.current[usize::from(victim.priority.to_byte())].is_some()
        {
            return Err(invalid_state("reclaim source is not a stable sealed extent"));
        }
        let target = state.reserve;
        let target_state = state.extents[target as usize];
        if target_state.role != ExtentRole::Reserve || target == victim.extent {
            return Err(invalid_state("reclaim target reserve is invalid"));
        }

        state.extents[victim.extent as usize].role = ExtentRole::ReclaimSource;
        let sequence = state.next_sequence;
        let target_state = &mut state.extents[target as usize];
        target_state.used = 0;
        target_state.sequence = sequence;
        target_state.priority = victim.priority;
        target_state.role = ExtentRole::ReclaimTarget;
        let target_generation = target_state.generation;
        self.persist_state_locked(&mut state)?;
        Ok(ReclaimTransaction {
            source: victim,
            target,
            target_generation,
            priority: victim.priority,
        })
    }

    pub fn pending_reclaim(&self) -> Option<ReclaimTransaction> {
        let state = mutex_lock(&self.state);
        let (source, source_state) = state
            .extents
            .iter()
            .enumerate()
            .find(|(_, extent)| extent.role == ExtentRole::ReclaimSource)?;
        let (target, target_state) = state
            .extents
            .iter()
            .enumerate()
            .find(|(_, extent)| extent.role == ExtentRole::ReclaimTarget)?;
        Some(ReclaimTransaction {
            source: ExtentVictim {
                extent: u32::try_from(source).ok()?,
                generation: source_state.generation,
                used: source_state.used,
                priority: source_state.priority,
                sequence: source_state.sequence,
            },
            target: u32::try_from(target).ok()?,
            target_generation: target_state.generation,
            priority: target_state.priority,
        })
    }

    pub fn allocate_reclaim_target(
        &self,
        transaction: ReclaimTransaction,
        slots: u32,
    ) -> Result<Option<EntryAllocation>> {
        if slots == 0 || slots > self.layout.slots_per_extent {
            return Err(invalid_state(
                "reclaim allocation must fit completely within one extent",
            ));
        }
        let mut state = mutex_lock(&self.state);
        let target = &mut state.extents[transaction.target as usize];
        if target.role != ExtentRole::ReclaimTarget
            || target.generation != transaction.target_generation
            || target.priority != transaction.priority
        {
            return Err(invalid_state("reclaim target changed during compaction"));
        }
        if self.layout.slots_per_extent.saturating_sub(target.used) < slots {
            return Ok(None);
        }
        let extent_slot = target.used;
        target.used += slots;
        let sequence = state.next_sequence;
        state.next_sequence = state
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| invalid_state("extent allocation sequence is exhausted"))?;
        Ok(Some(EntryAllocation {
            first_slot: self
                .layout
                .slot_index(transaction.target, extent_slot)
                .expect("reclaim target slot must be in the layout"),
            extent: transaction.target,
            extent_slot,
            slot_count: slots,
            extent_generation: transaction.target_generation,
            priority: transaction.priority,
            sequence,
        }))
    }

    pub fn finish_reclaim(&self, transaction: ReclaimTransaction) -> Result<()> {
        let mut state = mutex_lock(&self.state);
        let source = state.extents[transaction.source.extent as usize];
        let target = state.extents[transaction.target as usize];
        if source.role != ExtentRole::ReclaimSource
            || source.generation != transaction.source.generation
            || target.role != ExtentRole::ReclaimTarget
            || target.generation != transaction.target_generation
            || target.priority != transaction.priority
        {
            return Err(invalid_state("reclaim transaction changed before commit"));
        }

        let source = &mut state.extents[transaction.source.extent as usize];
        source.generation = source
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid_state("extent generation is exhausted"))?;
        source.used = 0;
        source.sequence = 0;
        source.priority = CachePriority::Low;
        source.role = ExtentRole::Reserve;
        state.reserve = transaction.source.extent;

        let target = &mut state.extents[transaction.target as usize];
        if target.used < self.layout.slots_per_extent {
            target.role = ExtentRole::Current;
            state.current[usize::from(transaction.priority.to_byte())] = Some(transaction.target);
        } else {
            target.role = ExtentRole::Sealed;
        }
        self.persist_state_locked(&mut state)
    }

    pub fn slot_owners(&self, victim: ExtentVictim) -> Result<Vec<(u64, SlotOwner)>> {
        let mut owners = Vec::new();
        for slot in 0..victim.used {
            let physical_slot = self
                .layout
                .slot_index(victim.extent, slot)
                .expect("victim slot must be in the layout");
            let Some(owner) = self.read_slot_owner(physical_slot)? else {
                continue;
            };
            if owner.extent_generation == victim.generation {
                owners.push((physical_slot, owner));
            }
        }
        Ok(owners)
    }

    pub fn release(&self, victim: ExtentVictim) -> Result<()> {
        let mut state = mutex_lock(&self.state);
        let extent = &mut state.extents[victim.extent as usize];
        if extent.role != ExtentRole::Sealed || extent.generation != victim.generation || extent.used != victim.used {
            return Err(invalid_state("extent victim changed during reclamation"));
        }
        extent.generation = extent
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid_state("extent generation is exhausted"))?;
        extent.used = 0;
        extent.sequence = 0;
        extent.priority = CachePriority::Low;
        extent.role = ExtentRole::Free;
        self.persist_state_locked(&mut state)
    }

    pub fn sync_payload(&self) -> Result<()> {
        self.io
            .write(|| self.data.sync_data())
            .map_err(|error| Error::io("sync extent data", error))?;
        self.io
            .write(|| self.owners.sync_data())
            .map_err(|error| Error::io("sync extent owners", error))
    }

    pub fn checkpoint_state(&self) -> Result<()> {
        let checkpoint = self.prepare_checkpoint_state()?;
        self.persist_checkpoint_state(&checkpoint)
    }

    pub fn prepare_checkpoint_state(&self) -> Result<ExtentPoolCheckpoint> {
        let state = mutex_lock(&self.state);
        let next_state_generation = state
            .state_generation
            .checked_add(1)
            .ok_or_else(|| invalid_state("allocator state generation is exhausted"))?;
        let next_page = 1 - state.active_page;
        let mut snapshot = state.clone();
        snapshot.state_generation = next_state_generation;
        snapshot.active_page = next_page;
        Ok(ExtentPoolCheckpoint {
            base_state_generation: state.state_generation,
            base_page: state.active_page,
            next_state_generation,
            next_page,
            encoded: snapshot.encode(self.layout)?,
        })
    }

    pub fn persist_checkpoint_state(&self, checkpoint: &ExtentPoolCheckpoint) -> Result<()> {
        let offset = u64::from(checkpoint.next_page)
            .checked_mul(self.layout.state_copy_size as u64)
            .ok_or_else(|| invalid_state("allocator state offset overflows u64"))?;
        write_all_at(&self.state_file, &checkpoint.encoded, offset)
            .map_err(|error| Error::io("write allocator state", error))?;
        self.writes.allocator_runs.fetch_add(1, Ordering::Relaxed);
        self.writes
            .allocator_bytes
            .fetch_add(checkpoint.encoded.len() as u64, Ordering::Relaxed);
        self.state_file
            .sync_data()
            .map_err(|error| Error::io("sync allocator state", error))?;

        let mut state = mutex_lock(&self.state);
        if state.state_generation != checkpoint.base_state_generation || state.active_page != checkpoint.base_page {
            return Err(invalid_state(
                "allocator checkpoint cursor changed while persistence was in flight",
            ));
        }
        state.state_generation = checkpoint.next_state_generation;
        state.active_page = checkpoint.next_page;
        Ok(())
    }

    #[cfg(test)]
    fn state_snapshot(&self) -> ExtentPoolState {
        mutex_lock(&self.state).clone()
    }

    fn recover_current_tails(&self) -> Result<()> {
        let mut state = mutex_lock(&self.state);
        let mut next_sequence = state.next_sequence;
        let mut tails = state
            .current
            .iter()
            .enumerate()
            .filter_map(|(priority, extent)| {
                extent.map(|extent| {
                    (
                        extent,
                        CachePriority::from_byte(priority as u8).expect("current priority index must be valid"),
                    )
                })
            })
            .collect::<Vec<_>>();
        if let Some((target, extent)) = state
            .extents
            .iter()
            .enumerate()
            .find(|(_, extent)| extent.role == ExtentRole::ReclaimTarget)
        {
            tails.push((
                u32::try_from(target).expect("extent index must fit u32"),
                extent.priority,
            ));
        }
        for (extent, priority) in tails {
            let entry_index = extent as usize;
            let generation = state.extents[entry_index].generation;
            let start = state.extents[entry_index].used;
            let mut recovered_used = start;
            for slot in start..self.layout.slots_per_extent {
                let physical_slot = self
                    .layout
                    .slot_index(extent, slot)
                    .expect("current extent slot must be in the layout");
                let Some(owner) = self.read_slot_owner(physical_slot)? else {
                    break;
                };
                if owner.extent_generation != generation
                    || owner.priority != priority
                    || owner.stored_len == 0
                    || owner.stored_len as usize > self.layout.extent_size
                {
                    break;
                }
                recovered_used = slot + 1;
                next_sequence = next_sequence.max(owner.sequence.saturating_add(1));
            }
            state.extents[entry_index].used = recovered_used;
        }
        state.next_sequence = next_sequence;
        Ok(())
    }

    fn read_slot_owner(&self, physical_slot: u64) -> Result<Option<SlotOwner>> {
        let offset = physical_slot
            .checked_mul(SLOT_OWNER_SIZE as u64)
            .ok_or_else(|| invalid_state("extent owner offset overflows u64"))?;
        let mut input = [0; SLOT_OWNER_SIZE];
        read_exact_at(&self.owners, &mut input, offset).map_err(|error| Error::io("read extent owner", error))?;
        Ok(SlotOwner::decode(&input))
    }

    fn write_data_runs(&self, writes: &[EntryWrite<'_>], runs: &[DataWriteRun]) -> Result<()> {
        let next = AtomicUsize::new(0);
        let concurrency = self.write_concurrency.min(runs.len());
        std::thread::scope(|scope| {
            let mut workers = Vec::with_capacity(concurrency);
            for _ in 0..concurrency {
                workers.push(scope.spawn(|| -> Result<()> {
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(run) = runs.get(index) else {
                            return Ok(());
                        };
                        let len = run
                            .slots
                            .checked_mul(self.layout.slot_size)
                            .ok_or_else(|| invalid_state("extent data batch size overflows usize"))?;
                        let mut output = AlignedBuffer::new(len);
                        for piece in &run.pieces {
                            let write = writes[piece.index];
                            let input_start = piece
                                .write_slot
                                .checked_mul(self.layout.slot_size)
                                .ok_or_else(|| invalid_state("extent write input overflows"))?;
                            let input_end = piece
                                .write_slot
                                .saturating_add(piece.slots)
                                .checked_mul(self.layout.slot_size)
                                .map(|end| end.min(write.stored_len()))
                                .ok_or_else(|| invalid_state("extent write input overflows"))?;
                            let output_start = piece
                                .run_slot
                                .checked_mul(self.layout.slot_size)
                                .ok_or_else(|| invalid_state("extent write output overflows"))?;
                            let output_end = output_start
                                .checked_add(input_end.saturating_sub(input_start))
                                .ok_or_else(|| invalid_state("extent write output overflows"))?;
                            if !copy_stored_entry_range(
                                write.key,
                                write.value,
                                input_start,
                                &mut output.as_mut_slice()[output_start..output_end],
                            ) {
                                return Err(invalid_state("extent stored entry slice is invalid"));
                            }
                        }
                        let offset = run
                            .first_slot
                            .checked_mul(self.layout.slot_size as u64)
                            .ok_or_else(|| invalid_state("extent data offset overflows u64"))?;
                        self.io
                            .write(|| write_all_at(&self.data, output.as_slice(), offset))
                            .map_err(|error| Error::io("write extent data batch", error))?;
                        self.writes.data_runs.fetch_add(1, Ordering::Relaxed);
                        self.writes.data_bytes.fetch_add(len as u64, Ordering::Relaxed);
                    }
                }));
            }
            for worker in workers {
                worker.join().expect("extent data writer must not panic")?;
            }
            Ok(())
        })
    }

    fn write_slot_owner_runs(&self, writes: &[EntryWrite<'_>], runs: &[DataWriteRun]) -> Result<()> {
        for run in runs {
            let mut output = vec![0; run.slots * SLOT_OWNER_SIZE];
            for piece in &run.pieces {
                let write = writes[piece.index];
                let owner = SlotOwner {
                    key_digest: write.key_digest,
                    extent_generation: write.allocation.extent_generation,
                    stored_len: u32::try_from(write.stored_len()).expect("validated stored entry length must fit u32"),
                    value_len: u32::try_from(write.value.len()).expect("validated value length must fit u32"),
                    checksum: write.checksum,
                    priority: write.allocation.priority,
                    sequence: write.allocation.sequence,
                }
                .encode();
                for slot in piece.run_slot..piece.run_slot + piece.slots {
                    let start = slot * SLOT_OWNER_SIZE;
                    output[start..start + SLOT_OWNER_SIZE].copy_from_slice(&owner);
                }
            }
            let offset = run
                .first_slot
                .checked_mul(SLOT_OWNER_SIZE as u64)
                .ok_or_else(|| invalid_state("extent owner offset overflows u64"))?;
            self.io
                .write(|| write_all_at(&self.owners, &output, offset))
                .map_err(|error| Error::io("write extent owner batch", error))?;
            self.writes.slot_owner_runs.fetch_add(1, Ordering::Relaxed);
            self.writes
                .slot_owner_bytes
                .fetch_add(output.len() as u64, Ordering::Relaxed);
        }
        Ok(())
    }

    fn persist_state_locked(&self, state: &mut ExtentPoolState) -> Result<()> {
        let state_generation = state
            .state_generation
            .checked_add(1)
            .ok_or_else(|| invalid_state("allocator state generation is exhausted"))?;
        let page = 1 - state.active_page;
        let mut next = state.clone();
        next.state_generation = state_generation;
        next.active_page = page;
        let offset = u64::from(page)
            .checked_mul(self.layout.state_copy_size as u64)
            .ok_or_else(|| invalid_state("allocator state offset overflows u64"))?;
        let encoded = next.encode(self.layout)?;
        write_all_at(&self.state_file, &encoded, offset).map_err(|error| Error::io("write allocator state", error))?;
        self.writes.allocator_runs.fetch_add(1, Ordering::Relaxed);
        self.writes
            .allocator_bytes
            .fetch_add(encoded.len() as u64, Ordering::Relaxed);
        self.state_file
            .sync_data()
            .map_err(|error| Error::io("sync allocator state", error))?;
        *state = next;
        Ok(())
    }
}

#[derive(Debug)]
struct DataWriteRun {
    first_slot: u64,
    slots: usize,
    pieces: Vec<DataWritePiece>,
}

#[derive(Debug)]
struct DataWritePiece {
    index: usize,
    run_slot: usize,
    write_slot: usize,
    slots: usize,
}

fn data_write_runs(order: &[usize], writes: &[EntryWrite<'_>], maximum_slots: usize) -> Vec<DataWriteRun> {
    debug_assert!(maximum_slots > 0);
    let mut runs: Vec<DataWriteRun> = Vec::new();
    for index in order.iter().copied() {
        let write = writes[index];
        let mut write_slot = 0usize;
        let write_slots = write.allocation.slot_count as usize;
        while write_slot < write_slots {
            let physical_slot = write
                .allocation
                .first_slot
                .checked_add(write_slot as u64)
                .expect("validated extent write must fit the data file");
            let append = runs.last().is_some_and(|run| {
                run.first_slot.checked_add(run.slots as u64) == Some(physical_slot) && run.slots < maximum_slots
            });
            if !append {
                runs.push(DataWriteRun {
                    first_slot: physical_slot,
                    slots: 0,
                    pieces: Vec::new(),
                });
            }
            let run = runs.last_mut().expect("extent write run must exist");
            let slots = (write_slots - write_slot).min(maximum_slots - run.slots);
            run.pieces.push(DataWritePiece {
                index,
                run_slot: run.slots,
                write_slot,
                slots,
            });
            run.slots += slots;
            write_slot += slots;
        }
    }
    runs
}

fn state_file_size(layout: StoreLayout) -> Result<u64> {
    layout
        .state_copy_size
        .checked_mul(2)
        .and_then(|size| u64::try_from(size).ok())
        .ok_or_else(|| invalid_state("extent state file size overflows u64"))
}

fn layout_allocated_size(layout: StoreLayout) -> Result<u64> {
    layout
        .total_file_size
        .checked_sub(layout.index_capacity_bytes)
        .ok_or_else(|| invalid_state("extent file allocation underflows total layout size"))
}

fn invalid_state(message: &str) -> Error {
    Error::InvalidSuperblock(format!("extent state: {message}"))
}

fn mutex_lock<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::{
        format::{PAGE_SIZE, stored_entry_checksum},
        store::config::{ExtentStoreConfig, ExtentStoreOptions},
    };

    fn create_pool(root: &Path) -> ExtentPool {
        let options = ExtentStoreOptions::default().with_extent_size(PAGE_SIZE * 8);
        let layout = StoreLayout::create(
            ExtentStoreConfig::new(4 * 1024 * 1024)
                .with_slot_size(PAGE_SIZE)
                .with_options(options),
        )
        .unwrap();
        ExtentPool::create(root, layout, false, 2, Duration::ZERO, PAGE_SIZE * 4, PAGE_SIZE * 4).unwrap()
    }

    fn key(index: u64) -> EntryKey {
        let mut bytes = [index as u8; 24];
        bytes[16..].copy_from_slice(&index.to_le_bytes());
        EntryKey::new(bytes).unwrap()
    }

    fn allocated(result: AllocationResult) -> EntryAllocation {
        match result {
            AllocationResult::Allocated(allocation) => allocation,
            AllocationResult::ReclaimRequired => panic!("test allocation needs reclaim"),
        }
    }

    #[test]
    fn batch_write_read_checkpoint_and_reopen() {
        let dir = tempdir().unwrap();
        let pool = create_pool(dir.path());
        let values = (0..12).map(|index| vec![index as u8; 100 + index]).collect::<Vec<_>>();
        let allocations = (0..values.len())
            .map(|_| allocated(pool.allocate(CachePriority::Normal, 1).unwrap()))
            .collect::<Vec<_>>();
        let keys = (0..values.len()).map(|index| key(index as u64)).collect::<Vec<_>>();
        let writes = values
            .iter()
            .enumerate()
            .map(|(index, value)| EntryWrite {
                allocation: allocations[index],
                key: &keys[index],
                key_digest: KeyDigest::for_key(&keys[index]),
                value,
                checksum: stored_entry_checksum(&keys[index], value),
            })
            .collect::<Vec<_>>();
        let result = pool.write_batch(&writes).unwrap();
        assert_eq!(result.data_runs, 3);
        assert_eq!(result.slot_owner_runs, 1);
        for (index, location) in result.locations.iter().enumerate() {
            assert_eq!(
                pool.read_entry(&keys[index], *location).unwrap(),
                StoredEntryRead {
                    value: Some(values[index].clone()),
                    data_slots: 1,
                    data_runs: 1,
                    data_bytes: stored_entry_len(&keys[index], &values[index]).unwrap(),
                }
            );
        }
        pool.sync_payload().unwrap();
        pool.checkpoint_state().unwrap();
        let layout = pool.layout();
        drop(pool);

        let reopened = ExtentPool::open(dir.path(), false, 2, Duration::ZERO, PAGE_SIZE * 4, PAGE_SIZE * 4).unwrap();
        assert_eq!(reopened.layout(), layout);
        for (index, location) in result.locations.iter().enumerate() {
            assert_eq!(
                reopened.read_entry(&keys[index], *location).unwrap(),
                StoredEntryRead {
                    value: Some(values[index].clone()),
                    data_slots: 1,
                    data_runs: 1,
                    data_bytes: stored_entry_len(&keys[index], &values[index]).unwrap(),
                }
            );
        }
    }

    #[test]
    fn point_read_rechecks_generation_after_payload_io() {
        let dir = tempdir().unwrap();
        let pool = create_pool(dir.path());
        let old_value = vec![0x11; 512];
        let mut replacement = vec![0x22; 512];
        replacement[508..].copy_from_slice(&[0xce, 0xe0, 0x15, 0xb2]);
        let key = key(1);
        let key_digest = KeyDigest::for_key(&key);
        let checksum = stored_entry_checksum(&key, &old_value);
        assert_ne!(old_value, replacement);
        assert_eq!(stored_entry_checksum(&key, &replacement), checksum);

        let allocation = allocated(pool.allocate(CachePriority::Normal, 1).unwrap());
        let location = pool
            .write_batch(&[EntryWrite {
                allocation,
                key: &key,
                key_digest,
                value: &old_value,
                checksum,
            }])
            .unwrap()
            .locations[0];
        let (victim, is_current) = pool.reclaim_candidates().oldest(CachePriority::Normal).unwrap();
        assert!(is_current);
        let victim = pool.seal_current(victim).unwrap();

        let value = pool
            .read_encoded_entry_after_validation(location, || {
                pool.release(victim)?;
                let replacement_allocation = allocated(pool.allocate(CachePriority::Normal, 1)?);
                assert_eq!(replacement_allocation.first_slot, allocation.first_slot);
                pool.write_batch(&[EntryWrite {
                    allocation: replacement_allocation,
                    key: &key,
                    key_digest,
                    value: &replacement,
                    checksum,
                }])?;
                Ok(())
            })
            .unwrap();

        assert_eq!(value.value, None);
        assert_eq!(value.data_runs, 1);
        assert_eq!(value.data_slots, 1);
        assert_eq!(value.data_bytes, stored_entry_len(&key, &old_value).unwrap());
    }

    #[test]
    fn reopen_recovers_uncheckpointed_current_tail() {
        let dir = tempdir().unwrap();
        let pool = create_pool(dir.path());
        let allocation = allocated(pool.allocate(CachePriority::High, 1).unwrap());
        pool.checkpoint_state().unwrap();
        let value = vec![9; 512];
        let key = key(1);
        pool.write_batch(&[EntryWrite {
            allocation,
            key: &key,
            key_digest: KeyDigest::for_key(&key),
            value: &value,
            checksum: stored_entry_checksum(&key, &value),
        }])
        .unwrap();
        pool.sync_payload().unwrap();
        drop(pool);

        let reopened = ExtentPool::open(dir.path(), false, 1, Duration::ZERO, PAGE_SIZE * 4, PAGE_SIZE * 4).unwrap();
        let next = allocated(reopened.allocate(CachePriority::High, 1).unwrap());
        assert_eq!(next.extent, allocation.extent);
        assert_eq!(next.extent_slot, allocation.extent_slot + 1);
    }

    #[test]
    fn falls_back_to_the_older_valid_state_copy() {
        let dir = tempdir().unwrap();
        let pool = create_pool(dir.path());
        pool.checkpoint_state().unwrap();
        let state = pool.state_snapshot();
        let layout = pool.layout();
        drop(pool);

        let state_file = open_cache_file(&dir.path().join(STATE_FILE), false, false).unwrap();
        let offset = u64::from(state.active_page) * layout.state_copy_size as u64;
        write_all_at(&state_file, &[0xff], offset).unwrap();
        state_file.sync_data().unwrap();

        let reopened = ExtentPool::open(dir.path(), false, 1, Duration::ZERO, PAGE_SIZE * 4, PAGE_SIZE * 4).unwrap();
        assert!(reopened.state_snapshot().state_generation < state.state_generation);
    }
}
