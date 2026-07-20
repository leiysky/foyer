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
    format::{
        PAGE_SIZE, copy_stored_entry_range, decode_entry_value, decode_stored_entry, stored_entry_len, value_checksum,
    },
    model::{CachePriority, EntryKey, KeyDigest},
    store::{
        format::{ENTRY_OWNER_SIZE, EntryLocation, EntryOwner, ExtentPoolState, ExtentRole, StoreLayout},
        io::{IoSchedulerStats, PayloadIoScheduler},
        stats::{ExtentOccupancy, PhysicalWriteStats},
    },
};

pub(crate) const DATA_FILE: &str = "data";
pub(crate) const ENTRY_DIRECTORY_FILE: &str = "directory";
pub(crate) const LEGACY_SLOT_OWNER_FILE: &str = "owners";
pub(crate) const STATE_FILE: &str = "state";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryAllocation {
    pub data_offset: u64,
    pub extent: u32,
    pub extent_offset: u32,
    pub directory_entry: u32,
    pub stored_len: u32,
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
    pub entry_directory_runs: usize,
    pub data_bytes: usize,
    pub entry_directory_bytes: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct StoredEntryRead {
    pub value: Option<Vec<u8>>,
    pub data_frames: usize,
    pub data_runs: usize,
    pub data_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtentVictim {
    pub extent: u32,
    pub generation: u32,
    pub used_bytes: u32,
    pub entries: u32,
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
    entry_directory: File,
    state_file: File,
    io: PayloadIoScheduler,
    layout: StoreLayout,
    direct_io: bool,
    write_concurrency: usize,
    read_run_size: usize,
    write_run_size: usize,
    allocated_size: u64,
    state: Mutex<ExtentPoolState>,
    writes: PoolWriteCounters,
}

#[derive(Debug, Default)]
struct PoolWriteCounters {
    data_runs: AtomicU64,
    data_bytes: AtomicU64,
    entry_directory_runs: AtomicU64,
    entry_directory_bytes: AtomicU64,
    allocator_runs: AtomicU64,
    allocator_bytes: AtomicU64,
}

impl PoolWriteCounters {
    fn snapshot(&self) -> PhysicalWriteStats {
        PhysicalWriteStats {
            data_runs: self.data_runs.load(Ordering::Relaxed),
            data_bytes: self.data_bytes.load(Ordering::Relaxed),
            entry_directory_runs: self.entry_directory_runs.load(Ordering::Relaxed),
            entry_directory_bytes: self.entry_directory_bytes.load(Ordering::Relaxed),
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
        let entry_directory = open_cache_file(&root.join(ENTRY_DIRECTORY_FILE), true, false)
            .map_err(|error| Error::io("create extent entry directory", error))?;
        reserve_cache_file(&entry_directory, layout.entry_directory_file_size)
            .map_err(|error| Error::io("reserve extent entry directory", error))?;
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
            entry_directory,
            state_file,
            io,
            layout,
            direct_io,
            write_concurrency,
            read_run_size,
            write_run_size,
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
        let entry_directory = open_cache_file(&root.join(ENTRY_DIRECTORY_FILE), false, false)
            .map_err(|error| Error::io("open extent entry directory", error))?;
        let state_file = open_cache_file(&root.join(STATE_FILE), false, false)
            .map_err(|error| Error::io("open extent state file", error))?;
        let data_size = data
            .metadata()
            .map_err(|error| Error::io("read extent data file size", error))?
            .len();
        let directory_size = entry_directory
            .metadata()
            .map_err(|error| Error::io("read extent entry-directory file size", error))?
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
            let Some(layout) = StoreLayout::discover(input, data_size, directory_size, state_size) else {
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
        ensure_cache_file_reserved(&entry_directory, layout.entry_directory_file_size)
            .map_err(|error| Error::io("verify extent entry-directory reservation", error))?;
        ensure_cache_file_reserved(&state_file, state_file_size(layout)?)
            .map_err(|error| Error::io("verify extent state reservation", error))?;
        let allocated_size = layout_allocated_size(layout)?;
        let io = PayloadIoScheduler::new(write_concurrency, io_read_priority_duration)
            .map_err(|error| Error::io("configure extent I/O scheduler", error))?;

        let pool = Self {
            data,
            entry_directory,
            state_file,
            io,
            layout,
            direct_io,
            write_concurrency,
            read_run_size,
            write_run_size,
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

    pub fn allocate(&self, priority: CachePriority, stored_len: usize) -> Result<AllocationResult> {
        if stored_len == 0 || stored_len > self.layout.extent_size {
            return Err(invalid_state("extent allocation must fit completely within one extent"));
        }
        let stored_len =
            u32::try_from(stored_len).map_err(|_| invalid_state("extent allocation length does not fit u32"))?;
        let priority_index = usize::from(priority.to_byte());
        let mut state = mutex_lock(&self.state);
        loop {
            if let Some(extent) = state.current[priority_index] {
                let entry_index = extent as usize;
                let current = state.extents[entry_index];
                let remaining = (self.layout.extent_size as u32).saturating_sub(current.used_bytes);
                if remaining >= stored_len && current.entries < self.layout.entries_per_extent {
                    let extent_offset = current.used_bytes;
                    let directory_entry = current.entries;
                    let extent_generation = state.extents[entry_index].generation;
                    state.extents[entry_index].used_bytes += stored_len;
                    state.extents[entry_index].entries += 1;
                    let sequence = state.next_sequence;
                    state.next_sequence = state
                        .next_sequence
                        .checked_add(1)
                        .ok_or_else(|| invalid_state("extent allocation sequence is exhausted"))?;
                    let data_offset = self
                        .layout
                        .data_offset(extent, extent_offset)
                        .expect("current extent byte offset must be in the layout");
                    return Ok(AllocationResult::Allocated(EntryAllocation {
                        data_offset,
                        extent,
                        extent_offset,
                        directory_entry,
                        stored_len,
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
            extent_state.used_bytes = 0;
            extent_state.entries = 0;
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
            if write.allocation.stored_len as usize != stored_len {
                return Err(invalid_state("extent allocation does not match the value length"));
            }
            let Some(end) = write.allocation.extent_offset.checked_add(write.allocation.stored_len) else {
                return Err(invalid_state("extent allocation does not match the value length"));
            };
            if end as usize > self.layout.extent_size
                || write.allocation.directory_entry >= self.layout.entries_per_extent
                || self
                    .layout
                    .data_offset(write.allocation.extent, write.allocation.extent_offset)
                    != Some(write.allocation.data_offset)
            {
                return Err(invalid_state("extent allocation does not match the value length"));
            }
        }
        {
            let state = mutex_lock(&self.state);
            for write in writes {
                let allocation = write.allocation;
                let extent = state
                    .extents
                    .get(allocation.extent as usize)
                    .ok_or_else(|| invalid_state("extent allocation is outside allocator state"))?;
                if extent.generation != allocation.extent_generation
                    || extent.priority != allocation.priority
                    || extent.entries <= allocation.directory_entry
                    || extent.used_bytes < allocation.extent_offset.saturating_add(allocation.stored_len)
                    || matches!(
                        extent.role,
                        ExtentRole::Free | ExtentRole::Reserve | ExtentRole::ReclaimSource
                    )
                {
                    return Err(invalid_state("extent allocation is no longer writable"));
                }
            }
        }

        let mut order = (0..writes.len()).collect::<Vec<_>>();
        order.sort_unstable_by_key(|index| writes[*index].allocation.data_offset);
        for pair in order.windows(2) {
            let previous = writes[pair[0]];
            let previous_end = previous
                .allocation
                .data_offset
                .checked_add(previous.stored_len() as u64)
                .ok_or_else(|| invalid_state("extent allocation end overflows u64"))?;
            if previous_end > writes[pair[1]].allocation.data_offset {
                return Err(invalid_state("extent batch allocations overlap"));
            }
        }
        let data_runs = data_write_runs(&order, writes, self.write_run_size)?;
        self.write_data_runs(writes, &data_runs)?;
        let entry_directory_runs = self.write_entry_directory_runs(writes)?;
        self.seal_write_frames(writes)?;

        let mut locations = vec![None; writes.len()];
        for (index, write) in writes.iter().enumerate() {
            locations[index] = Some(EntryLocation {
                data_offset: write.allocation.data_offset,
                extent_generation: write.allocation.extent_generation,
                stored_len: u32::try_from(write.stored_len()).expect("validated stored entry length must fit u32"),
                checksum: write.checksum,
                priority: write.allocation.priority,
            });
        }
        Ok(EntryWriteResult {
            locations: locations
                .into_iter()
                .map(|location| location.expect("every write must have a location"))
                .collect(),
            data_runs: data_runs.len(),
            entry_directory_runs,
            data_bytes: data_runs.iter().map(|run| run.len).sum(),
            entry_directory_bytes: writes.len().saturating_mul(ENTRY_OWNER_SIZE),
        })
    }

    fn location_range(&self, location: EntryLocation) -> Option<(u32, u32)> {
        let len = location.stored_len as usize;
        if len == 0 || len > self.layout.extent_size {
            return None;
        }
        let (extent, extent_offset) = self.layout.locate_data_offset(location.data_offset)?;
        ((extent_offset as usize).checked_add(len)? <= self.layout.extent_size).then_some((extent, extent_offset))
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
        let Some((extent, extent_offset)) = self.location_range(location) else {
            return Ok(result);
        };
        let extent_end = extent_offset.saturating_add(location.stored_len);
        {
            let state = mutex_lock(&self.state);
            let state = state.extents[extent as usize];
            if state.generation != location.extent_generation || state.used_bytes < extent_end {
                return Ok(result);
            }
        }
        after_validation()?;

        let mut value = Vec::with_capacity(location.stored_len as usize);
        self.io.read(|| -> Result<()> {
            let entry_start = location.data_offset;
            let entry_end = entry_start
                .checked_add(u64::from(location.stored_len))
                .ok_or_else(|| invalid_state("stored entry end overflows u64"))?;
            if self.direct_io {
                let mut file_offset = align_down(entry_start, PAGE_SIZE as u64);
                let physical_end = align_up(entry_end, PAGE_SIZE as u64)
                    .ok_or_else(|| invalid_state("stored entry aligned end overflows u64"))?;
                while file_offset < physical_end {
                    let physical_len = usize::try_from((physical_end - file_offset).min(self.read_run_size as u64))
                        .map_err(|_| invalid_state("stored entry read run does not fit usize"))?;
                    let mut input = AlignedBuffer::new(physical_len);
                    read_exact_at(&self.data, input.as_mut_slice(), file_offset)
                        .map_err(|error| Error::io("read stored entry", error))?;
                    let copy_start = entry_start.saturating_sub(file_offset) as usize;
                    let copy_end = usize::try_from(entry_end.min(file_offset + physical_len as u64) - file_offset)
                        .map_err(|_| invalid_state("stored entry read slice does not fit usize"))?;
                    value.extend_from_slice(&input.as_slice()[copy_start..copy_end]);
                    result.data_bytes = result.data_bytes.saturating_add(physical_len);
                    result.data_runs = result.data_runs.saturating_add(1);
                    file_offset += physical_len as u64;
                }
            } else {
                let mut file_offset = entry_start;
                while file_offset < entry_end {
                    let logical_len = usize::try_from((entry_end - file_offset).min(self.read_run_size as u64))
                        .map_err(|_| invalid_state("stored entry read run does not fit usize"))?;
                    let start = value.len();
                    value.resize(start + logical_len, 0);
                    read_exact_at(&self.data, &mut value[start..], file_offset)
                        .map_err(|error| Error::io("read stored entry", error))?;
                    result.data_bytes = result.data_bytes.saturating_add(logical_len);
                    result.data_runs = result.data_runs.saturating_add(1);
                    file_offset += logical_len as u64;
                }
            }
            Ok(())
        })?;
        result.data_frames = (extent_offset as usize % PAGE_SIZE + location.stored_len as usize).div_ceil(PAGE_SIZE);
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
                used_bytes: extent.used_bytes,
                entries: extent.entries,
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
        let mut used_entries = [0u64; 3];
        let mut used_bytes = [0u64; 3];
        for extent in &state.extents {
            if !matches!(
                extent.role,
                ExtentRole::Current | ExtentRole::Sealed | ExtentRole::ReclaimSource
            ) {
                continue;
            }
            let priority = extent.priority as usize;
            occupied_extents[priority] = occupied_extents[priority].saturating_add(1);
            used_entries[priority] = used_entries[priority].saturating_add(u64::from(extent.entries));
            used_bytes[priority] = used_bytes[priority].saturating_add(u64::from(extent.used_bytes));
        }
        ExtentOccupancy::new(
            self.layout.extent_count.saturating_sub(1),
            self.layout.entries_per_extent,
            occupied_extents,
            used_entries,
            used_bytes,
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
            || extent.used_bytes != victim.used_bytes
            || extent.entries != victim.entries
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
            || source.used_bytes != victim.used_bytes
            || source.entries != victim.entries
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
        target_state.used_bytes = 0;
        target_state.entries = 0;
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
                used_bytes: source_state.used_bytes,
                entries: source_state.entries,
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
        stored_len: usize,
    ) -> Result<Option<EntryAllocation>> {
        if stored_len == 0 || stored_len > self.layout.extent_size {
            return Err(invalid_state(
                "reclaim allocation must fit completely within one extent",
            ));
        }
        let stored_len =
            u32::try_from(stored_len).map_err(|_| invalid_state("reclaim allocation length does not fit u32"))?;
        let mut state = mutex_lock(&self.state);
        let target = &mut state.extents[transaction.target as usize];
        if target.role != ExtentRole::ReclaimTarget
            || target.generation != transaction.target_generation
            || target.priority != transaction.priority
        {
            return Err(invalid_state("reclaim target changed during compaction"));
        }
        if (self.layout.extent_size as u32).saturating_sub(target.used_bytes) < stored_len
            || target.entries >= self.layout.entries_per_extent
        {
            return Ok(None);
        }
        let extent_offset = target.used_bytes;
        let directory_entry = target.entries;
        target.used_bytes += stored_len;
        target.entries += 1;
        let sequence = state.next_sequence;
        state.next_sequence = state
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| invalid_state("extent allocation sequence is exhausted"))?;
        Ok(Some(EntryAllocation {
            data_offset: self
                .layout
                .data_offset(transaction.target, extent_offset)
                .expect("reclaim target byte offset must be in the layout"),
            extent: transaction.target,
            extent_offset,
            directory_entry,
            stored_len,
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
        source.used_bytes = 0;
        source.entries = 0;
        source.sequence = 0;
        source.priority = CachePriority::Low;
        source.role = ExtentRole::Reserve;
        state.reserve = transaction.source.extent;

        let target = &mut state.extents[transaction.target as usize];
        if (target.used_bytes as usize) < self.layout.extent_size && target.entries < self.layout.entries_per_extent {
            target.role = ExtentRole::Current;
            state.current[usize::from(transaction.priority.to_byte())] = Some(transaction.target);
        } else {
            target.role = ExtentRole::Sealed;
        }
        self.persist_state_locked(&mut state)
    }

    pub fn entry_owners(&self, victim: ExtentVictim) -> Result<Vec<(u64, EntryOwner)>> {
        let mut owners = Vec::new();
        let mut previous_end = 0u32;
        let mut previous_sequence = 0u64;
        for entry in 0..victim.entries {
            let directory_index = self
                .layout
                .directory_index(victim.extent, entry)
                .expect("victim directory entry must be in the layout");
            let owner = self
                .read_entry_owner(directory_index)?
                .ok_or_else(|| invalid_state("occupied extent contains an invalid entry-directory record"))?;
            let end = owner
                .extent_offset
                .checked_add(owner.stored_len)
                .ok_or_else(|| invalid_state("entry-directory range overflows u32"))?;
            let gap = owner.extent_offset.saturating_sub(previous_end);
            if owner.extent_generation != victim.generation
                || owner.priority != victim.priority
                || owner.stored_len == 0
                || owner.value_len == 0
                || owner.value_len >= owner.stored_len
                || owner.extent_offset < previous_end
                || end > victim.used_bytes
                || owner.sequence <= previous_sequence
                || (gap > 0 && (!(owner.extent_offset as usize).is_multiple_of(PAGE_SIZE) || gap as usize >= PAGE_SIZE))
            {
                return Err(invalid_state("occupied extent entry directory is inconsistent"));
            }
            let data_offset = self
                .layout
                .data_offset(victim.extent, owner.extent_offset)
                .ok_or_else(|| invalid_state("entry-directory offset is outside the data file"))?;
            owners.push((data_offset, owner));
            previous_end = end;
            previous_sequence = owner.sequence;
        }
        Ok(owners)
    }

    pub fn release(&self, victim: ExtentVictim) -> Result<()> {
        let mut state = mutex_lock(&self.state);
        let extent = &mut state.extents[victim.extent as usize];
        if extent.role != ExtentRole::Sealed
            || extent.generation != victim.generation
            || extent.used_bytes != victim.used_bytes
            || extent.entries != victim.entries
        {
            return Err(invalid_state("extent victim changed during reclamation"));
        }
        extent.generation = extent
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid_state("extent generation is exhausted"))?;
        extent.used_bytes = 0;
        extent.entries = 0;
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
            .write(|| self.entry_directory.sync_data())
            .map_err(|error| Error::io("sync extent entry directory", error))
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
        let recovery_sequence_floor = state.next_sequence;
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
            let start_entry = state.extents[entry_index].entries;
            let mut recovered_entries = start_entry;
            let mut recovered_used = state.extents[entry_index].used_bytes;
            let mut previous_sequence = recovery_sequence_floor.saturating_sub(1);
            for entry in start_entry..self.layout.entries_per_extent {
                let directory_index = self
                    .layout
                    .directory_index(extent, entry)
                    .expect("current extent directory entry must be in the layout");
                let Some(owner) = self.read_entry_owner(directory_index)? else {
                    break;
                };
                let Some(end) = owner.extent_offset.checked_add(owner.stored_len) else {
                    break;
                };
                let gap = owner.extent_offset.saturating_sub(recovered_used);
                if owner.extent_generation != generation
                    || owner.priority != priority
                    || owner.stored_len == 0
                    || owner.value_len == 0
                    || owner.value_len >= owner.stored_len
                    || owner.stored_len as usize > self.layout.extent_size
                    || owner.extent_offset < recovered_used
                    || end as usize > self.layout.extent_size
                    || owner.sequence <= previous_sequence
                    || (gap > 0
                        && (!(owner.extent_offset as usize).is_multiple_of(PAGE_SIZE) || gap as usize >= PAGE_SIZE))
                {
                    break;
                }
                recovered_used = end;
                recovered_entries = entry + 1;
                previous_sequence = owner.sequence;
                next_sequence = next_sequence.max(owner.sequence.saturating_add(1));
            }
            if recovered_entries > start_entry {
                recovered_used = u32::try_from(
                    (recovered_used as usize)
                        .next_multiple_of(PAGE_SIZE)
                        .min(self.layout.extent_size),
                )
                .expect("extent size must fit u32");
            }
            state.extents[entry_index].used_bytes = recovered_used;
            state.extents[entry_index].entries = recovered_entries;
        }
        state.next_sequence = next_sequence;
        Ok(())
    }

    fn read_entry_owner(&self, directory_index: u64) -> Result<Option<EntryOwner>> {
        let offset = directory_index
            .checked_mul(ENTRY_OWNER_SIZE as u64)
            .ok_or_else(|| invalid_state("extent entry-directory offset overflows u64"))?;
        let mut input = [0; ENTRY_OWNER_SIZE];
        read_exact_at(&self.entry_directory, &mut input, offset)
            .map_err(|error| Error::io("read extent entry directory", error))?;
        Ok(EntryOwner::decode(&input))
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
                        let mut output = AlignedBuffer::new(run.len);
                        for piece in &run.pieces {
                            let write = writes[piece.index];
                            let output_end = piece
                                .run_offset
                                .checked_add(piece.len)
                                .ok_or_else(|| invalid_state("extent write output overflows"))?;
                            if !copy_stored_entry_range(
                                write.key,
                                write.value,
                                piece.write_offset,
                                &mut output.as_mut_slice()[piece.run_offset..output_end],
                            ) {
                                return Err(invalid_state("extent stored entry slice is invalid"));
                            }
                        }
                        self.io
                            .write(|| write_all_at(&self.data, output.as_slice(), run.data_offset))
                            .map_err(|error| Error::io("write extent data batch", error))?;
                        self.writes.data_runs.fetch_add(1, Ordering::Relaxed);
                        self.writes.data_bytes.fetch_add(run.len as u64, Ordering::Relaxed);
                    }
                }));
            }
            for worker in workers {
                worker.join().expect("extent data writer must not panic")?;
            }
            Ok(())
        })
    }

    fn write_entry_directory_runs(&self, writes: &[EntryWrite<'_>]) -> Result<usize> {
        let mut order = (0..writes.len()).collect::<Vec<_>>();
        order.sort_unstable_by_key(|index| {
            self.layout
                .directory_index(
                    writes[*index].allocation.extent,
                    writes[*index].allocation.directory_entry,
                )
                .expect("validated directory entry must be in the layout")
        });
        let mut runs = 0usize;
        let mut start = 0usize;
        while start < order.len() {
            let first = writes[order[start]].allocation;
            let first_directory_index = self
                .layout
                .directory_index(first.extent, first.directory_entry)
                .expect("validated directory entry must be in the layout");
            let mut end = start + 1;
            while end < order.len() {
                let allocation = writes[order[end]].allocation;
                let directory_index = self
                    .layout
                    .directory_index(allocation.extent, allocation.directory_entry)
                    .expect("validated directory entry must be in the layout");
                if directory_index != first_directory_index + (end - start) as u64 {
                    break;
                }
                end += 1;
            }
            let mut output = vec![0; (end - start) * ENTRY_OWNER_SIZE];
            for (output_index, write_index) in order[start..end].iter().copied().enumerate() {
                let write = writes[write_index];
                let owner = EntryOwner {
                    key_digest: write.key_digest,
                    extent_generation: write.allocation.extent_generation,
                    extent_offset: write.allocation.extent_offset,
                    stored_len: u32::try_from(write.stored_len()).expect("validated stored entry length must fit u32"),
                    value_len: u32::try_from(write.value.len()).expect("validated value length must fit u32"),
                    checksum: write.checksum,
                    priority: write.allocation.priority,
                    sequence: write.allocation.sequence,
                }
                .encode();
                let offset = output_index * ENTRY_OWNER_SIZE;
                output[offset..offset + ENTRY_OWNER_SIZE].copy_from_slice(&owner);
            }
            let offset = first_directory_index
                .checked_mul(ENTRY_OWNER_SIZE as u64)
                .ok_or_else(|| invalid_state("extent entry-directory offset overflows u64"))?;
            self.io
                .write(|| write_all_at(&self.entry_directory, &output, offset))
                .map_err(|error| Error::io("write extent entry directory", error))?;
            self.writes.entry_directory_runs.fetch_add(1, Ordering::Relaxed);
            self.writes
                .entry_directory_bytes
                .fetch_add(output.len() as u64, Ordering::Relaxed);
            runs += 1;
            start = end;
        }
        Ok(runs)
    }

    fn seal_write_frames(&self, writes: &[EntryWrite<'_>]) -> Result<()> {
        let mut state = mutex_lock(&self.state);
        let mut extents = writes.iter().map(|write| write.allocation.extent).collect::<Vec<_>>();
        extents.sort_unstable();
        extents.dedup();
        for extent in extents {
            let extent_state = &mut state.extents[extent as usize];
            let aligned = (extent_state.used_bytes as usize)
                .next_multiple_of(PAGE_SIZE)
                .min(self.layout.extent_size);
            extent_state.used_bytes = u32::try_from(aligned).expect("extent size must fit u32");
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
    data_offset: u64,
    len: usize,
    pieces: Vec<DataWritePiece>,
}

#[derive(Debug)]
struct DataWritePiece {
    index: usize,
    run_offset: usize,
    write_offset: usize,
    len: usize,
}

fn data_write_runs(order: &[usize], writes: &[EntryWrite<'_>], maximum_size: usize) -> Result<Vec<DataWriteRun>> {
    if maximum_size == 0 || !maximum_size.is_multiple_of(PAGE_SIZE) {
        return Err(invalid_state("extent write run size must be a positive page multiple"));
    }
    let mut runs = Vec::new();
    let mut group_start = 0usize;
    while group_start < order.len() {
        let first = writes[order[group_start]];
        if !first.allocation.data_offset.is_multiple_of(PAGE_SIZE as u64) {
            return Err(invalid_state("extent write batch does not begin on a frame boundary"));
        }
        let mut group_end = group_start + 1;
        let mut logical_end = first
            .allocation
            .data_offset
            .checked_add(first.stored_len() as u64)
            .ok_or_else(|| invalid_state("extent write group end overflows u64"))?;
        while group_end < order.len() {
            let write = writes[order[group_end]];
            if write.allocation.data_offset != logical_end {
                break;
            }
            logical_end = logical_end
                .checked_add(write.stored_len() as u64)
                .ok_or_else(|| invalid_state("extent write group end overflows u64"))?;
            group_end += 1;
        }
        let physical_end = align_up(logical_end, PAGE_SIZE as u64)
            .ok_or_else(|| invalid_state("extent write group aligned end overflows u64"))?;
        let mut run_start = first.allocation.data_offset;
        while run_start < physical_end {
            let len = usize::try_from((physical_end - run_start).min(maximum_size as u64))
                .map_err(|_| invalid_state("extent write run length does not fit usize"))?;
            let run_end = run_start + len as u64;
            let mut pieces = Vec::new();
            for index in order[group_start..group_end].iter().copied() {
                let write = writes[index];
                let write_start = write.allocation.data_offset;
                let write_end = write_start + write.stored_len() as u64;
                let overlap_start = write_start.max(run_start);
                let overlap_end = write_end.min(run_end);
                if overlap_start < overlap_end {
                    pieces.push(DataWritePiece {
                        index,
                        run_offset: (overlap_start - run_start) as usize,
                        write_offset: (overlap_start - write_start) as usize,
                        len: (overlap_end - overlap_start) as usize,
                    });
                }
            }
            runs.push(DataWriteRun {
                data_offset: run_start,
                len,
                pieces,
            });
            run_start = run_end;
        }
        group_start = group_end;
    }
    Ok(runs)
}

const fn align_down(value: u64, alignment: u64) -> u64 {
    value / alignment * alignment
}

fn align_up(value: u64, alignment: u64) -> Option<u64> {
    value
        .checked_add(alignment - 1)
        .map(|value| value / alignment * alignment)
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
                .with_entry_charge(PAGE_SIZE)
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
        let keys = (0..values.len()).map(|index| key(index as u64)).collect::<Vec<_>>();
        let allocations = values
            .iter()
            .zip(&keys)
            .map(|(value, key)| {
                allocated(
                    pool.allocate(CachePriority::Normal, stored_entry_len(key, value).unwrap())
                        .unwrap(),
                )
            })
            .collect::<Vec<_>>();
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
        assert_eq!(result.data_runs, 2);
        assert_eq!(result.data_bytes, PAGE_SIZE * 2);
        assert_eq!(result.entry_directory_runs, 1);
        assert_eq!(result.entry_directory_bytes, values.len() * ENTRY_OWNER_SIZE);
        for (index, location) in result.locations.iter().enumerate() {
            assert_eq!(
                pool.read_entry(&keys[index], *location).unwrap(),
                StoredEntryRead {
                    value: Some(values[index].clone()),
                    data_frames: 1,
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
                    data_frames: 1,
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

        let stored_len = stored_entry_len(&key, &old_value).unwrap();
        let allocation = allocated(pool.allocate(CachePriority::Normal, stored_len).unwrap());
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
                let replacement_allocation = allocated(pool.allocate(CachePriority::Normal, stored_len)?);
                assert_eq!(replacement_allocation.data_offset, allocation.data_offset);
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
        assert_eq!(value.data_frames, 1);
        assert_eq!(value.data_bytes, stored_entry_len(&key, &old_value).unwrap());
    }

    #[test]
    fn aligned_read_covers_an_unaligned_packed_entry() {
        let dir = tempdir().unwrap();
        let mut pool = create_pool(dir.path());
        // Exercise the direct-I/O alignment branch with a regular test file on every platform.
        // Linux-only tests separately open the file with O_DIRECT.
        pool.direct_io = true;
        let keys = [key(1), key(2)];
        let values = [vec![1; 2_000], vec![2; 3_000]];
        let allocations: [EntryAllocation; 2] = std::array::from_fn(|index| {
            allocated(
                pool.allocate(
                    CachePriority::Normal,
                    stored_entry_len(&keys[index], &values[index]).unwrap(),
                )
                .unwrap(),
            )
        });
        let writes: [EntryWrite<'_>; 2] = std::array::from_fn(|index| EntryWrite {
            allocation: allocations[index],
            key: &keys[index],
            key_digest: KeyDigest::for_key(&keys[index]),
            value: &values[index],
            checksum: stored_entry_checksum(&keys[index], &values[index]),
        });
        let locations = pool.write_batch(&writes).unwrap().locations;

        assert!(!locations[1].data_offset.is_multiple_of(PAGE_SIZE as u64));
        let loaded = pool.read_entry(&keys[1], locations[1]).unwrap();
        assert_eq!(loaded.value, Some(values[1].clone()));
        assert_eq!(loaded.data_frames, 2);
        assert_eq!(loaded.data_runs, 1);
        assert_eq!(loaded.data_bytes, PAGE_SIZE * 2);
    }

    #[test]
    fn reopen_recovers_uncheckpointed_current_tail() {
        let dir = tempdir().unwrap();
        let pool = create_pool(dir.path());
        let value = vec![9; 512];
        let first_key = key(1);
        let stored_len = stored_entry_len(&first_key, &value).unwrap();
        let first = allocated(pool.allocate(CachePriority::High, stored_len).unwrap());
        pool.write_batch(&[EntryWrite {
            allocation: first,
            key: &first_key,
            key_digest: KeyDigest::for_key(&first_key),
            value: &value,
            checksum: stored_entry_checksum(&first_key, &value),
        }])
        .unwrap();
        pool.sync_payload().unwrap();
        pool.checkpoint_state().unwrap();

        let tail_key = key(2);
        let tail = allocated(pool.allocate(CachePriority::High, stored_len).unwrap());
        pool.write_batch(&[EntryWrite {
            allocation: tail,
            key: &tail_key,
            key_digest: KeyDigest::for_key(&tail_key),
            value: &value,
            checksum: stored_entry_checksum(&tail_key, &value),
        }])
        .unwrap();
        pool.sync_payload().unwrap();
        drop(pool);

        let reopened = ExtentPool::open(dir.path(), false, 1, Duration::ZERO, PAGE_SIZE * 4, PAGE_SIZE * 4).unwrap();
        let next = allocated(reopened.allocate(CachePriority::High, stored_len).unwrap());
        assert_eq!(next.extent, tail.extent);
        assert_eq!(
            next.extent_offset as usize,
            (tail.extent_offset as usize + stored_len).next_multiple_of(PAGE_SIZE)
        );
        assert_eq!(next.directory_entry, tail.directory_entry + 1);
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
