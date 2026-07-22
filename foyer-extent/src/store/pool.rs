use std::{
    fs,
    fs::File,
    path::Path,
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use rayon::prelude::*;

use crate::{
    error::{Error, Result},
    file::{
        AlignedBuffer, allocated_file_size, ensure_cache_file_preallocated, open_cache_file, preallocate_cache_file,
        read_exact_at, write_all_at,
    },
    format::{
        ContentDigest, PAGE_SIZE, copy_stored_entry_range, decode_entry_value, encoded_entry_value_digest,
        stored_entry_len,
    },
    model::{CachePriority, EntryKey},
    store::{
        format::{EntryLocation, ExtentPoolState, StoreLayout},
        io::{IoSchedulerStats, PayloadIoScheduler},
        stats::{ExtentOccupancy, PhysicalWriteStats},
    },
};

pub(crate) const DATA_FILE: &str = "data";
pub(crate) const STATE_FILE: &str = "state";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryAllocation {
    pub data_offset: u64,
    pub extent: u32,
    pub extent_offset: u32,
    pub stored_len: u32,
    pub extent_generation: u32,
    pub priority: CachePriority,
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
    pub value: &'a [u8],
    pub content_digest: ContentDigest,
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
    pub data_bytes: usize,
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
    pub activation_sequence: u64,
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
            (Some(sealed), Some(current))
                if (current.activation_sequence, current.extent) < (sealed.activation_sequence, sealed.extent) =>
            {
                Some((current, true))
            }
            (Some(sealed), _) => Some((sealed, false)),
            (None, Some(current)) => Some((current, true)),
            (None, None) => None,
        }
    }
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
    state_file: File,
    io: PayloadIoScheduler,
    layout: StoreLayout,
    direct_io: bool,
    write_concurrency: usize,
    write_pool: Option<rayon::ThreadPool>,
    read_run_size: usize,
    write_run_size: usize,
    payload_dirty: AtomicBool,
    state: Mutex<ExtentPoolState>,
    liveness: Box<[AtomicU64]>,
    writes: PoolWriteCounters,
}

#[derive(Debug, Default)]
struct PoolWriteCounters {
    data_runs: AtomicU64,
    data_bytes: AtomicU64,
    data_syncs: AtomicU64,
    allocator_runs: AtomicU64,
    allocator_bytes: AtomicU64,
    allocator_syncs: AtomicU64,
}

impl PoolWriteCounters {
    fn snapshot(&self) -> PhysicalWriteStats {
        PhysicalWriteStats {
            data_runs: self.data_runs.load(Ordering::Relaxed),
            data_bytes: self.data_bytes.load(Ordering::Relaxed),
            data_syncs: self.data_syncs.load(Ordering::Relaxed),
            allocator_runs: self.allocator_runs.load(Ordering::Relaxed),
            allocator_bytes: self.allocator_bytes.load(Ordering::Relaxed),
            allocator_syncs: self.allocator_syncs.load(Ordering::Relaxed),
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
        preallocate_cache_file(&data, layout.data_file_size)
            .map_err(|error| Error::io("preallocate extent data file", error))?;
        let state_file = open_cache_file(&root.join(STATE_FILE), true, false)
            .map_err(|error| Error::io("create extent state file", error))?;
        let state_file_size = state_file_size(layout)?;
        preallocate_cache_file(&state_file, state_file_size)
            .map_err(|error| Error::io("preallocate extent state file", error))?;

        let state = ExtentPoolState::empty(layout);
        let liveness = liveness_table(&state);
        write_all_at(&state_file, &state.encode(layout)?, 0)
            .map_err(|error| Error::io("write initial extent state", error))?;
        state_file
            .sync_data()
            .map_err(|error| Error::io("sync initial extent state", error))?;
        let io = PayloadIoScheduler::new(write_concurrency, io_read_priority_duration)
            .map_err(|error| Error::io("configure extent I/O scheduler", error))?;
        let write_pool = data_write_pool(write_concurrency)?;
        Ok(Self {
            data,
            state_file,
            io,
            layout,
            direct_io,
            write_concurrency,
            write_pool,
            read_run_size,
            write_run_size,
            payload_dirty: AtomicBool::new(false),
            state: Mutex::new(state),
            liveness,
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
        let state_file = open_cache_file(&root.join(STATE_FILE), false, false)
            .map_err(|error| Error::io("open extent state file", error))?;
        let data_size = data
            .metadata()
            .map_err(|error| Error::io("read extent data file size", error))?
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
            let Some(layout) = StoreLayout::discover(input, data_size, state_size) else {
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
        ensure_cache_file_preallocated(&data, layout.data_file_size)
            .map_err(|error| Error::io("verify extent data allocation", error))?;
        ensure_cache_file_preallocated(&state_file, state_file_size(layout)?)
            .map_err(|error| Error::io("verify extent state allocation", error))?;
        let io = PayloadIoScheduler::new(write_concurrency, io_read_priority_duration)
            .map_err(|error| Error::io("configure extent I/O scheduler", error))?;
        let write_pool = data_write_pool(write_concurrency)?;

        let liveness = liveness_table(&state);
        let pool = Self {
            data,
            state_file,
            io,
            layout,
            direct_io,
            write_concurrency,
            write_pool,
            read_run_size,
            write_run_size,
            payload_dirty: AtomicBool::new(false),
            state: Mutex::new(state),
            liveness,
            writes: PoolWriteCounters::default(),
        };
        pool.publish_all_liveness();
        Ok(pool)
    }

    pub const fn layout(&self) -> StoreLayout {
        self.layout
    }

    pub fn allocated_size(&self) -> Result<u64> {
        [&self.data, &self.state_file].into_iter().try_fold(0u64, |size, file| {
            allocated_file_size(file)
                .map_err(|error| Error::io("read allocated extent file size", error))
                .and_then(|bytes| {
                    size.checked_add(bytes)
                        .ok_or_else(|| invalid_state("allocated extent file size overflows u64"))
                })
        })
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
                if remaining >= stored_len && current.entries < self.layout.maximum_entries_per_extent {
                    let extent_offset = current.used_bytes;
                    let extent_generation = state.extents[entry_index].generation;
                    state.extents[entry_index].used_bytes += stored_len;
                    state.extents[entry_index].entries += 1;
                    let extent_state = state.extents[entry_index];
                    let data_offset = self
                        .layout
                        .data_offset(extent, extent_offset)
                        .expect("current extent byte offset must be in the layout");
                    self.publish_liveness(extent, extent_state);
                    return Ok(AllocationResult::Allocated(EntryAllocation {
                        data_offset,
                        extent,
                        extent_offset,
                        stored_len,
                        extent_generation,
                        priority,
                    }));
                }
                state.current[priority_index] = None;
                continue;
            }

            let Some(extent) = state.extents.iter().position(|extent| extent.is_free()) else {
                return Ok(AllocationResult::ReclaimRequired);
            };
            let extent = u32::try_from(extent).map_err(|_| invalid_state("free extent index does not fit u32"))?;
            let activation_sequence = state.next_activation_sequence;
            state.next_activation_sequence = state
                .next_activation_sequence
                .checked_add(1)
                .ok_or_else(|| invalid_state("extent activation sequence is exhausted"))?;
            let extent_state = &mut state.extents[extent as usize];
            extent_state.used_bytes = 0;
            extent_state.entries = 0;
            extent_state.activation_sequence = activation_sequence;
            extent_state.priority = priority;
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
                    || extent.used_bytes < allocation.extent_offset.saturating_add(allocation.stored_len)
                    || extent.is_free()
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
        self.payload_dirty.store(true, Ordering::Release);
        self.seal_write_frames(writes)?;

        let mut locations = vec![None; writes.len()];
        for (index, write) in writes.iter().enumerate() {
            locations[index] = Some(EntryLocation {
                data_offset: write.allocation.data_offset,
                extent_generation: write.allocation.extent_generation,
                stored_len: u32::try_from(write.stored_len()).expect("validated stored entry length must fit u32"),
                content_digest: write.content_digest,
                priority: write.allocation.priority,
            });
        }
        Ok(EntryWriteResult {
            locations: locations
                .into_iter()
                .map(|location| location.expect("every write must have a location"))
                .collect(),
            data_runs: data_runs.len(),
            data_bytes: data_runs.iter().map(|run| run.len).sum(),
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

    pub fn location_is_live(&self, location: EntryLocation) -> bool {
        let Some((extent, extent_offset)) = self.location_range(location) else {
            return false;
        };
        let extent_end = extent_offset.saturating_add(location.stored_len);
        let (generation, used_bytes) = decode_liveness(self.liveness[extent as usize].load(Ordering::Acquire));
        generation == location.extent_generation && used_bytes >= extent_end
    }

    pub fn read_entry(&self, key: &EntryKey, location: EntryLocation) -> Result<StoredEntryRead> {
        let mut result = self.read_encoded_entry(location)?;
        result.value = result.value.and_then(|stored| decode_entry_value(stored, key));
        Ok(result)
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
        let (generation, used_bytes) = decode_liveness(self.liveness[extent as usize].load(Ordering::Acquire));
        if generation != location.extent_generation || used_bytes < extent_end {
            return Ok(result);
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
        let (generation, _) = decode_liveness(self.liveness[extent as usize].load(Ordering::Acquire));
        let generation_matches = generation == location.extent_generation;
        if encoded_entry_value_digest(&value) != Some(location.content_digest) || !generation_matches {
            return Ok(result);
        }
        result.value = Some(value);
        Ok(result)
    }

    pub fn reclaim_candidates(&self) -> ReclaimCandidates {
        let state = mutex_lock(&self.state);
        let mut candidates = ReclaimCandidates::default();
        for (index, extent) in state.extents.iter().enumerate() {
            if extent.is_free() {
                continue;
            }
            let priority = extent.priority as usize;
            candidates.occupied_extents[priority] = candidates.occupied_extents[priority].saturating_add(1);
            let extent_index = u32::try_from(index).expect("extent index must fit u32");
            let target = if state.current[priority] == Some(extent_index) {
                &mut candidates.current[priority]
            } else {
                &mut candidates.sealed[priority]
            };
            let victim = ExtentVictim {
                extent: extent_index,
                generation: extent.generation,
                used_bytes: extent.used_bytes,
                entries: extent.entries,
                priority: extent.priority,
                activation_sequence: extent.activation_sequence,
            };
            if target.is_none_or(|current| {
                (victim.activation_sequence, victim.extent) < (current.activation_sequence, current.extent)
            }) {
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
            if extent.is_free() {
                continue;
            }
            let priority = extent.priority as usize;
            occupied_extents[priority] = occupied_extents[priority].saturating_add(1);
            used_entries[priority] = used_entries[priority].saturating_add(u64::from(extent.entries));
            used_bytes[priority] = used_bytes[priority].saturating_add(u64::from(extent.used_bytes));
        }
        ExtentOccupancy::new(
            self.layout.extent_count,
            self.layout.planned_entries_per_extent,
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
        if extent.is_free()
            || extent.generation != victim.generation
            || extent.used_bytes != victim.used_bytes
            || extent.entries != victim.entries
            || extent.priority != victim.priority
            || extent.activation_sequence != victim.activation_sequence
            || state.current[priority] != Some(victim.extent)
        {
            return Err(invalid_state("current reclaim victim changed before seal"));
        }
        state.current[priority] = None;
        Ok(victim)
    }

    pub fn release(&self, victim: ExtentVictim) -> Result<()> {
        let mut state = mutex_lock(&self.state);
        let extent = &state.extents[victim.extent as usize];
        let priority = usize::from(extent.priority.to_byte());
        if extent.is_free()
            || state.current[priority] == Some(victim.extent)
            || extent.generation != victim.generation
            || extent.used_bytes != victim.used_bytes
            || extent.entries != victim.entries
            || extent.priority != victim.priority
            || extent.activation_sequence != victim.activation_sequence
        {
            return Err(invalid_state("extent victim changed during reclamation"));
        }
        let mut next = state.clone();
        let extent = &mut next.extents[victim.extent as usize];
        extent.generation = extent
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid_state("extent generation is exhausted"))?;
        extent.used_bytes = 0;
        extent.entries = 0;
        extent.activation_sequence = 0;
        extent.priority = CachePriority::Low;
        self.persist_state_locked(&mut next)?;
        *state = next;
        self.publish_liveness(victim.extent, state.extents[victim.extent as usize]);
        Ok(())
    }

    pub fn sync_payload(&self) -> Result<()> {
        if !self.payload_dirty.load(Ordering::Acquire) {
            return Ok(());
        }
        self.io
            .write(|| self.data.sync_data())
            .map_err(|error| Error::io("sync extent data", error))?;
        self.writes.data_syncs.fetch_add(1, Ordering::Relaxed);
        self.payload_dirty.store(false, Ordering::Release);
        Ok(())
    }

    #[cfg(test)]
    pub fn payload_is_dirty(&self) -> bool {
        self.payload_dirty.load(Ordering::Acquire)
    }

    #[cfg(test)]
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
        self.writes.allocator_syncs.fetch_add(1, Ordering::Relaxed);

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

    fn write_data_runs(&self, writes: &[EntryWrite<'_>], runs: &[DataWriteRun]) -> Result<()> {
        if runs.is_empty() {
            return Ok(());
        }
        let maximum_run_size = runs
            .iter()
            .map(|run| run.len)
            .max()
            .expect("a non-empty run set must have a maximum size");
        if self.write_concurrency == 1 || runs.len() == 1 {
            let mut output = AlignedBuffer::new(maximum_run_size);
            for run in runs {
                self.write_data_run(writes, run, &mut output.as_mut_slice()[..run.len])?;
            }
            return Ok(());
        }

        self.write_pool
            .as_ref()
            .expect("write concurrency above one must have a persistent data-write pool")
            .install(|| {
                runs.par_iter().try_for_each_init(
                    || AlignedBuffer::new(maximum_run_size),
                    |output, run| self.write_data_run(writes, run, &mut output.as_mut_slice()[..run.len]),
                )
            })
    }

    fn write_data_run(&self, writes: &[EntryWrite<'_>], run: &DataWriteRun, output: &mut [u8]) -> Result<()> {
        debug_assert_eq!(output.len(), run.len);
        output.fill(0);
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
                &mut output[piece.run_offset..output_end],
            ) {
                return Err(invalid_state("extent stored entry slice is invalid"));
            }
        }
        self.io
            .write(|| write_all_at(&self.data, output, run.data_offset))
            .map_err(|error| Error::io("write extent data batch", error))?;
        self.writes.data_runs.fetch_add(1, Ordering::Relaxed);
        self.writes.data_bytes.fetch_add(run.len as u64, Ordering::Relaxed);
        Ok(())
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
            self.publish_liveness(extent, *extent_state);
        }
        Ok(())
    }

    fn publish_all_liveness(&self) {
        let state = mutex_lock(&self.state);
        for (extent, extent_state) in state.extents.iter().copied().enumerate() {
            self.publish_liveness(extent as u32, extent_state);
        }
    }

    fn publish_liveness(&self, extent: u32, state: crate::store::format::ExtentState) {
        self.liveness[extent as usize].store(encode_liveness(state.generation, state.used_bytes), Ordering::Release);
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
        self.writes.allocator_syncs.fetch_add(1, Ordering::Relaxed);
        *state = next;
        Ok(())
    }
}

fn liveness_table(state: &ExtentPoolState) -> Box<[AtomicU64]> {
    state
        .extents
        .iter()
        .map(|state| AtomicU64::new(encode_liveness(state.generation, state.used_bytes)))
        .collect()
}

const fn encode_liveness(generation: u32, used_bytes: u32) -> u64 {
    (generation as u64) << 32 | used_bytes as u64
}

const fn decode_liveness(word: u64) -> (u32, u32) {
    ((word >> 32) as u32, word as u32)
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

fn invalid_state(message: &str) -> Error {
    Error::InvalidSuperblock(format!("extent state: {message}"))
}

fn data_write_pool(concurrency: usize) -> Result<Option<rayon::ThreadPool>> {
    if concurrency <= 1 {
        return Ok(None);
    }
    rayon::ThreadPoolBuilder::new()
        .num_threads(concurrency)
        .thread_name(|index| format!("extent-data-{index}"))
        .build()
        .map(Some)
        .map_err(|error| Error::InvalidConfig(format!("create extent data-write pool: {error}")))
}

fn mutex_lock<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::{
        format::{PAGE_SIZE, stored_entry_checksum, value_digest},
        store::config::{ExtentStoreConfig, ExtentStoreOptions},
    };

    fn create_pool(root: &Path) -> ExtentPool {
        create_pool_with_extent(root, PAGE_SIZE * 8, 4 * 1024 * 1024)
    }

    fn create_pool_with_extent(root: &Path, extent_size: usize, capacity: u64) -> ExtentPool {
        let options = ExtentStoreOptions::default().with_extent_size(extent_size);
        let layout = StoreLayout::create(
            ExtentStoreConfig::new(capacity)
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
    fn activation_sequence_advances_once_per_extent() {
        let dir = tempdir().unwrap();
        let pool = create_pool(dir.path());
        let first = allocated(pool.allocate(CachePriority::Normal, 1).unwrap());
        let second = allocated(pool.allocate(CachePriority::Normal, 1).unwrap());
        let state = pool.state_snapshot();

        assert_eq!(second.extent, first.extent);
        assert_eq!(state.extents[first.extent as usize].activation_sequence, 1);
        assert_eq!(state.next_activation_sequence, 2);
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
                value,
                content_digest: value_digest(value),
            })
            .collect::<Vec<_>>();
        let result = pool.write_batch(&writes).unwrap();
        assert!(writes.len() > pool.layout().planned_entries_per_extent as usize);
        assert_eq!(result.data_runs, 1);
        assert_eq!(result.data_bytes, PAGE_SIZE);
        assert!(pool.payload_is_dirty());
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
        assert!(!pool.payload_is_dirty());
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
    fn random_small_writes_can_exceed_planned_cardinality() {
        let dir = tempdir().unwrap();
        let pool = create_pool_with_extent(dir.path(), PAGE_SIZE * 64, 16 * 1024 * 1024);
        let entry_count = 1_100;
        let keys = (0..entry_count).map(|index| key(index as u64)).collect::<Vec<_>>();
        let values = (0..entry_count)
            .map(|index| {
                let mixed = (index as u64)
                    .wrapping_add(0x9e37_79b9_7f4a_7c15)
                    .wrapping_mul(0xbf58_476d_1ce4_e5b9);
                vec![mixed as u8; 1 + (mixed as usize % 128)]
            })
            .collect::<Vec<_>>();
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
                value,
                content_digest: value_digest(value),
            })
            .collect::<Vec<_>>();
        let result = pool.write_batch(&writes).unwrap();
        assert!(entry_count > pool.layout().planned_entries_per_extent as usize);
        assert_eq!(result.locations.len(), entry_count);
        pool.sync_payload().unwrap();
    }

    #[test]
    fn point_read_rechecks_generation_after_payload_io() {
        let dir = tempdir().unwrap();
        let pool = create_pool(dir.path());
        let old_value = vec![0x11; 512];
        let mut replacement = vec![0x22; 512];
        replacement[508..].copy_from_slice(&[0xce, 0xe0, 0x15, 0xb2]);
        let key = key(1);
        let checksum = stored_entry_checksum(&key, &old_value);
        assert_ne!(old_value, replacement);
        assert_eq!(stored_entry_checksum(&key, &replacement), checksum);

        let stored_len = stored_entry_len(&key, &old_value).unwrap();
        let allocation = allocated(pool.allocate(CachePriority::Normal, stored_len).unwrap());
        let location = pool
            .write_batch(&[EntryWrite {
                allocation,
                key: &key,
                value: &old_value,
                content_digest: value_digest(&old_value),
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
                    value: &replacement,
                    content_digest: value_digest(&replacement),
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
            value: &values[index],
            content_digest: value_digest(&values[index]),
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
    fn default_read_run_coalesces_one_mib_value() {
        let dir = tempdir().unwrap();
        let extent_size = 2 * 1024 * 1024;
        let options = ExtentStoreOptions::default().with_extent_size(extent_size);
        let layout = StoreLayout::create(
            ExtentStoreConfig::new(64 * 1024 * 1024)
                .with_entry_charge(PAGE_SIZE)
                .with_options(options),
        )
        .unwrap();
        let mut pool = ExtentPool::create(
            dir.path(),
            layout,
            false,
            1,
            Duration::ZERO,
            options.read_run_size,
            options.write_run_size,
        )
        .unwrap();
        // Exercise the direct-I/O alignment branch without requiring O_DIRECT in the unit test.
        pool.direct_io = true;

        let key = key(1);
        let value = vec![0x5a; 1024 * 1024];
        let allocation = allocated(
            pool.allocate(CachePriority::Normal, stored_entry_len(&key, &value).unwrap())
                .unwrap(),
        );
        let location = pool
            .write_batch(&[EntryWrite {
                allocation,
                key: &key,
                value: &value,
                content_digest: value_digest(&value),
            }])
            .unwrap()
            .locations[0];

        let loaded = pool.read_entry(&key, location).unwrap();
        assert_eq!(loaded.value, Some(value));
        assert_eq!(loaded.data_runs, 1);
        assert!(loaded.data_frames > 1);
    }

    #[test]
    fn reopen_discards_uncheckpointed_current_tail() {
        let dir = tempdir().unwrap();
        let pool = create_pool(dir.path());
        let value = vec![9; 512];
        let first_key = key(1);
        let stored_len = stored_entry_len(&first_key, &value).unwrap();
        let first = allocated(pool.allocate(CachePriority::High, stored_len).unwrap());
        pool.write_batch(&[EntryWrite {
            allocation: first,
            key: &first_key,
            value: &value,
            content_digest: value_digest(&value),
        }])
        .unwrap();
        pool.sync_payload().unwrap();
        pool.checkpoint_state().unwrap();

        let tail_key = key(2);
        let tail = allocated(pool.allocate(CachePriority::High, stored_len).unwrap());
        pool.write_batch(&[EntryWrite {
            allocation: tail,
            key: &tail_key,
            value: &value,
            content_digest: value_digest(&value),
        }])
        .unwrap();
        pool.sync_payload().unwrap();
        drop(pool);

        let reopened = ExtentPool::open(dir.path(), false, 1, Duration::ZERO, PAGE_SIZE * 4, PAGE_SIZE * 4).unwrap();
        let next = allocated(reopened.allocate(CachePriority::High, stored_len).unwrap());
        assert_eq!(next.extent, tail.extent);
        assert_eq!(next.extent_offset, tail.extent_offset);
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
