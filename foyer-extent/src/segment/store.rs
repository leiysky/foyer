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
    format::{copy_blob_range, decode_blob, decode_stored_blob, stored_blob_len, value_checksum},
    model::{BlobKey, CachePriority, KeyDigest},
    segment::{
        format::{AllocatorState, OWNER_RECORD_SIZE, OwnerRecord, SegmentLayout, SegmentLocation, SegmentRole},
        io::{IoSchedulerStats, SegmentIoScheduler},
        stats::{PhysicalWriteStats, PriorityOccupancy},
    },
};

pub(crate) const DATA_FILE: &str = "data";
pub(crate) const OWNER_FILE: &str = "owners";
pub(crate) const STATE_FILE: &str = "state";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentAllocation {
    pub physical_slot: u64,
    pub segment: u32,
    pub slot: u32,
    pub slots: u32,
    pub segment_generation: u32,
    pub priority: CachePriority,
    pub sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentAllocationResult {
    Allocated(SegmentAllocation),
    ReclaimRequired,
}

#[derive(Debug, Clone, Copy)]
pub struct SegmentWrite<'a> {
    pub allocation: SegmentAllocation,
    pub key: &'a BlobKey,
    pub key_digest: KeyDigest,
    pub value: &'a [u8],
    pub checksum: u32,
}

impl SegmentWrite<'_> {
    fn stored_len(self) -> usize {
        stored_blob_len(self.key, self.value).expect("validated stored blob length must fit usize")
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SegmentWriteResult {
    pub locations: Vec<SegmentLocation>,
    pub data_runs: usize,
    pub owner_runs: usize,
    pub data_bytes: usize,
    pub owner_bytes: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SegmentBlobReadResult {
    pub value: Option<Vec<u8>>,
    pub data_slots: usize,
    pub data_runs: usize,
    pub data_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentVictim {
    pub segment: u32,
    pub generation: u32,
    pub used: u32,
    pub priority: CachePriority,
    pub sequence: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ReclaimCandidates {
    occupied_segments: [u32; 3],
    sealed: [Option<SegmentVictim>; 3],
    current: [Option<SegmentVictim>; 3],
}

impl ReclaimCandidates {
    pub const fn occupied_segments(self, priority: CachePriority) -> u32 {
        self.occupied_segments[priority as usize]
    }

    pub fn oldest(self, priority: CachePriority) -> Option<(SegmentVictim, bool)> {
        let priority = priority as usize;
        match (self.sealed[priority], self.current[priority]) {
            (Some(sealed), Some(current))
                if (current.sequence, current.segment) < (sealed.sequence, sealed.segment) =>
            {
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
    pub source: SegmentVictim,
    pub target: u32,
    pub target_generation: u32,
    pub priority: CachePriority,
}

/// An immutable allocator image captured at one engine publication boundary.
///
/// Allocations may continue after capture. Only the generation/page cursor is installed back into
/// the live allocator after this exact image reaches durable storage; reclaim persistence is
/// serialized separately and therefore cannot race this cursor transition.
#[derive(Debug)]
pub struct AllocatorCheckpoint {
    base_generation: u64,
    base_page: u8,
    next_generation: u64,
    next_page: u8,
    encoded: Vec<u8>,
}

#[derive(Debug)]
pub struct SegmentStore {
    data: File,
    owners: File,
    state_file: File,
    io: SegmentIoScheduler,
    layout: SegmentLayout,
    direct_io: bool,
    write_concurrency: usize,
    read_run_slots: usize,
    write_run_slots: usize,
    allocated_size: u64,
    state: Mutex<AllocatorState>,
    writes: StoreWriteCounters,
}

#[derive(Debug, Default)]
struct StoreWriteCounters {
    data_runs: AtomicU64,
    data_bytes: AtomicU64,
    owner_runs: AtomicU64,
    owner_bytes: AtomicU64,
    allocator_runs: AtomicU64,
    allocator_bytes: AtomicU64,
}

impl StoreWriteCounters {
    fn snapshot(&self) -> PhysicalWriteStats {
        PhysicalWriteStats {
            data_runs: self.data_runs.load(Ordering::Relaxed),
            data_bytes: self.data_bytes.load(Ordering::Relaxed),
            owner_runs: self.owner_runs.load(Ordering::Relaxed),
            owner_bytes: self.owner_bytes.load(Ordering::Relaxed),
            allocator_runs: self.allocator_runs.load(Ordering::Relaxed),
            allocator_bytes: self.allocator_bytes.load(Ordering::Relaxed),
            ..Default::default()
        }
    }
}

impl SegmentStore {
    pub fn create(
        root: &Path,
        layout: SegmentLayout,
        direct_io: bool,
        write_concurrency: usize,
        io_read_priority_duration: Duration,
        read_run_size: usize,
        write_run_size: usize,
    ) -> Result<Self> {
        fs::create_dir_all(root).map_err(|error| Error::io("create segment cache directory", error))?;
        let data = open_cache_file(&root.join(DATA_FILE), true, direct_io)
            .map_err(|error| Error::io("create segment data file", error))?;
        reserve_cache_file(&data, layout.data_file_size)
            .map_err(|error| Error::io("reserve segment data file", error))?;
        let owners = open_cache_file(&root.join(OWNER_FILE), true, false)
            .map_err(|error| Error::io("create segment owner file", error))?;
        reserve_cache_file(&owners, layout.owner_file_size)
            .map_err(|error| Error::io("reserve segment owner file", error))?;
        let state_file = open_cache_file(&root.join(STATE_FILE), true, false)
            .map_err(|error| Error::io("create segment state file", error))?;
        let state_file_size = state_file_size(layout)?;
        reserve_cache_file(&state_file, state_file_size)
            .map_err(|error| Error::io("reserve segment state file", error))?;

        let state = AllocatorState::empty(layout);
        write_all_at(&state_file, &state.encode(layout)?, 0)
            .map_err(|error| Error::io("write initial segment state", error))?;
        state_file
            .sync_data()
            .map_err(|error| Error::io("sync initial segment state", error))?;
        let allocated_size = layout_allocated_size(layout)?;
        let io = SegmentIoScheduler::new(write_concurrency, io_read_priority_duration)
            .map_err(|error| Error::io("configure segment I/O scheduler", error))?;
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
            writes: StoreWriteCounters::default(),
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
            .map_err(|error| Error::io("open segment data file", error))?;
        let owners = open_cache_file(&root.join(OWNER_FILE), false, false)
            .map_err(|error| Error::io("open segment owner file", error))?;
        let state_file = open_cache_file(&root.join(STATE_FILE), false, false)
            .map_err(|error| Error::io("open segment state file", error))?;
        let data_size = data
            .metadata()
            .map_err(|error| Error::io("read segment data file size", error))?
            .len();
        let owner_size = owners
            .metadata()
            .map_err(|error| Error::io("read segment owner file size", error))?
            .len();
        let state_size = state_file
            .metadata()
            .map_err(|error| Error::io("read segment state file size", error))?
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
            read_exact_at(&state_file, output, offset).map_err(|error| Error::io("read segment state copy", error))?;
        }

        let mut candidates = Vec::new();
        for (page, input) in copies.iter().enumerate() {
            let Some(layout) = SegmentLayout::discover(input, data_size, owner_size, state_size) else {
                continue;
            };
            let Some(state) = AllocatorState::decode(input, layout, page as u8) else {
                continue;
            };
            candidates.push((layout, state));
        }
        if candidates.len() == 2 && candidates[0].0 != candidates[1].0 {
            return Err(invalid_state("valid state copies disagree on the layout"));
        }
        let Some((layout, state)) = candidates
            .into_iter()
            .max_by_key(|(_, state)| (state.generation, state.active_page))
        else {
            return Err(invalid_state("both allocator state copies are invalid"));
        };
        ensure_cache_file_reserved(&data, layout.data_file_size)
            .map_err(|error| Error::io("verify segment data reservation", error))?;
        ensure_cache_file_reserved(&owners, layout.owner_file_size)
            .map_err(|error| Error::io("verify segment owner reservation", error))?;
        ensure_cache_file_reserved(&state_file, state_file_size(layout)?)
            .map_err(|error| Error::io("verify segment state reservation", error))?;
        let allocated_size = layout_allocated_size(layout)?;
        let io = SegmentIoScheduler::new(write_concurrency, io_read_priority_duration)
            .map_err(|error| Error::io("configure segment I/O scheduler", error))?;

        let store = Self {
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
            writes: StoreWriteCounters::default(),
        };
        store.recover_current_tails()?;
        Ok(store)
    }

    pub const fn layout(&self) -> SegmentLayout {
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

    pub fn allocate(&self, priority: CachePriority, slots: u32) -> Result<SegmentAllocationResult> {
        if slots == 0 || slots > self.layout.slots_per_segment {
            return Err(invalid_state(
                "segment allocation must fit completely within one segment",
            ));
        }
        let priority_index = usize::from(priority.to_byte());
        let mut state = mutex_lock(&self.state);
        loop {
            if let Some(segment) = state.current[priority_index] {
                let segment_index = segment as usize;
                let remaining = self
                    .layout
                    .slots_per_segment
                    .saturating_sub(state.segments[segment_index].used);
                if remaining >= slots {
                    let slot = state.segments[segment_index].used;
                    let segment_generation = state.segments[segment_index].generation;
                    state.segments[segment_index].used += slots;
                    let sequence = state.next_sequence;
                    state.next_sequence = state
                        .next_sequence
                        .checked_add(1)
                        .ok_or_else(|| invalid_state("segment allocation sequence is exhausted"))?;
                    let physical_slot = self
                        .layout
                        .physical_slot(segment, slot)
                        .expect("current segment slot must be in the layout");
                    return Ok(SegmentAllocationResult::Allocated(SegmentAllocation {
                        physical_slot,
                        segment,
                        slot,
                        slots,
                        segment_generation,
                        priority,
                        sequence,
                    }));
                }
                state.segments[segment_index].role = SegmentRole::Sealed;
                state.current[priority_index] = None;
                continue;
            }

            let Some(segment) = state
                .segments
                .iter()
                .position(|segment| segment.role == SegmentRole::Free)
            else {
                return Ok(SegmentAllocationResult::ReclaimRequired);
            };
            let segment = u32::try_from(segment).map_err(|_| invalid_state("free segment index does not fit u32"))?;
            let sequence = state.next_sequence;
            let segment_state = &mut state.segments[segment as usize];
            segment_state.used = 0;
            segment_state.sequence = sequence;
            segment_state.priority = priority;
            segment_state.role = SegmentRole::Current;
            state.current[priority_index] = Some(segment);

            // Normal allocator transitions are folded into the next coordinated checkpoint.
            // SegmentEngine persists this state after payload sync and before index publication.
        }
    }

    pub fn write_batch(&self, writes: &[SegmentWrite<'_>]) -> Result<SegmentWriteResult> {
        if writes.is_empty() {
            return Ok(SegmentWriteResult::default());
        }
        for write in writes {
            if write.value.is_empty() {
                return Err(Error::EmptyValue);
            }
            let stored_len = write.stored_len();
            if stored_len > self.layout.segment_size {
                return Err(Error::ValueTooLarge {
                    len: stored_len,
                    maximum: self.layout.segment_size,
                });
            }
            let slots = stored_len.div_ceil(self.layout.slot_size);
            if usize::try_from(write.allocation.slots).ok() != Some(slots) {
                return Err(invalid_state("segment allocation does not match the value length"));
            }
        }

        let mut order = (0..writes.len()).collect::<Vec<_>>();
        order.sort_unstable_by_key(|index| writes[*index].allocation.physical_slot);
        for pair in order.windows(2) {
            let previous = writes[pair[0]].allocation;
            let previous_end = previous
                .physical_slot
                .checked_add(u64::from(previous.slots))
                .ok_or_else(|| invalid_state("segment allocation end overflows u64"))?;
            if previous_end > writes[pair[1]].allocation.physical_slot {
                return Err(invalid_state("segment batch allocations overlap"));
            }
        }
        let owner_runs = segment_write_runs(&order, writes, usize::MAX);
        let data_runs = segment_write_runs(&order, writes, self.write_run_slots);
        self.write_data_runs(writes, &data_runs)?;
        self.write_owner_runs(writes, &owner_runs)?;

        let mut locations = vec![None; writes.len()];
        for (index, write) in writes.iter().enumerate() {
            locations[index] = Some(SegmentLocation {
                physical_slot: write.allocation.physical_slot,
                segment_generation: write.allocation.segment_generation,
                stored_len: u32::try_from(write.stored_len()).expect("validated stored blob length must fit u32"),
                checksum: write.checksum,
                priority: write.allocation.priority,
            });
        }
        let allocated_slots = writes.iter().fold(0usize, |slots, write| {
            slots.saturating_add(write.allocation.slots as usize)
        });
        Ok(SegmentWriteResult {
            locations: locations
                .into_iter()
                .map(|location| location.expect("every write must have a location"))
                .collect(),
            data_runs: data_runs.len(),
            owner_runs: owner_runs.len(),
            data_bytes: allocated_slots.saturating_mul(self.layout.slot_size),
            owner_bytes: allocated_slots.saturating_mul(OWNER_RECORD_SIZE),
        })
    }

    fn location_slots(&self, location: SegmentLocation) -> Option<(u32, u32, u32)> {
        let len = location.stored_len as usize;
        if len == 0 || len > self.layout.segment_size {
            return None;
        }
        let slots = u32::try_from(len.div_ceil(self.layout.slot_size)).ok()?;
        let (segment, slot) = self.layout.segment_for_slot(location.physical_slot)?;
        (slot.checked_add(slots)? <= self.layout.slots_per_segment).then_some((segment, slot, slots))
    }

    pub fn get_blob(&self, key: &BlobKey, location: SegmentLocation) -> Result<SegmentBlobReadResult> {
        let mut result = self.read_stored_blob(location)?;
        result.value = result.value.and_then(|stored| decode_blob(stored, key));
        Ok(result)
    }

    pub fn get_stored_blob(&self, location: SegmentLocation) -> Result<Option<(BlobKey, Vec<u8>)>> {
        let result = self.read_stored_blob(location)?;
        Ok(result.value.and_then(decode_stored_blob))
    }

    fn read_stored_blob(&self, location: SegmentLocation) -> Result<SegmentBlobReadResult> {
        self.read_stored_blob_after_validation(location, || Ok(()))
    }

    fn read_stored_blob_after_validation<F>(
        &self,
        location: SegmentLocation,
        after_validation: F,
    ) -> Result<SegmentBlobReadResult>
    where
        F: FnOnce() -> Result<()>,
    {
        let mut result = SegmentBlobReadResult::default();
        let Some((segment, slot, slots)) = self.location_slots(location) else {
            return Ok(result);
        };
        {
            let state = mutex_lock(&self.state);
            let state = state.segments[segment as usize];
            if state.generation != location.segment_generation || state.used < slot.saturating_add(slots) {
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
                    .physical_slot
                    .checked_add(slot_offset as u64)
                    .ok_or_else(|| invalid_state("segment blob slot overflows u64"))?;
                let file_offset = physical_slot
                    .checked_mul(self.layout.slot_size as u64)
                    .ok_or_else(|| invalid_state("segment blob offset overflows u64"))?;
                if self.direct_io {
                    let physical_len = run_slots * self.layout.slot_size;
                    let mut input = AlignedBuffer::new(physical_len);
                    read_exact_at(&self.data, input.as_mut_slice(), file_offset)
                        .map_err(|error| Error::io("read segment blob", error))?;
                    value.extend_from_slice(&input.as_slice()[..logical_len]);
                    result.data_bytes = result.data_bytes.saturating_add(physical_len);
                } else {
                    let start = value.len();
                    value.resize(start + logical_len, 0);
                    read_exact_at(&self.data, &mut value[start..], file_offset)
                        .map_err(|error| Error::io("read segment blob", error))?;
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
            state.segments[segment as usize].generation == location.segment_generation
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
        for (index, segment) in state.segments.iter().enumerate() {
            let priority = segment.priority as usize;
            if matches!(
                segment.role,
                SegmentRole::Current | SegmentRole::Sealed | SegmentRole::ReclaimSource
            ) {
                candidates.occupied_segments[priority] = candidates.occupied_segments[priority].saturating_add(1);
            }
            let target = match segment.role {
                SegmentRole::Current => &mut candidates.current[priority],
                SegmentRole::Sealed => &mut candidates.sealed[priority],
                _ => continue,
            };
            let victim = SegmentVictim {
                segment: u32::try_from(index).expect("segment index must fit u32"),
                generation: segment.generation,
                used: segment.used,
                priority: segment.priority,
                sequence: segment.sequence,
            };
            if target.is_none_or(|current| (victim.sequence, victim.segment) < (current.sequence, current.segment)) {
                *target = Some(victim);
            }
        }
        candidates
    }

    pub fn priority_occupancy(&self, capacity_floor_segments: [u32; 3]) -> PriorityOccupancy {
        let state = mutex_lock(&self.state);
        let mut occupied_segments = [0u32; 3];
        let mut used_slots = [0u64; 3];
        for segment in &state.segments {
            if !matches!(
                segment.role,
                SegmentRole::Current | SegmentRole::Sealed | SegmentRole::ReclaimSource
            ) {
                continue;
            }
            let priority = segment.priority as usize;
            occupied_segments[priority] = occupied_segments[priority].saturating_add(1);
            used_slots[priority] = used_slots[priority].saturating_add(u64::from(segment.used));
        }
        PriorityOccupancy::new(
            self.layout.segment_count.saturating_sub(1),
            self.layout.slots_per_segment,
            self.layout.slot_size,
            occupied_segments,
            used_slots,
            capacity_floor_segments,
        )
    }

    pub fn seal_current(&self, victim: SegmentVictim) -> Result<SegmentVictim> {
        let mut state = mutex_lock(&self.state);
        let segment = state
            .segments
            .get(victim.segment as usize)
            .copied()
            .ok_or_else(|| invalid_state("current reclaim victim is out of range"))?;
        let priority = usize::from(victim.priority.to_byte());
        if segment.role != SegmentRole::Current
            || segment.generation != victim.generation
            || segment.used != victim.used
            || segment.priority != victim.priority
            || segment.sequence != victim.sequence
            || state.current[priority] != Some(victim.segment)
        {
            return Err(invalid_state("current reclaim victim changed before seal"));
        }
        state.segments[victim.segment as usize].role = SegmentRole::Sealed;
        state.current[priority] = None;
        Ok(victim)
    }

    pub fn begin_reclaim(&self, victim: SegmentVictim) -> Result<ReclaimTransaction> {
        let mut state = mutex_lock(&self.state);
        let source = state
            .segments
            .get(victim.segment as usize)
            .copied()
            .ok_or_else(|| invalid_state("reclaim source is out of range"))?;
        if source.role != SegmentRole::Sealed
            || source.generation != victim.generation
            || source.used != victim.used
            || source.priority != victim.priority
            || state.current[usize::from(victim.priority.to_byte())].is_some()
        {
            return Err(invalid_state("reclaim source is not a stable sealed segment"));
        }
        let target = state.reserve;
        let target_state = state.segments[target as usize];
        if target_state.role != SegmentRole::Reserve || target == victim.segment {
            return Err(invalid_state("reclaim target reserve is invalid"));
        }

        state.segments[victim.segment as usize].role = SegmentRole::ReclaimSource;
        let sequence = state.next_sequence;
        let target_state = &mut state.segments[target as usize];
        target_state.used = 0;
        target_state.sequence = sequence;
        target_state.priority = victim.priority;
        target_state.role = SegmentRole::ReclaimTarget;
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
            .segments
            .iter()
            .enumerate()
            .find(|(_, segment)| segment.role == SegmentRole::ReclaimSource)?;
        let (target, target_state) = state
            .segments
            .iter()
            .enumerate()
            .find(|(_, segment)| segment.role == SegmentRole::ReclaimTarget)?;
        Some(ReclaimTransaction {
            source: SegmentVictim {
                segment: u32::try_from(source).ok()?,
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
    ) -> Result<Option<SegmentAllocation>> {
        if slots == 0 || slots > self.layout.slots_per_segment {
            return Err(invalid_state(
                "reclaim allocation must fit completely within one segment",
            ));
        }
        let mut state = mutex_lock(&self.state);
        let target = &mut state.segments[transaction.target as usize];
        if target.role != SegmentRole::ReclaimTarget
            || target.generation != transaction.target_generation
            || target.priority != transaction.priority
        {
            return Err(invalid_state("reclaim target changed during compaction"));
        }
        if self.layout.slots_per_segment.saturating_sub(target.used) < slots {
            return Ok(None);
        }
        let slot = target.used;
        target.used += slots;
        let sequence = state.next_sequence;
        state.next_sequence = state
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| invalid_state("segment allocation sequence is exhausted"))?;
        Ok(Some(SegmentAllocation {
            physical_slot: self
                .layout
                .physical_slot(transaction.target, slot)
                .expect("reclaim target slot must be in the layout"),
            segment: transaction.target,
            slot,
            slots,
            segment_generation: transaction.target_generation,
            priority: transaction.priority,
            sequence,
        }))
    }

    pub fn finish_reclaim(&self, transaction: ReclaimTransaction) -> Result<()> {
        let mut state = mutex_lock(&self.state);
        let source = state.segments[transaction.source.segment as usize];
        let target = state.segments[transaction.target as usize];
        if source.role != SegmentRole::ReclaimSource
            || source.generation != transaction.source.generation
            || target.role != SegmentRole::ReclaimTarget
            || target.generation != transaction.target_generation
            || target.priority != transaction.priority
        {
            return Err(invalid_state("reclaim transaction changed before commit"));
        }

        let source = &mut state.segments[transaction.source.segment as usize];
        source.generation = source
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid_state("segment generation is exhausted"))?;
        source.used = 0;
        source.sequence = 0;
        source.priority = CachePriority::Low;
        source.role = SegmentRole::Reserve;
        state.reserve = transaction.source.segment;

        let target = &mut state.segments[transaction.target as usize];
        if target.used < self.layout.slots_per_segment {
            target.role = SegmentRole::Current;
            state.current[usize::from(transaction.priority.to_byte())] = Some(transaction.target);
        } else {
            target.role = SegmentRole::Sealed;
        }
        self.persist_state_locked(&mut state)
    }

    pub fn owners(&self, victim: SegmentVictim) -> Result<Vec<(u64, OwnerRecord)>> {
        let mut owners = Vec::new();
        for slot in 0..victim.used {
            let physical_slot = self
                .layout
                .physical_slot(victim.segment, slot)
                .expect("victim slot must be in the layout");
            let Some(owner) = self.read_owner(physical_slot)? else {
                continue;
            };
            if owner.segment_generation == victim.generation {
                owners.push((physical_slot, owner));
            }
        }
        Ok(owners)
    }

    pub fn release(&self, victim: SegmentVictim) -> Result<()> {
        let mut state = mutex_lock(&self.state);
        let segment = &mut state.segments[victim.segment as usize];
        if segment.role != SegmentRole::Sealed || segment.generation != victim.generation || segment.used != victim.used
        {
            return Err(invalid_state("segment victim changed during reclamation"));
        }
        segment.generation = segment
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid_state("segment generation is exhausted"))?;
        segment.used = 0;
        segment.sequence = 0;
        segment.priority = CachePriority::Low;
        segment.role = SegmentRole::Free;
        self.persist_state_locked(&mut state)
    }

    pub fn sync_payload(&self) -> Result<()> {
        self.io
            .write(|| self.data.sync_data())
            .map_err(|error| Error::io("sync segment data", error))?;
        self.io
            .write(|| self.owners.sync_data())
            .map_err(|error| Error::io("sync segment owners", error))
    }

    pub fn checkpoint_state(&self) -> Result<()> {
        let checkpoint = self.prepare_checkpoint_state()?;
        self.persist_checkpoint_state(&checkpoint)
    }

    pub fn prepare_checkpoint_state(&self) -> Result<AllocatorCheckpoint> {
        let state = mutex_lock(&self.state);
        let next_generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid_state("allocator state generation is exhausted"))?;
        let next_page = 1 - state.active_page;
        let mut snapshot = state.clone();
        snapshot.generation = next_generation;
        snapshot.active_page = next_page;
        Ok(AllocatorCheckpoint {
            base_generation: state.generation,
            base_page: state.active_page,
            next_generation,
            next_page,
            encoded: snapshot.encode(self.layout)?,
        })
    }

    pub fn persist_checkpoint_state(&self, checkpoint: &AllocatorCheckpoint) -> Result<()> {
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
        if state.generation != checkpoint.base_generation || state.active_page != checkpoint.base_page {
            return Err(invalid_state(
                "allocator checkpoint cursor changed while persistence was in flight",
            ));
        }
        state.generation = checkpoint.next_generation;
        state.active_page = checkpoint.next_page;
        Ok(())
    }

    #[cfg(test)]
    fn state_snapshot(&self) -> AllocatorState {
        mutex_lock(&self.state).clone()
    }

    fn recover_current_tails(&self) -> Result<()> {
        let mut state = mutex_lock(&self.state);
        let mut next_sequence = state.next_sequence;
        let mut tails = state
            .current
            .iter()
            .enumerate()
            .filter_map(|(priority, segment)| {
                segment.map(|segment| {
                    (
                        segment,
                        CachePriority::from_byte(priority as u8).expect("current priority index must be valid"),
                    )
                })
            })
            .collect::<Vec<_>>();
        if let Some((target, segment)) = state
            .segments
            .iter()
            .enumerate()
            .find(|(_, segment)| segment.role == SegmentRole::ReclaimTarget)
        {
            tails.push((
                u32::try_from(target).expect("segment index must fit u32"),
                segment.priority,
            ));
        }
        for (segment, priority) in tails {
            let segment_index = segment as usize;
            let generation = state.segments[segment_index].generation;
            let start = state.segments[segment_index].used;
            let mut recovered_used = start;
            for slot in start..self.layout.slots_per_segment {
                let physical_slot = self
                    .layout
                    .physical_slot(segment, slot)
                    .expect("current segment slot must be in the layout");
                let Some(owner) = self.read_owner(physical_slot)? else {
                    break;
                };
                if owner.segment_generation != generation
                    || owner.priority != priority
                    || owner.stored_len == 0
                    || owner.stored_len as usize > self.layout.segment_size
                {
                    break;
                }
                recovered_used = slot + 1;
                next_sequence = next_sequence.max(owner.sequence.saturating_add(1));
            }
            state.segments[segment_index].used = recovered_used;
        }
        state.next_sequence = next_sequence;
        Ok(())
    }

    fn read_owner(&self, physical_slot: u64) -> Result<Option<OwnerRecord>> {
        let offset = physical_slot
            .checked_mul(OWNER_RECORD_SIZE as u64)
            .ok_or_else(|| invalid_state("segment owner offset overflows u64"))?;
        let mut input = [0; OWNER_RECORD_SIZE];
        read_exact_at(&self.owners, &mut input, offset).map_err(|error| Error::io("read segment owner", error))?;
        Ok(OwnerRecord::decode(&input))
    }

    fn write_data_runs(&self, writes: &[SegmentWrite<'_>], runs: &[SegmentWriteRun]) -> Result<()> {
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
                            .ok_or_else(|| invalid_state("segment data batch size overflows usize"))?;
                        let mut output = AlignedBuffer::new(len);
                        for piece in &run.pieces {
                            let write = writes[piece.index];
                            let input_start = piece
                                .write_slot
                                .checked_mul(self.layout.slot_size)
                                .ok_or_else(|| invalid_state("segment write input overflows"))?;
                            let input_end = piece
                                .write_slot
                                .saturating_add(piece.slots)
                                .checked_mul(self.layout.slot_size)
                                .map(|end| end.min(write.stored_len()))
                                .ok_or_else(|| invalid_state("segment write input overflows"))?;
                            let output_start = piece
                                .run_slot
                                .checked_mul(self.layout.slot_size)
                                .ok_or_else(|| invalid_state("segment write output overflows"))?;
                            let output_end = output_start
                                .checked_add(input_end.saturating_sub(input_start))
                                .ok_or_else(|| invalid_state("segment write output overflows"))?;
                            if !copy_blob_range(
                                write.key,
                                write.value,
                                input_start,
                                &mut output.as_mut_slice()[output_start..output_end],
                            ) {
                                return Err(invalid_state("segment stored blob slice is invalid"));
                            }
                        }
                        let offset = run
                            .first_slot
                            .checked_mul(self.layout.slot_size as u64)
                            .ok_or_else(|| invalid_state("segment data offset overflows u64"))?;
                        self.io
                            .write(|| write_all_at(&self.data, output.as_slice(), offset))
                            .map_err(|error| Error::io("write segment data batch", error))?;
                        self.writes.data_runs.fetch_add(1, Ordering::Relaxed);
                        self.writes.data_bytes.fetch_add(len as u64, Ordering::Relaxed);
                    }
                }));
            }
            for worker in workers {
                worker.join().expect("segment data writer must not panic")?;
            }
            Ok(())
        })
    }

    fn write_owner_runs(&self, writes: &[SegmentWrite<'_>], runs: &[SegmentWriteRun]) -> Result<()> {
        for run in runs {
            let mut output = vec![0; run.slots * OWNER_RECORD_SIZE];
            for piece in &run.pieces {
                let write = writes[piece.index];
                let owner = OwnerRecord {
                    key_digest: write.key_digest,
                    segment_generation: write.allocation.segment_generation,
                    stored_len: u32::try_from(write.stored_len()).expect("validated stored blob length must fit u32"),
                    value_len: u32::try_from(write.value.len()).expect("validated value length must fit u32"),
                    checksum: write.checksum,
                    priority: write.allocation.priority,
                    sequence: write.allocation.sequence,
                }
                .encode();
                for slot in piece.run_slot..piece.run_slot + piece.slots {
                    let start = slot * OWNER_RECORD_SIZE;
                    output[start..start + OWNER_RECORD_SIZE].copy_from_slice(&owner);
                }
            }
            let offset = run
                .first_slot
                .checked_mul(OWNER_RECORD_SIZE as u64)
                .ok_or_else(|| invalid_state("segment owner offset overflows u64"))?;
            self.io
                .write(|| write_all_at(&self.owners, &output, offset))
                .map_err(|error| Error::io("write segment owner batch", error))?;
            self.writes.owner_runs.fetch_add(1, Ordering::Relaxed);
            self.writes
                .owner_bytes
                .fetch_add(output.len() as u64, Ordering::Relaxed);
        }
        Ok(())
    }

    fn persist_state_locked(&self, state: &mut AllocatorState) -> Result<()> {
        let generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid_state("allocator state generation is exhausted"))?;
        let page = 1 - state.active_page;
        let mut next = state.clone();
        next.generation = generation;
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
struct SegmentWriteRun {
    first_slot: u64,
    slots: usize,
    pieces: Vec<SegmentWritePiece>,
}

#[derive(Debug)]
struct SegmentWritePiece {
    index: usize,
    run_slot: usize,
    write_slot: usize,
    slots: usize,
}

fn segment_write_runs(order: &[usize], writes: &[SegmentWrite<'_>], maximum_slots: usize) -> Vec<SegmentWriteRun> {
    debug_assert!(maximum_slots > 0);
    let mut runs: Vec<SegmentWriteRun> = Vec::new();
    for index in order.iter().copied() {
        let write = writes[index];
        let mut write_slot = 0usize;
        let write_slots = write.allocation.slots as usize;
        while write_slot < write_slots {
            let physical_slot = write
                .allocation
                .physical_slot
                .checked_add(write_slot as u64)
                .expect("validated segment write must fit the data file");
            let append = runs.last().is_some_and(|run| {
                run.first_slot.checked_add(run.slots as u64) == Some(physical_slot) && run.slots < maximum_slots
            });
            if !append {
                runs.push(SegmentWriteRun {
                    first_slot: physical_slot,
                    slots: 0,
                    pieces: Vec::new(),
                });
            }
            let run = runs.last_mut().expect("segment write run must exist");
            let slots = (write_slots - write_slot).min(maximum_slots - run.slots);
            run.pieces.push(SegmentWritePiece {
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

fn state_file_size(layout: SegmentLayout) -> Result<u64> {
    layout
        .state_copy_size
        .checked_mul(2)
        .and_then(|size| u64::try_from(size).ok())
        .ok_or_else(|| invalid_state("segment state file size overflows u64"))
}

fn layout_allocated_size(layout: SegmentLayout) -> Result<u64> {
    layout
        .total_file_size
        .checked_sub(layout.index_capacity_bytes)
        .ok_or_else(|| invalid_state("segment file allocation underflows total layout size"))
}

fn invalid_state(message: &str) -> Error {
    Error::InvalidSuperblock(format!("segment state: {message}"))
}

fn mutex_lock<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::{
        format::{PAGE_SIZE, blob_checksum},
        segment::config::{SegmentEngineConfig, SegmentEngineOptions},
    };

    fn create_store(root: &Path) -> SegmentStore {
        let options = SegmentEngineOptions::default().with_segment_size(PAGE_SIZE * 8);
        let layout = SegmentLayout::create(
            SegmentEngineConfig::new(4 * 1024 * 1024)
                .with_slot_size(PAGE_SIZE)
                .with_options(options),
        )
        .unwrap();
        SegmentStore::create(root, layout, false, 2, Duration::ZERO, PAGE_SIZE * 4, PAGE_SIZE * 4).unwrap()
    }

    fn key(index: u64) -> BlobKey {
        let mut bytes = [index as u8; 24];
        bytes[16..].copy_from_slice(&index.to_le_bytes());
        BlobKey::new(bytes).unwrap()
    }

    fn allocated(result: SegmentAllocationResult) -> SegmentAllocation {
        match result {
            SegmentAllocationResult::Allocated(allocation) => allocation,
            SegmentAllocationResult::ReclaimRequired => panic!("test allocation needs reclaim"),
        }
    }

    #[test]
    fn batch_write_read_checkpoint_and_reopen() {
        let dir = tempdir().unwrap();
        let store = create_store(dir.path());
        let values = (0..12).map(|index| vec![index as u8; 100 + index]).collect::<Vec<_>>();
        let allocations = (0..values.len())
            .map(|_| allocated(store.allocate(CachePriority::Normal, 1).unwrap()))
            .collect::<Vec<_>>();
        let keys = (0..values.len()).map(|index| key(index as u64)).collect::<Vec<_>>();
        let writes = values
            .iter()
            .enumerate()
            .map(|(index, value)| SegmentWrite {
                allocation: allocations[index],
                key: &keys[index],
                key_digest: KeyDigest::for_key(&keys[index]),
                value,
                checksum: blob_checksum(&keys[index], value),
            })
            .collect::<Vec<_>>();
        let result = store.write_batch(&writes).unwrap();
        assert_eq!(result.data_runs, 3);
        assert_eq!(result.owner_runs, 1);
        for (index, location) in result.locations.iter().enumerate() {
            assert_eq!(
                store.get_blob(&keys[index], *location).unwrap(),
                SegmentBlobReadResult {
                    value: Some(values[index].clone()),
                    data_slots: 1,
                    data_runs: 1,
                    data_bytes: stored_blob_len(&keys[index], &values[index]).unwrap(),
                }
            );
        }
        store.sync_payload().unwrap();
        store.checkpoint_state().unwrap();
        let layout = store.layout();
        drop(store);

        let reopened = SegmentStore::open(dir.path(), false, 2, Duration::ZERO, PAGE_SIZE * 4, PAGE_SIZE * 4).unwrap();
        assert_eq!(reopened.layout(), layout);
        for (index, location) in result.locations.iter().enumerate() {
            assert_eq!(
                reopened.get_blob(&keys[index], *location).unwrap(),
                SegmentBlobReadResult {
                    value: Some(values[index].clone()),
                    data_slots: 1,
                    data_runs: 1,
                    data_bytes: stored_blob_len(&keys[index], &values[index]).unwrap(),
                }
            );
        }
    }

    #[test]
    fn point_read_rechecks_generation_after_payload_io() {
        let dir = tempdir().unwrap();
        let store = create_store(dir.path());
        let old_value = vec![0x11; 512];
        let mut replacement = vec![0x22; 512];
        replacement[508..].copy_from_slice(&[0xce, 0xe0, 0x15, 0xb2]);
        let key = key(1);
        let key_digest = KeyDigest::for_key(&key);
        let checksum = blob_checksum(&key, &old_value);
        assert_ne!(old_value, replacement);
        assert_eq!(blob_checksum(&key, &replacement), checksum);

        let allocation = allocated(store.allocate(CachePriority::Normal, 1).unwrap());
        let location = store
            .write_batch(&[SegmentWrite {
                allocation,
                key: &key,
                key_digest,
                value: &old_value,
                checksum,
            }])
            .unwrap()
            .locations[0];
        let (victim, is_current) = store.reclaim_candidates().oldest(CachePriority::Normal).unwrap();
        assert!(is_current);
        let victim = store.seal_current(victim).unwrap();

        let value = store
            .read_stored_blob_after_validation(location, || {
                store.release(victim)?;
                let replacement_allocation = allocated(store.allocate(CachePriority::Normal, 1)?);
                assert_eq!(replacement_allocation.physical_slot, allocation.physical_slot);
                store.write_batch(&[SegmentWrite {
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
    }

    #[test]
    fn reopen_recovers_uncheckpointed_current_tail() {
        let dir = tempdir().unwrap();
        let store = create_store(dir.path());
        let allocation = allocated(store.allocate(CachePriority::High, 1).unwrap());
        store.checkpoint_state().unwrap();
        let value = vec![9; 512];
        let key = key(1);
        store
            .write_batch(&[SegmentWrite {
                allocation,
                key: &key,
                key_digest: KeyDigest::for_key(&key),
                value: &value,
                checksum: blob_checksum(&key, &value),
            }])
            .unwrap();
        store.sync_payload().unwrap();
        drop(store);

        let reopened = SegmentStore::open(dir.path(), false, 1, Duration::ZERO, PAGE_SIZE * 4, PAGE_SIZE * 4).unwrap();
        let next = allocated(reopened.allocate(CachePriority::High, 1).unwrap());
        assert_eq!(next.segment, allocation.segment);
        assert_eq!(next.slot, allocation.slot + 1);
    }

    #[test]
    fn falls_back_to_the_older_valid_state_copy() {
        let dir = tempdir().unwrap();
        let store = create_store(dir.path());
        store.checkpoint_state().unwrap();
        let state = store.state_snapshot();
        let layout = store.layout();
        drop(store);

        let state_file = open_cache_file(&dir.path().join(STATE_FILE), false, false).unwrap();
        let offset = u64::from(state.active_page) * layout.state_copy_size as u64;
        write_all_at(&state_file, &[0xff], offset).unwrap();
        state_file.sync_data().unwrap();

        let reopened = SegmentStore::open(dir.path(), false, 1, Duration::ZERO, PAGE_SIZE * 4, PAGE_SIZE * 4).unwrap();
        assert!(reopened.state_snapshot().generation < state.generation);
    }
}
