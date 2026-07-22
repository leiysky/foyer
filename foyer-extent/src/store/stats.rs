use crate::model::CachePriority;

/// Immutable capacity and cardinality decisions for one Extent layout.
///
/// `planned_file_bytes` is the hard space planned inside the configured cache capacity. The
/// EntryIndex target is reported separately because it is soft and may be overcommitted without
/// rejecting a cache write.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ExtentLayoutStats {
    pub configured_capacity_bytes: u64,
    pub planned_file_bytes: u64,
    pub data_file_bytes: u64,
    pub usable_payload_bytes: u64,
    pub index_soft_capacity_bytes: u64,
    pub extent_size_bytes: u64,
    pub physical_extents: u64,
    pub usable_extents: u64,
    pub entry_charge_bytes: u64,
    pub planned_live_entries: u64,
    pub maximum_live_entries: u64,
}

/// A point-in-time view of physical extent ownership by cache priority.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ExtentOccupancy {
    usable_extents: u32,
    entries_per_extent: u32,
    occupied_extents: [u32; 3],
    used_entries: [u64; 3],
    used_bytes: [u64; 3],
    capacity_floor_extents: [u32; 3],
}

impl ExtentOccupancy {
    pub(crate) const fn new(
        usable_extents: u32,
        entries_per_extent: u32,
        occupied_extents: [u32; 3],
        used_entries: [u64; 3],
        used_bytes: [u64; 3],
        capacity_floor_extents: [u32; 3],
    ) -> Self {
        Self {
            usable_extents,
            entries_per_extent,
            occupied_extents,
            used_entries,
            used_bytes,
            capacity_floor_extents,
        }
    }

    pub const fn usable_extents(self) -> u32 {
        self.usable_extents
    }

    pub const fn entries_per_extent(self) -> u32 {
        self.entries_per_extent
    }

    pub const fn occupied_extents(self, priority: CachePriority) -> u32 {
        self.occupied_extents[priority as usize]
    }

    pub const fn used_entries(self, priority: CachePriority) -> u64 {
        self.used_entries[priority as usize]
    }

    pub const fn used_bytes(self, priority: CachePriority) -> u64 {
        self.used_bytes[priority as usize]
    }

    pub const fn capacity_floor_extents(self, priority: CachePriority) -> u32 {
        self.capacity_floor_extents[priority as usize]
    }

    pub const fn borrowed_extents(self, priority: CachePriority) -> u32 {
        self.occupied_extents(priority)
            .saturating_sub(self.capacity_floor_extents(priority))
    }
}

/// Cumulative physical writes issued by the store after creation or reopen.
///
/// The counters describe userspace write calls and bytes, not filesystem writeback accounting.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalWriteStats {
    pub data_runs: u64,
    pub data_bytes: u64,
    pub data_syncs: u64,
    pub index_runs: u64,
    pub index_bytes: u64,
    pub index_syncs: u64,
    pub index_wal_runs: u64,
    pub index_wal_bytes: u64,
    pub index_wal_syncs: u64,
    pub index_sst_runs: u64,
    pub index_sst_bytes: u64,
    pub index_sst_syncs: u64,
    pub index_manifest_runs: u64,
    pub index_manifest_bytes: u64,
    pub index_manifest_syncs: u64,
    pub index_flushes: u64,
    pub index_compactions: u64,
    pub index_compaction_input_bytes: u64,
    pub index_compaction_output_bytes: u64,
    pub allocator_runs: u64,
    pub allocator_bytes: u64,
    pub allocator_syncs: u64,
}

impl PhysicalWriteStats {
    pub const fn total_runs(self) -> u64 {
        self.data_runs
            .saturating_add(self.index_runs)
            .saturating_add(self.allocator_runs)
    }

    pub const fn total_bytes(self) -> u64 {
        self.data_bytes
            .saturating_add(self.index_bytes)
            .saturating_add(self.allocator_bytes)
    }

    pub const fn total_syncs(self) -> u64 {
        self.data_syncs
            .saturating_add(self.index_syncs)
            .saturating_add(self.allocator_syncs)
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.data_runs = self.data_runs.saturating_add(other.data_runs);
        self.data_bytes = self.data_bytes.saturating_add(other.data_bytes);
        self.data_syncs = self.data_syncs.saturating_add(other.data_syncs);
        self.index_runs = self.index_runs.saturating_add(other.index_runs);
        self.index_bytes = self.index_bytes.saturating_add(other.index_bytes);
        self.index_syncs = self.index_syncs.saturating_add(other.index_syncs);
        self.index_wal_runs = self.index_wal_runs.saturating_add(other.index_wal_runs);
        self.index_wal_bytes = self.index_wal_bytes.saturating_add(other.index_wal_bytes);
        self.index_wal_syncs = self.index_wal_syncs.saturating_add(other.index_wal_syncs);
        self.index_sst_runs = self.index_sst_runs.saturating_add(other.index_sst_runs);
        self.index_sst_bytes = self.index_sst_bytes.saturating_add(other.index_sst_bytes);
        self.index_sst_syncs = self.index_sst_syncs.saturating_add(other.index_sst_syncs);
        self.index_manifest_runs = self.index_manifest_runs.saturating_add(other.index_manifest_runs);
        self.index_manifest_bytes = self.index_manifest_bytes.saturating_add(other.index_manifest_bytes);
        self.index_manifest_syncs = self.index_manifest_syncs.saturating_add(other.index_manifest_syncs);
        self.index_flushes = self.index_flushes.saturating_add(other.index_flushes);
        self.index_compactions = self.index_compactions.saturating_add(other.index_compactions);
        self.index_compaction_input_bytes = self
            .index_compaction_input_bytes
            .saturating_add(other.index_compaction_input_bytes);
        self.index_compaction_output_bytes = self
            .index_compaction_output_bytes
            .saturating_add(other.index_compaction_output_bytes);
        self.allocator_runs = self.allocator_runs.saturating_add(other.allocator_runs);
        self.allocator_bytes = self.allocator_bytes.saturating_add(other.allocator_bytes);
        self.allocator_syncs = self.allocator_syncs.saturating_add(other.allocator_syncs);
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimStats {
    reclaimed_extents: [usize; 3],
    invalidated_entries: [usize; 3],
    invalidated_bytes: [usize; 3],
    checkpoint_wait_nanos: u64,
    generation_invalidation_nanos: u64,
    total_nanos: u64,
}

impl ReclaimStats {
    pub const fn reclaimed_extents(self, priority: CachePriority) -> usize {
        self.reclaimed_extents[priority as usize]
    }

    pub const fn invalidated_entries(self, priority: CachePriority) -> usize {
        self.invalidated_entries[priority as usize]
    }

    pub const fn invalidated_bytes(self, priority: CachePriority) -> usize {
        self.invalidated_bytes[priority as usize]
    }

    pub const fn total_reclaimed_extents(self) -> usize {
        sum_priority_counts(self.reclaimed_extents)
    }

    pub const fn total_invalidated_entries(self) -> usize {
        sum_priority_counts(self.invalidated_entries)
    }

    pub const fn total_invalidated_bytes(self) -> usize {
        sum_priority_counts(self.invalidated_bytes)
    }

    pub const fn checkpoint_wait_nanos(self) -> u64 {
        self.checkpoint_wait_nanos
    }

    pub const fn generation_invalidation_nanos(self) -> u64 {
        self.generation_invalidation_nanos
    }

    pub const fn total_nanos(self) -> u64 {
        self.total_nanos
    }

    pub(crate) fn record(&mut self, priority: CachePriority, invalidated_entries: usize, invalidated_bytes: usize) {
        let priority = priority as usize;
        self.reclaimed_extents[priority] = self.reclaimed_extents[priority].saturating_add(1);
        self.invalidated_entries[priority] = self.invalidated_entries[priority].saturating_add(invalidated_entries);
        self.invalidated_bytes[priority] = self.invalidated_bytes[priority].saturating_add(invalidated_bytes);
    }

    pub(crate) fn record_work(&mut self, checkpoint_wait_nanos: u64, invalidation_nanos: u64, total_nanos: u64) {
        self.checkpoint_wait_nanos = self.checkpoint_wait_nanos.saturating_add(checkpoint_wait_nanos);
        self.generation_invalidation_nanos = self.generation_invalidation_nanos.saturating_add(invalidation_nanos);
        self.total_nanos = self.total_nanos.saturating_add(total_nanos);
    }

    pub(crate) fn merge(&mut self, other: Self) {
        for priority in 0..3 {
            self.reclaimed_extents[priority] =
                self.reclaimed_extents[priority].saturating_add(other.reclaimed_extents[priority]);
            self.invalidated_entries[priority] =
                self.invalidated_entries[priority].saturating_add(other.invalidated_entries[priority]);
            self.invalidated_bytes[priority] =
                self.invalidated_bytes[priority].saturating_add(other.invalidated_bytes[priority]);
        }
        self.checkpoint_wait_nanos = self.checkpoint_wait_nanos.saturating_add(other.checkpoint_wait_nanos);
        self.generation_invalidation_nanos = self
            .generation_invalidation_nanos
            .saturating_add(other.generation_invalidation_nanos);
        self.total_nanos = self.total_nanos.saturating_add(other.total_nanos);
    }
}

const fn sum_priority_counts(values: [usize; 3]) -> usize {
    values[0].saturating_add(values[1]).saturating_add(values[2])
}
