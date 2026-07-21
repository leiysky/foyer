use crate::model::CachePriority;

/// Cumulative buffered reads issued against the sparse Entry directory.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DirectoryReadStats {
    pub runs: u64,
    pub bytes: u64,
}

/// Immutable capacity and cardinality decisions for one Extent layout.
///
/// `planned_file_bytes` is the hard space planned inside the configured cache capacity. The
/// EntryIndex target and directory address space are deliberately reported separately because
/// both may overcommit their planning targets without rejecting a cache write.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ExtentLayoutStats {
    pub configured_capacity_bytes: u64,
    pub planned_file_bytes: u64,
    pub data_file_bytes: u64,
    pub usable_payload_bytes: u64,
    pub index_soft_capacity_bytes: u64,
    pub directory_planned_bytes: u64,
    pub directory_logical_bytes: u64,
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
    pub entry_directory_runs: u64,
    pub entry_directory_bytes: u64,
    pub entry_directory_syncs: u64,
    pub index_runs: u64,
    pub index_bytes: u64,
    pub index_syncs: u64,
    pub allocator_runs: u64,
    pub allocator_bytes: u64,
    pub allocator_syncs: u64,
}

impl PhysicalWriteStats {
    pub const fn total_runs(self) -> u64 {
        self.data_runs
            .saturating_add(self.entry_directory_runs)
            .saturating_add(self.index_runs)
            .saturating_add(self.allocator_runs)
    }

    pub const fn total_bytes(self) -> u64 {
        self.data_bytes
            .saturating_add(self.entry_directory_bytes)
            .saturating_add(self.index_bytes)
            .saturating_add(self.allocator_bytes)
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.data_runs = self.data_runs.saturating_add(other.data_runs);
        self.data_bytes = self.data_bytes.saturating_add(other.data_bytes);
        self.data_syncs = self.data_syncs.saturating_add(other.data_syncs);
        self.entry_directory_runs = self.entry_directory_runs.saturating_add(other.entry_directory_runs);
        self.entry_directory_bytes = self.entry_directory_bytes.saturating_add(other.entry_directory_bytes);
        self.entry_directory_syncs = self.entry_directory_syncs.saturating_add(other.entry_directory_syncs);
        self.index_runs = self.index_runs.saturating_add(other.index_runs);
        self.index_bytes = self.index_bytes.saturating_add(other.index_bytes);
        self.index_syncs = self.index_syncs.saturating_add(other.index_syncs);
        self.allocator_runs = self.allocator_runs.saturating_add(other.allocator_runs);
        self.allocator_bytes = self.allocator_bytes.saturating_add(other.allocator_bytes);
        self.allocator_syncs = self.allocator_syncs.saturating_add(other.allocator_syncs);
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimStats {
    reclaimed_extents: [usize; 3],
    evicted_entries: [usize; 3],
    promoted_entries: [usize; 3],
    evicted_bytes: [usize; 3],
    promoted_bytes: [usize; 3],
    scanned_entries: u64,
    directory_read_runs: u64,
    directory_read_bytes: u64,
    index_lookups: u64,
    preparation_nanos: u64,
    directory_scan_nanos: u64,
    index_lookup_nanos: u64,
    transaction_nanos: u64,
    promotion_nanos: u64,
    publication_nanos: u64,
    total_nanos: u64,
}

impl ReclaimStats {
    pub const fn reclaimed_extents(self, priority: CachePriority) -> usize {
        self.reclaimed_extents[priority as usize]
    }

    pub const fn evicted_entries(self, priority: CachePriority) -> usize {
        self.evicted_entries[priority as usize]
    }

    pub const fn promoted_entries(self, priority: CachePriority) -> usize {
        self.promoted_entries[priority as usize]
    }

    pub const fn evicted_bytes(self, priority: CachePriority) -> usize {
        self.evicted_bytes[priority as usize]
    }

    pub const fn promoted_bytes(self, priority: CachePriority) -> usize {
        self.promoted_bytes[priority as usize]
    }

    pub const fn total_reclaimed_extents(self) -> usize {
        sum_priority_counts(self.reclaimed_extents)
    }

    pub const fn total_evicted_entries(self) -> usize {
        sum_priority_counts(self.evicted_entries)
    }

    pub const fn total_promoted_entries(self) -> usize {
        sum_priority_counts(self.promoted_entries)
    }

    pub const fn total_evicted_bytes(self) -> usize {
        sum_priority_counts(self.evicted_bytes)
    }

    pub const fn total_promoted_bytes(self) -> usize {
        sum_priority_counts(self.promoted_bytes)
    }

    pub const fn scanned_entries(self) -> u64 {
        self.scanned_entries
    }

    pub const fn directory_read_runs(self) -> u64 {
        self.directory_read_runs
    }

    pub const fn directory_read_bytes(self) -> u64 {
        self.directory_read_bytes
    }

    pub const fn index_lookups(self) -> u64 {
        self.index_lookups
    }

    pub const fn directory_scan_nanos(self) -> u64 {
        self.directory_scan_nanos
    }

    pub const fn preparation_nanos(self) -> u64 {
        self.preparation_nanos
    }

    pub const fn index_lookup_nanos(self) -> u64 {
        self.index_lookup_nanos
    }

    pub const fn transaction_nanos(self) -> u64 {
        self.transaction_nanos
    }

    pub const fn promotion_nanos(self) -> u64 {
        self.promotion_nanos
    }

    pub const fn publication_nanos(self) -> u64 {
        self.publication_nanos
    }

    pub const fn total_nanos(self) -> u64 {
        self.total_nanos
    }

    pub(crate) fn record(
        &mut self,
        priority: CachePriority,
        evicted_entries: usize,
        promoted_entries: usize,
        evicted_bytes: usize,
        promoted_bytes: usize,
    ) {
        let priority = priority as usize;
        self.reclaimed_extents[priority] = self.reclaimed_extents[priority].saturating_add(1);
        self.evicted_entries[priority] = self.evicted_entries[priority].saturating_add(evicted_entries);
        self.promoted_entries[priority] = self.promoted_entries[priority].saturating_add(promoted_entries);
        self.evicted_bytes[priority] = self.evicted_bytes[priority].saturating_add(evicted_bytes);
        self.promoted_bytes[priority] = self.promoted_bytes[priority].saturating_add(promoted_bytes);
    }

    pub(crate) fn record_work(
        &mut self,
        scanned_entries: u64,
        directory_read_runs: u64,
        directory_read_bytes: u64,
        index_lookups: u64,
        preparation_nanos: u64,
        directory_scan_nanos: u64,
        index_lookup_nanos: u64,
        transaction_nanos: u64,
        promotion_nanos: u64,
        publication_nanos: u64,
        total_nanos: u64,
    ) {
        self.scanned_entries = self.scanned_entries.saturating_add(scanned_entries);
        self.directory_read_runs = self.directory_read_runs.saturating_add(directory_read_runs);
        self.directory_read_bytes = self.directory_read_bytes.saturating_add(directory_read_bytes);
        self.index_lookups = self.index_lookups.saturating_add(index_lookups);
        self.preparation_nanos = self.preparation_nanos.saturating_add(preparation_nanos);
        self.directory_scan_nanos = self.directory_scan_nanos.saturating_add(directory_scan_nanos);
        self.index_lookup_nanos = self.index_lookup_nanos.saturating_add(index_lookup_nanos);
        self.transaction_nanos = self.transaction_nanos.saturating_add(transaction_nanos);
        self.promotion_nanos = self.promotion_nanos.saturating_add(promotion_nanos);
        self.publication_nanos = self.publication_nanos.saturating_add(publication_nanos);
        self.total_nanos = self.total_nanos.saturating_add(total_nanos);
    }

    pub(crate) fn merge(&mut self, other: Self) {
        for priority in 0..3 {
            self.reclaimed_extents[priority] =
                self.reclaimed_extents[priority].saturating_add(other.reclaimed_extents[priority]);
            self.evicted_entries[priority] =
                self.evicted_entries[priority].saturating_add(other.evicted_entries[priority]);
            self.promoted_entries[priority] =
                self.promoted_entries[priority].saturating_add(other.promoted_entries[priority]);
            self.evicted_bytes[priority] = self.evicted_bytes[priority].saturating_add(other.evicted_bytes[priority]);
            self.promoted_bytes[priority] =
                self.promoted_bytes[priority].saturating_add(other.promoted_bytes[priority]);
        }
        self.scanned_entries = self.scanned_entries.saturating_add(other.scanned_entries);
        self.directory_read_runs = self.directory_read_runs.saturating_add(other.directory_read_runs);
        self.directory_read_bytes = self.directory_read_bytes.saturating_add(other.directory_read_bytes);
        self.index_lookups = self.index_lookups.saturating_add(other.index_lookups);
        self.preparation_nanos = self.preparation_nanos.saturating_add(other.preparation_nanos);
        self.directory_scan_nanos = self.directory_scan_nanos.saturating_add(other.directory_scan_nanos);
        self.index_lookup_nanos = self.index_lookup_nanos.saturating_add(other.index_lookup_nanos);
        self.transaction_nanos = self.transaction_nanos.saturating_add(other.transaction_nanos);
        self.promotion_nanos = self.promotion_nanos.saturating_add(other.promotion_nanos);
        self.publication_nanos = self.publication_nanos.saturating_add(other.publication_nanos);
        self.total_nanos = self.total_nanos.saturating_add(other.total_nanos);
    }
}

const fn sum_priority_counts(values: [usize; 3]) -> usize {
    values[0].saturating_add(values[1]).saturating_add(values[2])
}
