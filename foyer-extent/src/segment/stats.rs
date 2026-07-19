use crate::model::CachePriority;

/// A point-in-time view of physical segment ownership by cache priority.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PriorityOccupancy {
    usable_segments: u32,
    slots_per_segment: u32,
    slot_size: usize,
    occupied_segments: [u32; 3],
    used_slots: [u64; 3],
    capacity_floor_segments: [u32; 3],
}

impl PriorityOccupancy {
    pub(crate) const fn new(
        usable_segments: u32,
        slots_per_segment: u32,
        slot_size: usize,
        occupied_segments: [u32; 3],
        used_slots: [u64; 3],
        capacity_floor_segments: [u32; 3],
    ) -> Self {
        Self {
            usable_segments,
            slots_per_segment,
            slot_size,
            occupied_segments,
            used_slots,
            capacity_floor_segments,
        }
    }

    pub const fn usable_segments(self) -> u32 {
        self.usable_segments
    }

    pub const fn slots_per_segment(self) -> u32 {
        self.slots_per_segment
    }

    pub const fn occupied_segments(self, priority: CachePriority) -> u32 {
        self.occupied_segments[priority as usize]
    }

    pub const fn used_slots(self, priority: CachePriority) -> u64 {
        self.used_slots[priority as usize]
    }

    pub const fn used_bytes(self, priority: CachePriority) -> u64 {
        self.used_slots(priority).saturating_mul(self.slot_size as u64)
    }

    pub const fn capacity_floor_segments(self, priority: CachePriority) -> u32 {
        self.capacity_floor_segments[priority as usize]
    }

    pub const fn borrowed_segments(self, priority: CachePriority) -> u32 {
        self.occupied_segments(priority)
            .saturating_sub(self.capacity_floor_segments(priority))
    }
}

/// Cumulative physical writes issued by the engine after creation or reopen.
///
/// The counters describe userspace write calls and bytes, not filesystem writeback accounting.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalWriteStats {
    pub data_runs: u64,
    pub data_bytes: u64,
    pub owner_runs: u64,
    pub owner_bytes: u64,
    pub index_runs: u64,
    pub index_bytes: u64,
    pub allocator_runs: u64,
    pub allocator_bytes: u64,
}

impl PhysicalWriteStats {
    pub const fn total_runs(self) -> u64 {
        self.data_runs
            .saturating_add(self.owner_runs)
            .saturating_add(self.index_runs)
            .saturating_add(self.allocator_runs)
    }

    pub const fn total_bytes(self) -> u64 {
        self.data_bytes
            .saturating_add(self.owner_bytes)
            .saturating_add(self.index_bytes)
            .saturating_add(self.allocator_bytes)
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.data_runs = self.data_runs.saturating_add(other.data_runs);
        self.data_bytes = self.data_bytes.saturating_add(other.data_bytes);
        self.owner_runs = self.owner_runs.saturating_add(other.owner_runs);
        self.owner_bytes = self.owner_bytes.saturating_add(other.owner_bytes);
        self.index_runs = self.index_runs.saturating_add(other.index_runs);
        self.index_bytes = self.index_bytes.saturating_add(other.index_bytes);
        self.allocator_runs = self.allocator_runs.saturating_add(other.allocator_runs);
        self.allocator_bytes = self.allocator_bytes.saturating_add(other.allocator_bytes);
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimStats {
    reclaimed_segments: [usize; 3],
    evicted_entries: [usize; 3],
    promoted_entries: [usize; 3],
    evicted_bytes: [usize; 3],
    promoted_bytes: [usize; 3],
}

impl ReclaimStats {
    pub const fn reclaimed_segments(self, priority: CachePriority) -> usize {
        self.reclaimed_segments[priority as usize]
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

    pub const fn total_reclaimed_segments(self) -> usize {
        sum_priority_counts(self.reclaimed_segments)
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

    pub(crate) fn record(
        &mut self,
        priority: CachePriority,
        evicted_entries: usize,
        promoted_entries: usize,
        evicted_bytes: usize,
        promoted_bytes: usize,
    ) {
        let priority = priority as usize;
        self.reclaimed_segments[priority] = self.reclaimed_segments[priority].saturating_add(1);
        self.evicted_entries[priority] = self.evicted_entries[priority].saturating_add(evicted_entries);
        self.promoted_entries[priority] = self.promoted_entries[priority].saturating_add(promoted_entries);
        self.evicted_bytes[priority] = self.evicted_bytes[priority].saturating_add(evicted_bytes);
        self.promoted_bytes[priority] = self.promoted_bytes[priority].saturating_add(promoted_bytes);
    }

    pub(crate) fn merge(&mut self, other: Self) {
        for priority in 0..3 {
            self.reclaimed_segments[priority] =
                self.reclaimed_segments[priority].saturating_add(other.reclaimed_segments[priority]);
            self.evicted_entries[priority] =
                self.evicted_entries[priority].saturating_add(other.evicted_entries[priority]);
            self.promoted_entries[priority] =
                self.promoted_entries[priority].saturating_add(other.promoted_entries[priority]);
            self.evicted_bytes[priority] = self.evicted_bytes[priority].saturating_add(other.evicted_bytes[priority]);
            self.promoted_bytes[priority] =
                self.promoted_bytes[priority].saturating_add(other.promoted_bytes[priority]);
        }
    }
}

const fn sum_priority_counts(values: [usize; 3]) -> usize {
    values[0].saturating_add(values[1]).saturating_add(values[2])
}
