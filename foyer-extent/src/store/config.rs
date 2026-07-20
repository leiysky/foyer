use std::time::Duration;

use crate::format::DEFAULT_SLOT_SIZE;

pub const DEFAULT_EXTENT_SIZE: usize = 64 * 1024 * 1024;
pub const DEFAULT_HIGH_PRIORITY_CAPACITY_PERCENT: u8 = 10;
pub const DEFAULT_NORMAL_PRIORITY_CAPACITY_PERCENT: u8 = 70;
const DEFAULT_READ_RUN_SIZE: usize = DEFAULT_SLOT_SIZE;
const DEFAULT_WRITE_RUN_SIZE: usize = 1024 * 1024;
const DEFAULT_INDEX_WRITE_BUFFER_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_INDEX_CACHE_SIZE: usize = 1024 * 1024 * 1024;
const DEFAULT_IO_READ_PRIORITY_DURATION: Duration = Duration::from_millis(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriorityCapacityFloors {
    high_percent: u8,
    normal_percent: u8,
}

impl PriorityCapacityFloors {
    pub const fn new(high_percent: u8, normal_percent: u8) -> Self {
        Self {
            high_percent,
            normal_percent,
        }
    }

    pub const fn high_percent(self) -> u8 {
        self.high_percent
    }

    pub const fn normal_percent(self) -> u8 {
        self.normal_percent
    }

    pub fn extent_floors(self, usable_extents: u32) -> [u32; 3] {
        let floor = |percent: u8| {
            if percent == 0 {
                0
            } else {
                let extents = u64::from(usable_extents)
                    .saturating_mul(u64::from(percent))
                    .div_ceil(100);
                u32::try_from(extents).expect("a capacity floor cannot exceed the usable extent count")
            }
        };
        [0, floor(self.normal_percent), floor(self.high_percent)]
    }
}

impl Default for PriorityCapacityFloors {
    fn default() -> Self {
        Self::new(
            DEFAULT_HIGH_PRIORITY_CAPACITY_PERCENT,
            DEFAULT_NORMAL_PRIORITY_CAPACITY_PERCENT,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtentStoreOptions {
    pub extent_size: usize,
    pub write_concurrency: usize,
    pub io_read_priority_duration: Duration,
    pub read_run_size: usize,
    pub write_run_size: usize,
    pub index_write_buffer_size: usize,
    pub index_cache_size: usize,
    pub checkpoint_changes: usize,
    pub hot_frequency: u8,
    pub low_hot_frequency: u8,
    pub priority_capacity_floors: PriorityCapacityFloors,
    pub direct_io: bool,
}

impl Default for ExtentStoreOptions {
    fn default() -> Self {
        Self {
            extent_size: DEFAULT_EXTENT_SIZE,
            write_concurrency: 1,
            io_read_priority_duration: DEFAULT_IO_READ_PRIORITY_DURATION,
            read_run_size: DEFAULT_READ_RUN_SIZE,
            write_run_size: DEFAULT_WRITE_RUN_SIZE,
            index_write_buffer_size: DEFAULT_INDEX_WRITE_BUFFER_SIZE,
            index_cache_size: DEFAULT_INDEX_CACHE_SIZE,
            checkpoint_changes: 4_096,
            hot_frequency: 2,
            low_hot_frequency: 2,
            priority_capacity_floors: PriorityCapacityFloors::default(),
            direct_io: false,
        }
    }
}

#[cfg(test)]
impl ExtentStoreOptions {
    pub fn with_extent_size(mut self, extent_size: usize) -> Self {
        self.extent_size = extent_size;
        self
    }

    pub fn with_read_run_size(mut self, size: usize) -> Self {
        self.read_run_size = size;
        self
    }

    pub fn with_index_write_buffer_size(mut self, size: usize) -> Self {
        self.index_write_buffer_size = size;
        self
    }

    pub fn with_index_cache_size(mut self, size: usize) -> Self {
        self.index_cache_size = size;
        self
    }

    pub fn with_checkpoint_changes(mut self, changes: usize) -> Self {
        self.checkpoint_changes = changes;
        self
    }

    pub fn with_hot_frequency(mut self, frequency: u8) -> Self {
        self.hot_frequency = frequency;
        self
    }

    pub fn with_low_hot_frequency(mut self, frequency: u8) -> Self {
        self.low_hot_frequency = frequency;
        self
    }

    pub fn with_priority_capacity_floors(mut self, high_percent: u8, normal_percent: u8) -> Self {
        self.priority_capacity_floors = PriorityCapacityFloors::new(high_percent, normal_percent);
        self
    }

    #[cfg(target_os = "linux")]
    pub fn with_direct_io(mut self, direct_io: bool) -> Self {
        self.direct_io = direct_io;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtentStoreConfig {
    pub capacity_bytes: u64,
    pub slot_size: usize,
    pub options: ExtentStoreOptions,
}

impl ExtentStoreConfig {
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            capacity_bytes,
            slot_size: DEFAULT_SLOT_SIZE,
            options: ExtentStoreOptions::default(),
        }
    }

    #[cfg(test)]
    pub fn with_slot_size(mut self, slot_size: usize) -> Self {
        self.slot_size = slot_size;
        self
    }

    #[cfg(test)]
    pub fn with_options(mut self, options: ExtentStoreOptions) -> Self {
        self.options = options;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_capacity_rounds_up_to_reclaim_units() {
        assert_eq!(PriorityCapacityFloors::new(10, 70).extent_floors(5), [0, 4, 1]);
        assert_eq!(PriorityCapacityFloors::new(0, 100).extent_floors(5), [0, 5, 0]);
    }
}
