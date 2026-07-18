use crate::format::DEFAULT_SLOT_SIZE;

const DEFAULT_SEGMENT_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_READ_RUN_SIZE: usize = DEFAULT_SLOT_SIZE;
const DEFAULT_WRITE_RUN_SIZE: usize = 1024 * 1024;
const DEFAULT_INDEX_WRITE_BUFFER_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_INDEX_CACHE_SIZE: usize = 512 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentEngineOptions {
    pub segment_size: usize,
    pub write_concurrency: usize,
    pub read_run_size: usize,
    pub write_run_size: usize,
    pub index_write_buffer_size: usize,
    pub index_cache_size: usize,
    pub checkpoint_changes: usize,
    pub hot_frequency: u8,
    pub low_hot_frequency: u8,
    pub direct_io: bool,
}

impl Default for SegmentEngineOptions {
    fn default() -> Self {
        Self {
            segment_size: DEFAULT_SEGMENT_SIZE,
            write_concurrency: 1,
            read_run_size: DEFAULT_READ_RUN_SIZE,
            write_run_size: DEFAULT_WRITE_RUN_SIZE,
            index_write_buffer_size: DEFAULT_INDEX_WRITE_BUFFER_SIZE,
            index_cache_size: DEFAULT_INDEX_CACHE_SIZE,
            checkpoint_changes: 4_096,
            hot_frequency: 2,
            low_hot_frequency: 2,
            direct_io: false,
        }
    }
}

#[cfg(test)]
impl SegmentEngineOptions {
    pub fn with_segment_size(mut self, segment_size: usize) -> Self {
        self.segment_size = segment_size;
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

    #[cfg(target_os = "linux")]
    pub fn with_direct_io(mut self, direct_io: bool) -> Self {
        self.direct_io = direct_io;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentEngineConfig {
    pub capacity_bytes: u64,
    pub slot_size: usize,
    pub options: SegmentEngineOptions,
}

impl SegmentEngineConfig {
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            capacity_bytes,
            slot_size: DEFAULT_SLOT_SIZE,
            options: SegmentEngineOptions::default(),
        }
    }

    #[cfg(test)]
    pub fn with_slot_size(mut self, slot_size: usize) -> Self {
        self.slot_size = slot_size;
        self
    }

    #[cfg(test)]
    pub fn with_options(mut self, options: SegmentEngineOptions) -> Self {
        self.options = options;
        self
    }
}
