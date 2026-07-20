use crate::{
    model::{CachePriority, EntryKey},
    store::stats::ReclaimStats,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    Updated,
    Rejected,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GetResult {
    pub value: Option<Vec<u8>>,
    pub priority: Option<CachePriority>,
    pub data_frames: usize,
    pub data_runs: usize,
    pub data_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct EntryInsert<'a> {
    pub key: &'a EntryKey,
    pub value: &'a [u8],
    pub priority: CachePriority,
}

impl<'a> EntryInsert<'a> {
    pub const fn new(key: &'a EntryKey, value: &'a [u8], priority: CachePriority) -> Self {
        Self { key, value, priority }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BatchInsertResult {
    pub outcomes: Vec<InsertOutcome>,
    /// Number of physical write syscalls planned after adjacent byte ranges were merged.
    pub write_runs: usize,
    /// Full aligned allocation bytes submitted by the physical batch writer.
    pub written_bytes: usize,
    pub reclaim: ReclaimStats,
}
