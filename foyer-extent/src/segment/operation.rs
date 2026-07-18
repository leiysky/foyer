use crate::{
    model::{BlobKey, CachePriority},
    segment::stats::ReclaimStats,
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
    pub data_slots: usize,
    pub data_runs: usize,
    pub data_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct BlobInsert<'a> {
    pub key: &'a BlobKey,
    pub value: &'a [u8],
    pub priority: CachePriority,
}

impl<'a> BlobInsert<'a> {
    pub const fn new(key: &'a BlobKey, value: &'a [u8], priority: CachePriority) -> Self {
        Self { key, value, priority }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BatchInsertResult {
    pub outcomes: Vec<InsertOutcome>,
    /// Number of physical write syscalls planned after adjacent slots were merged.
    pub write_runs: usize,
    /// Full aligned allocation bytes submitted by the physical batch writer.
    pub written_bytes: usize,
    pub reclaim: ReclaimStats,
}
