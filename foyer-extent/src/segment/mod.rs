mod checkpoint;
#[cfg(test)]
mod compatibility_tests;
mod config;
mod engine;
mod format;
mod index;
mod io;
mod operation;
mod reclaim;
mod stats;
mod store;

pub use self::{
    checkpoint::CheckpointStats,
    config::{DEFAULT_SEGMENT_SIZE, SegmentEngineConfig},
    engine::SegmentEngine,
    format::EXTENT_FORMAT_VERSION,
    index::{IndexReadStats, IndexStats},
    io::IoSchedulerStats,
    stats::{PhysicalWriteStats, ReclaimStats},
};
pub(crate) use self::{
    format::SegmentLayout,
    operation::{BatchInsertResult, BlobInsert, InsertOutcome},
};

#[cfg(test)]
pub(crate) use self::{config::SegmentEngineOptions, engine::InjectedFault};

#[cfg(test)]
pub(crate) fn crash_if_requested(point: &str) {
    if std::env::var("EXTENT_ENGINE_CRASH_AT").as_deref() == Ok(point) {
        std::process::abort();
    }
}
