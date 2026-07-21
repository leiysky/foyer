mod checkpoint;
#[cfg(test)]
mod compatibility_tests;
mod config;
mod core;
mod format;
mod index;
mod io;
mod operation;
mod pool;
mod reclaim;
mod stats;

pub use self::{
    checkpoint::CheckpointStats,
    config::{
        DEFAULT_EXTENT_SIZE, DEFAULT_HIGH_PRIORITY_CAPACITY_PERCENT, DEFAULT_NORMAL_PRIORITY_CAPACITY_PERCENT,
        ExtentStoreConfig,
    },
    core::ExtentStore,
    format::EXTENT_FORMAT_VERSION,
    index::{EntryIndexReadStats, EntryIndexStats},
    io::IoSchedulerStats,
    stats::{DirectoryReadStats, ExtentLayoutStats, ExtentOccupancy, PhysicalWriteStats, ReclaimStats},
};
#[cfg(test)]
pub(crate) use self::{config::ExtentStoreOptions, core::InjectedFault};
pub(crate) use self::{
    config::PriorityCapacityFloors,
    core::PreparedGet,
    format::StoreLayout,
    operation::{BatchInsertResult, EntryInsert, InsertOutcome},
};

#[cfg(test)]
pub(crate) fn crash_if_requested(point: &str) {
    if std::env::var("EXTENT_STORE_CRASH_AT").as_deref() == Ok(point) {
        std::process::abort();
    }
}
