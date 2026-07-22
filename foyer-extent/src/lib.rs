mod cache;
mod entry;
mod error;
mod file;
mod format;
mod foyer_engine;
mod model;
mod store;

const _: () = assert!(foyer::DISK_ENGINE_API_VERSION == 1);

pub use self::{
    cache::{Cache, CacheBuilder},
    entry::{EngineValue, Entry},
    error::{Error, Result},
    format::DEFAULT_ENTRY_CHARGE,
    foyer_engine::{EngineReadStats, EngineWriteStats, ExtentEngineConfig, ExtentEngineHandle},
    model::{CachePriority, MAX_KEY_SIZE},
    store::{
        CheckpointStats, DEFAULT_EXTENT_SIZE, DEFAULT_HIGH_PRIORITY_CAPACITY_PERCENT,
        DEFAULT_NORMAL_PRIORITY_CAPACITY_PERCENT, EXTENT_FORMAT_VERSION, EntryIndexReadStats, EntryIndexStats,
        ExtentLayoutStats, ExtentOccupancy, IoSchedulerStats, PhysicalWriteStats, ReclaimStats,
    },
};
