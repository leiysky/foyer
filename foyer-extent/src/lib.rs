mod cache;
mod entry;
mod error;
mod file;
mod format;
mod foyer_engine;
mod frequency;
mod model;
mod segment;

const _: () = assert!(foyer::DISK_ENGINE_API_VERSION == 1);

pub use self::{
    cache::{Cache, CacheBuilder},
    entry::{EngineValue, Entry},
    error::{Error, Result},
    format::DEFAULT_SLOT_SIZE,
    foyer_engine::{EngineReadStats, EngineWriteStats, ExtentEngineConfig, ExtentEngineHandle},
    model::{CachePriority, MAX_BLOB_KEY_SIZE},
    segment::{CheckpointStats, IndexReadStats, IndexStats, PhysicalWriteStats, ReclaimStats},
};
