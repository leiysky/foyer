use std::borrow::Cow;

use bytes::Bytes;
use foyer::{Hint, HybridCache, HybridCachePolicy, HybridCacheProperties, RecoverMode, Spawner};

use crate::{CachePriority, EngineValue, Entry, Error, ExtentEngineConfig, Result, model::BlobKey};

const CACHE_ENTRY_META_SIZE: usize = 64;

/// The public hybrid Extent cache.
#[derive(Debug, Clone)]
pub struct Cache {
    inner: HybridCache<Bytes, EngineValue>,
}

impl Cache {
    pub fn builder(memory_capacity: usize, engine: ExtentEngineConfig) -> CacheBuilder {
        CacheBuilder::new(memory_capacity, engine)
    }

    /// Offer an entry to the cache without waiting for disk admission or persistence.
    pub fn put(&self, entry: Entry) {
        let priority = entry.priority();
        let (key, value) = entry.into_engine();
        let hint = match priority {
            CachePriority::Low => Hint::Low,
            CachePriority::Normal | CachePriority::High => Hint::Normal,
        };
        let properties = HybridCacheProperties::default().with_hint(hint);
        self.inner.insert_with_properties(key, value, properties);
    }

    /// Look up a complete entry.
    ///
    /// Storage errors and throttling are cache misses at this best-effort boundary.
    pub async fn get(&self, key: &[u8]) -> Option<Entry> {
        if BlobKey::validate(key).is_err() {
            return None;
        }
        let key = Bytes::copy_from_slice(key);
        self.inner
            .get(&key)
            .await
            .ok()
            .flatten()
            .map(|entry| Entry::from_engine(entry.key().clone(), entry.value().clone()))
    }

    /// Offer a best-effort deletion without waiting for the disk engine.
    pub fn delete(&self, key: &[u8]) {
        if BlobKey::validate(key).is_err() {
            return;
        }
        self.inner.remove(&Bytes::copy_from_slice(key));
    }

    /// Gracefully drain and close the cache.
    pub async fn close(&self) -> Result<()> {
        self.inner
            .close()
            .await
            .map_err(|source| Error::foyer("close Extent cache", source))
    }
}

/// Builder for the public hybrid Extent cache.
#[derive(Debug)]
pub struct CacheBuilder {
    name: Cow<'static, str>,
    memory_capacity: usize,
    memory_shards: Option<usize>,
    recover_mode: RecoverMode,
    spawner: Option<Spawner>,
    engine: ExtentEngineConfig,
}

impl CacheBuilder {
    pub fn new(memory_capacity: usize, engine: ExtentEngineConfig) -> Self {
        Self {
            name: "extent".into(),
            memory_capacity,
            memory_shards: None,
            recover_mode: RecoverMode::Quiet,
            spawner: None,
            engine,
        }
    }

    pub fn with_name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.name = name.into();
        self
    }

    pub fn with_memory_shards(mut self, shards: usize) -> Self {
        self.memory_shards = Some(shards);
        self
    }

    pub fn with_recover_mode(mut self, recover_mode: RecoverMode) -> Self {
        self.recover_mode = recover_mode;
        self
    }

    pub fn with_spawner(mut self, spawner: Spawner) -> Self {
        self.spawner = Some(spawner);
        self
    }

    pub async fn build(self) -> Result<Cache> {
        if self.memory_capacity == 0 {
            return Err(Error::InvalidConfig(
                "memory cache capacity must be positive".to_string(),
            ));
        }
        if self.memory_shards == Some(0) {
            return Err(Error::InvalidConfig(
                "memory cache shard count must be positive".to_string(),
            ));
        }

        let mut memory = HybridCache::builder()
            .with_name(self.name)
            .with_flush_on_close(false)
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .memory(self.memory_capacity)
            .with_weighter(|key: &Bytes, value: &EngineValue| {
                key.len()
                    .saturating_add(value.value().len())
                    .saturating_add(CACHE_ENTRY_META_SIZE)
            });
        if let Some(shards) = self.memory_shards {
            memory = memory.with_shards(shards);
        }
        let mut storage = memory
            .storage()
            .with_recover_mode(self.recover_mode)
            .with_engine_config(
                Box::new(self.engine) as Box<dyn foyer::EngineConfig<Bytes, EngineValue, HybridCacheProperties>>
            );
        if let Some(spawner) = self.spawner {
            storage = storage.with_spawner(spawner);
        }
        let inner = storage
            .build()
            .await
            .map_err(|source| Error::foyer("open Extent cache", source))?;
        Ok(Cache { inner })
    }
}
