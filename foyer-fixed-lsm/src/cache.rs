use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
};

const MAX_SHARDS: usize = 64;
// The reservation is only an eviction preference: either class borrows unused capacity, while
// metadata can displace a one-pass data scan until it reaches half of the shared budget.
const METADATA_RESERVATION_DIVISOR: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheKind {
    Data,
    Filter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub file_id: u64,
    pub block: u32,
    pub kind: CacheKind,
}

#[derive(Debug)]
struct CacheSlot {
    data: Arc<[u8]>,
    referenced: bool,
}

#[derive(Debug)]
struct CacheShard {
    capacity: usize,
    metadata_reservation: usize,
    used: usize,
    metadata_used: usize,
    entries: HashMap<CacheKey, CacheSlot>,
    data_clock: VecDeque<CacheKey>,
    metadata_clock: VecDeque<CacheKey>,
}

impl CacheShard {
    fn get(&mut self, key: CacheKey) -> Option<Arc<[u8]>> {
        let slot = self.entries.get_mut(&key)?;
        slot.referenced = true;
        Some(slot.data.clone())
    }

    fn insert(&mut self, key: CacheKey, data: Arc<[u8]>) -> Arc<[u8]> {
        if let Some(slot) = self.entries.get_mut(&key) {
            slot.referenced = true;
            return slot.data.clone();
        }
        if self.capacity == 0 || data.len() > self.capacity {
            return data;
        }
        while self.used + data.len() > self.capacity {
            let kind = self.eviction_kind();
            self.evict_one(kind);
        }
        self.used += data.len();
        if key.kind != CacheKind::Data {
            self.metadata_used += data.len();
            self.metadata_clock.push_back(key);
        } else {
            self.data_clock.push_back(key);
        }
        self.entries.insert(
            key,
            CacheSlot {
                data: data.clone(),
                referenced: true,
            },
        );
        data
    }

    fn eviction_kind(&self) -> CacheKind {
        let data_used = self.used - self.metadata_used;
        let data_target = self.capacity - self.metadata_reservation;
        if data_used > data_target && !self.data_clock.is_empty() {
            CacheKind::Data
        } else if self.metadata_used > self.metadata_reservation && !self.metadata_clock.is_empty() {
            CacheKind::Filter
        } else if !self.data_clock.is_empty() {
            CacheKind::Data
        } else {
            CacheKind::Filter
        }
    }

    fn evict_one(&mut self, kind: CacheKind) {
        loop {
            let victim = self
                .clock(kind)
                .pop_front()
                .expect("a non-empty fixed-lsm cache must have a clock entry");
            let referenced = self
                .entries
                .get(&victim)
                .expect("fixed-lsm cache clock must reference a resident entry")
                .referenced;
            if referenced {
                self.entries
                    .get_mut(&victim)
                    .expect("fixed-lsm cache entry must remain resident")
                    .referenced = false;
                self.clock(kind).push_back(victim);
            } else {
                let removed = self
                    .entries
                    .remove(&victim)
                    .expect("fixed-lsm cache victim must remain resident");
                self.used -= removed.data.len();
                if kind == CacheKind::Filter {
                    self.metadata_used -= removed.data.len();
                }
                return;
            }
        }
    }

    fn clock(&mut self, kind: CacheKind) -> &mut VecDeque<CacheKey> {
        match kind {
            CacheKind::Data => &mut self.data_clock,
            CacheKind::Filter => &mut self.metadata_clock,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub resident_bytes: u64,
    pub data_resident_bytes: u64,
    pub metadata_resident_bytes: u64,
}

#[derive(Debug)]
pub struct BlockCache {
    shards: Box<[Mutex<CacheShard>]>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        let shard_count = if capacity == 0 {
            1
        } else {
            (capacity / (64 * 1024)).clamp(1, MAX_SHARDS)
        };
        let base_capacity = capacity / shard_count;
        let remainder = capacity % shard_count;
        let mut shards = Vec::with_capacity(shard_count);
        for shard in 0..shard_count {
            shards.push(Mutex::new(CacheShard {
                capacity: base_capacity + usize::from(shard < remainder),
                metadata_reservation: (base_capacity + usize::from(shard < remainder)) / METADATA_RESERVATION_DIVISOR,
                used: 0,
                metadata_used: 0,
                entries: HashMap::new(),
                data_clock: VecDeque::new(),
                metadata_clock: VecDeque::new(),
            }));
        }
        Self {
            shards: shards.into_boxed_slice(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, key: CacheKey) -> Option<Arc<[u8]>> {
        let data = mutex_lock(self.shard(key)).get(key);
        if data.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        data
    }

    pub fn insert(&self, key: CacheKey, data: Arc<[u8]>) -> Arc<[u8]> {
        mutex_lock(self.shard(key)).insert(key, data)
    }

    pub fn stats(&self) -> CacheStats {
        let (resident_bytes, metadata_resident_bytes) =
            self.shards
                .iter()
                .fold((0_u64, 0_u64), |(resident_bytes, metadata_resident_bytes), shard| {
                    let shard = mutex_lock(shard);
                    (
                        resident_bytes + shard.used as u64,
                        metadata_resident_bytes + shard.metadata_used as u64,
                    )
                });
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            resident_bytes,
            data_resident_bytes: resident_bytes - metadata_resident_bytes,
            metadata_resident_bytes,
        }
    }

    fn shard(&self, key: CacheKey) -> &Mutex<CacheShard> {
        &self.shards[key.file_id.wrapping_add(u64::from(key.block)) as usize % self.shards.len()]
    }
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::cache::{BlockCache, CacheKey, CacheKind};

    #[test]
    fn clock_cache_respects_its_byte_budget() {
        let cache = BlockCache::new(128);
        for block in 0..8 {
            cache.insert(
                CacheKey {
                    file_id: 1,
                    block,
                    kind: CacheKind::Data,
                },
                Arc::from([block as u8; 64]),
            );
        }
        assert!(cache.stats().resident_bytes <= 128);
    }

    #[test]
    fn metadata_reservation_survives_a_data_scan() {
        let cache = BlockCache::new(256);
        let metadata = CacheKey {
            file_id: 1,
            block: 0,
            kind: CacheKind::Filter,
        };
        cache.insert(metadata, Arc::from([0_u8; 64]));
        for block in 0..8 {
            cache.insert(
                CacheKey {
                    file_id: 2,
                    block,
                    kind: CacheKind::Data,
                },
                Arc::from([block as u8; 64]),
            );
        }

        assert!(cache.get(metadata).is_some());
        let stats = cache.stats();
        assert_eq!(stats.metadata_resident_bytes, 64);
        assert!(stats.data_resident_bytes <= 192);
    }
}
