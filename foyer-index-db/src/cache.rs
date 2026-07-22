use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

const MAX_SHARDS: usize = 64;
// Bloom pages are small, immutable, and consulted for every L0 miss. Keep a bounded direct-access
// tier while leaving most of the configured budget available to data pages.
const PINNED_METADATA_DIVISOR: usize = 8;
// The reservation is only an eviction preference: either class borrows unused capacity, while
// metadata can displace a one-pass data scan until it reaches half of the shared budget.
const METADATA_RESERVATION_DIVISOR: usize = 2;

#[derive(Debug)]
struct MetadataBudget {
    capacity: usize,
    used: AtomicUsize,
    filter_checks: [AtomicU64; MAX_SHARDS],
    filter_positives: [AtomicU64; MAX_SHARDS],
}

impl MetadataBudget {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            used: AtomicUsize::new(0),
            filter_checks: std::array::from_fn(|_| AtomicU64::new(0)),
            filter_positives: std::array::from_fn(|_| AtomicU64::new(0)),
        })
    }

    fn reserve(self: &Arc<Self>, bytes: usize) -> Option<MetadataReservation> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |used| {
                used.checked_add(bytes).filter(|used| *used <= self.capacity)
            })
            .ok()?;
        Some(MetadataReservation {
            budget: self.clone(),
            bytes,
        })
    }

    fn record_filter(&self, shard: usize, positive: bool) {
        self.filter_checks[shard].fetch_add(1, Ordering::Relaxed);
        if positive {
            self.filter_positives[shard].fetch_add(1, Ordering::Relaxed);
        }
    }

    fn stats(&self) -> (u64, u64, u64) {
        let filter_checks = self
            .filter_checks
            .iter()
            .map(|counter| counter.load(Ordering::Relaxed))
            .sum();
        let filter_positives = self
            .filter_positives
            .iter()
            .map(|counter| counter.load(Ordering::Relaxed))
            .sum();
        (
            self.used.load(Ordering::Relaxed) as u64,
            filter_checks,
            filter_positives,
        )
    }
}

#[derive(Debug)]
struct MetadataReservation {
    budget: Arc<MetadataBudget>,
    bytes: usize,
}

impl Drop for MetadataReservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::Release);
    }
}

#[derive(Debug)]
pub struct PinnedMetadata {
    data: Box<[u8]>,
    _reservation: MetadataReservation,
}

impl PinnedMetadata {
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

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
    hits: u64,
    misses: u64,
    data_hits: u64,
    filter_accesses: u64,
    filter_positives: u64,
    entries: HashMap<CacheKey, CacheSlot>,
    data_clock: VecDeque<CacheKey>,
    metadata_clock: VecDeque<CacheKey>,
}

impl CacheShard {
    fn get_with<T>(&mut self, key: CacheKey, read: impl FnOnce(&[u8]) -> T) -> Option<T> {
        let slot = self.entries.get_mut(&key)?;
        slot.referenced = true;
        Some(read(&slot.data))
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
                .expect("a non-empty IndexDB cache must have a clock entry");
            let referenced = self
                .entries
                .get(&victim)
                .expect("IndexDB cache clock must reference a resident entry")
                .referenced;
            if referenced {
                self.entries
                    .get_mut(&victim)
                    .expect("IndexDB cache entry must remain resident")
                    .referenced = false;
                self.clock(kind).push_back(victim);
            } else {
                let removed = self
                    .entries
                    .remove(&victim)
                    .expect("IndexDB cache victim must remain resident");
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
    pub data_hits: u64,
    pub filter_accesses: u64,
    pub filter_positives: u64,
    pub resident_bytes: u64,
    pub data_resident_bytes: u64,
    pub metadata_resident_bytes: u64,
}

#[derive(Debug)]
pub struct BlockCache {
    shards: Box<[Mutex<CacheShard>]>,
    pinned_metadata: Arc<MetadataBudget>,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        let pinned_metadata = MetadataBudget::new(capacity / PINNED_METADATA_DIVISOR);
        let capacity = capacity - capacity / PINNED_METADATA_DIVISOR;
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
                hits: 0,
                misses: 0,
                data_hits: 0,
                filter_accesses: 0,
                filter_positives: 0,
                entries: HashMap::new(),
                data_clock: VecDeque::new(),
                metadata_clock: VecDeque::new(),
            }));
        }
        Self {
            shards: shards.into_boxed_slice(),
            pinned_metadata,
        }
    }

    pub fn get_with<T>(&self, key: CacheKey, read: impl FnOnce(&[u8]) -> T) -> Option<T> {
        debug_assert_eq!(key.kind, CacheKind::Data);
        let mut shard = mutex_lock(self.shard(key));
        let data = shard.get_with(key, read);
        if data.is_some() {
            shard.hits += 1;
            shard.data_hits += 1;
        } else {
            shard.misses += 1;
        }
        data
    }

    pub fn get_filter_with(&self, key: CacheKey, read: impl FnOnce(&[u8]) -> bool) -> Option<bool> {
        debug_assert_eq!(key.kind, CacheKind::Filter);
        let mut shard = mutex_lock(self.shard(key));
        let result = shard.get_with(key, read);
        if result.is_some() {
            shard.hits += 1;
            shard.filter_accesses += 1;
            shard.filter_positives += u64::from(result == Some(true));
        } else {
            shard.misses += 1;
        }
        result
    }

    pub fn record_filter_result(&self, key: CacheKey, positive: bool) {
        debug_assert_eq!(key.kind, CacheKind::Filter);
        let mut shard = mutex_lock(self.shard(key));
        shard.filter_accesses += 1;
        shard.filter_positives += u64::from(positive);
    }

    pub fn pin_metadata(&self, data: Box<[u8]>) -> Result<PinnedMetadata, Box<[u8]>> {
        let Some(reservation) = self.pinned_metadata.reserve(data.len()) else {
            return Err(data);
        };
        Ok(PinnedMetadata {
            data,
            _reservation: reservation,
        })
    }

    pub fn record_pinned_filter(&self, key: CacheKey, positive: bool) {
        debug_assert_eq!(key.kind, CacheKind::Filter);
        self.pinned_metadata.record_filter(self.shard_index(key), positive);
    }

    pub fn insert(&self, key: CacheKey, data: Arc<[u8]>) -> Arc<[u8]> {
        mutex_lock(self.shard(key)).insert(key, data)
    }

    pub fn stats(&self) -> CacheStats {
        let (hits, misses, data_hits, filter_accesses, filter_positives, resident_bytes, metadata_resident_bytes) =
            self.shards.iter().fold(
                (0_u64, 0_u64, 0_u64, 0_u64, 0_u64, 0_u64, 0_u64),
                |(
                    hits,
                    misses,
                    data_hits,
                    filter_accesses,
                    filter_positives,
                    resident_bytes,
                    metadata_resident_bytes,
                ),
                 shard| {
                    let shard = mutex_lock(shard);
                    (
                        hits + shard.hits,
                        misses + shard.misses,
                        data_hits + shard.data_hits,
                        filter_accesses + shard.filter_accesses,
                        filter_positives + shard.filter_positives,
                        resident_bytes + shard.used as u64,
                        metadata_resident_bytes + shard.metadata_used as u64,
                    )
                },
            );
        let (pinned_metadata_bytes, pinned_filter_checks, pinned_filter_positives) = self.pinned_metadata.stats();
        CacheStats {
            hits: hits + pinned_filter_checks,
            misses,
            data_hits,
            filter_accesses: filter_accesses + pinned_filter_checks,
            filter_positives: filter_positives + pinned_filter_positives,
            resident_bytes: resident_bytes + pinned_metadata_bytes,
            data_resident_bytes: resident_bytes - metadata_resident_bytes,
            metadata_resident_bytes: metadata_resident_bytes + pinned_metadata_bytes,
        }
    }

    fn shard(&self, key: CacheKey) -> &Mutex<CacheShard> {
        &self.shards[self.shard_index(key)]
    }

    fn shard_index(&self, key: CacheKey) -> usize {
        key.file_id.wrapping_add(u64::from(key.block)) as usize % self.shards.len()
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

        assert_eq!(cache.get_filter_with(metadata, |_| false), Some(false));
        let stats = cache.stats();
        assert_eq!(stats.metadata_resident_bytes, 64);
        assert!(stats.data_resident_bytes <= 192);
    }

    #[test]
    fn pinned_metadata_is_bounded_and_releases_its_reservation() {
        let cache = BlockCache::new(64 * 1024);
        let pinned = cache.pin_metadata(vec![0; 8 * 1024].into_boxed_slice()).unwrap();
        assert!(cache.pin_metadata(vec![0; 1].into_boxed_slice()).is_err());
        let filter = CacheKey {
            file_id: 3,
            block: 7,
            kind: CacheKind::Filter,
        };
        cache.record_pinned_filter(filter, false);
        cache.record_pinned_filter(filter, true);
        for block in 0..16 {
            cache.insert(
                CacheKey {
                    file_id: 4,
                    block,
                    kind: CacheKind::Data,
                },
                Arc::from([0; 8 * 1024]),
            );
        }
        let stats = cache.stats();
        assert!(stats.resident_bytes <= 64 * 1024);
        assert_eq!(stats.metadata_resident_bytes, 8 * 1024);
        assert_eq!(stats.filter_accesses, 2);
        assert_eq!(stats.filter_positives, 1);
        assert_eq!(stats.hits, 2);

        drop(pinned);
        assert!(cache.pin_metadata(vec![0; 8 * 1024].into_boxed_slice()).is_ok());
    }
}
