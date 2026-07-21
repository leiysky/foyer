use std::{
    collections::HashMap,
    mem::size_of,
    path::Path,
    sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use fixed_lsm::{FixedLsm, FixedLsmMemoryLookup, FixedLsmOptions, WriteBatch, WriteOptions};
use twox_hash::XxHash3_64;

#[cfg(test)]
use crate::format::PAGE_SIZE;
use crate::{
    error::{Error, Result},
    frequency::FrequencySketch,
    model::{CachePriority, KeyDigest},
    store::{
        format::EntryLocation,
        operation::{BatchInsertResult, InsertOutcome},
        stats::PhysicalWriteStats,
    },
};

pub(crate) const INDEX_DIRECTORY: &str = "index-lsm";

const HASH_SEED: u64 = 0xd6e8_feb8_6659_fd93;
const MIN_FREQUENCY_COUNTERS: usize = 4 * 1024;
const MAX_FREQUENCY_COUNTERS: usize = 16 * 1024 * 1024;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EntryIndexStats {
    pub manifest_generation: u64,
    pub last_sequence: u64,
    pub live_entries: u64,
    pub pending_changes: u64,
    pub disk_capacity_bytes: u64,
    pub disk_used_bytes: u64,
    pub wal_bytes: u64,
    pub sst_files: u64,
    pub sst_bytes: u64,
    pub mutable_entries: u64,
    pub immutable_memtables: u64,
    pub cache_resident_bytes: u64,
    pub frequency_counters: u64,
    pub frequency_bytes: u64,
    pub frequency_sample_window: u64,
    pub background_running: bool,
    pub background_failed: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EntryIndexReadStats {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub read_operations: u64,
    pub read_bytes: u64,
    pub filter_checks: u64,
    pub filter_positives: u64,
    pub false_positives: u64,
    pub data_reads: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntryIndexMemoryLookup {
    Location(EntryLocation),
    Miss,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Mutation {
    location: Option<EntryLocation>,
}

impl Mutation {
    const fn insert(location: EntryLocation) -> Self {
        Self {
            location: Some(location),
        }
    }

    const fn remove() -> Self {
        Self { location: None }
    }
}

#[derive(Debug, Default)]
struct RuntimeState {
    active: HashMap<KeyDigest, Mutation>,
    frozen: Option<Arc<HashMap<KeyDigest, Mutation>>>,
    live_count: u64,
    base_revision: u64,
}

#[derive(Debug)]
pub struct IndexCheckpoint {
    frozen: Arc<HashMap<KeyDigest, Mutation>>,
    live_count: u64,
}

#[derive(Debug)]
pub struct EntryIndex {
    database: FixedLsm,
    live_capacity: u64,
    capacity_bytes: u64,
    state: RwLock<RuntimeState>,
    mutations: Mutex<()>,
    frequency: RwLock<FrequencySketch>,
}

impl EntryIndex {
    pub fn create(
        root: &Path,
        live_capacity: u64,
        capacity_bytes: u64,
        write_buffer_capacity: usize,
        cache_capacity: usize,
    ) -> Result<Self> {
        let directory = root.join(INDEX_DIRECTORY);
        let database = FixedLsm::create(
            &directory,
            FixedLsmOptions {
                write_buffer_capacity,
                cache_capacity,
                max_disk_bytes: capacity_bytes,
            },
        )
        .map_err(|error| fixed_error("create", error))?;
        Ok(Self::from_parts(database, live_capacity, capacity_bytes))
    }

    pub fn open(
        root: &Path,
        live_capacity: u64,
        capacity_bytes: u64,
        write_buffer_capacity: usize,
        cache_capacity: usize,
    ) -> Result<Self> {
        let directory = root.join(INDEX_DIRECTORY);
        let database = FixedLsm::open(
            &directory,
            FixedLsmOptions {
                write_buffer_capacity,
                cache_capacity,
                max_disk_bytes: capacity_bytes,
            },
        )
        .map_err(|error| fixed_error("open", error))?;
        if database.user_state() > live_capacity {
            return Err(Error::InvalidSuperblock(format!(
                "EntryIndex live count {} exceeds capacity {live_capacity}",
                database.user_state()
            )));
        }
        Ok(Self::from_parts(database, live_capacity, capacity_bytes))
    }

    fn from_parts(database: FixedLsm, live_capacity: u64, capacity_bytes: u64) -> Self {
        let live_count = database.user_state();
        let frequency_counters = frequency_counters_for_entries(live_count);
        Self {
            database,
            live_capacity,
            capacity_bytes,
            state: RwLock::new(RuntimeState {
                live_count,
                ..RuntimeState::default()
            }),
            mutations: Mutex::new(()),
            frequency: RwLock::new(FrequencySketch::new(frequency_counters)),
        }
    }

    pub const fn file_size(&self) -> u64 {
        self.capacity_bytes
    }

    pub fn allocated_size(&self) -> Result<u64> {
        Ok(self.database.disk_used_bytes())
    }

    pub fn physical_write_stats(&self) -> PhysicalWriteStats {
        let stats = self.database.stats();
        PhysicalWriteStats {
            index_runs: stats.table_write_operations.saturating_add(stats.wal_write_operations),
            index_bytes: stats.table_write_bytes.saturating_add(stats.wal_write_bytes),
            // Extent checkpoints always append the WAL with synchronous durability, so each WAL
            // write operation is one foreground Index durability fence. Background SST/manifest
            // maintenance has separate structural counters and is not folded into this number.
            index_syncs: stats.wal_write_operations,
            ..PhysicalWriteStats::default()
        }
    }

    pub fn read_stats(&self) -> EntryIndexReadStats {
        let stats = self.database.stats();
        EntryIndexReadStats {
            cache_hits: stats.cache_hits,
            cache_misses: stats.cache_misses,
            read_operations: stats.table_read_operations,
            read_bytes: stats.table_read_bytes,
            filter_checks: stats.point_filter_checks,
            filter_positives: stats.point_filter_positives,
            false_positives: stats.point_false_positives,
            data_reads: stats.point_data_reads,
        }
    }

    pub fn io_read_stats(&self) -> EntryIndexReadStats {
        let stats = self.database.read_stats();
        EntryIndexReadStats {
            read_operations: stats.read_operations,
            read_bytes: stats.read_bytes,
            ..EntryIndexReadStats::default()
        }
    }

    pub fn stats(&self) -> EntryIndexStats {
        let database = self.database.stats();
        let state = read_lock(&self.state);
        let frequency = read_lock(&self.frequency);
        let pending_changes = state
            .active
            .len()
            .saturating_add(state.frozen.as_ref().map_or(0, |frozen| frozen.len()))
            as u64;
        EntryIndexStats {
            manifest_generation: database.manifest_generation,
            last_sequence: database.next_sequence.saturating_sub(1),
            live_entries: state.live_count,
            pending_changes,
            disk_capacity_bytes: database.disk_capacity_bytes,
            disk_used_bytes: database.disk_used_bytes,
            wal_bytes: database.wal_bytes,
            sst_files: database.level_files.into_iter().sum(),
            sst_bytes: database.level_bytes.into_iter().sum(),
            mutable_entries: database.mutable_entries,
            immutable_memtables: database.immutable_memtables,
            cache_resident_bytes: database.cache_resident_bytes,
            frequency_counters: frequency.counters() as u64,
            frequency_bytes: (frequency.counters() * size_of::<u64>()) as u64,
            frequency_sample_window: frequency.sample_window(),
            background_running: database.background_running,
            background_failed: database.background_failed,
        }
    }

    pub fn ensure_healthy(&self) -> Result<()> {
        self.database
            .ensure_healthy()
            .map_err(|error| fixed_error("health check", error))
    }

    pub fn wait_for_maintenance(&self) -> Result<()> {
        self.database
            .wait_for_maintenance()
            .map_err(|error| fixed_error("wait for maintenance", error))
    }

    pub fn lookup_memory(&self, key: KeyDigest) -> Result<EntryIndexMemoryLookup> {
        read_lock(&self.frequency).record(key_hash(key));
        loop {
            let base_revision = {
                let state = read_lock(&self.state);
                if let Some(mutation) = overlay_mutation(&state, key) {
                    return Ok(mutation
                        .location
                        .map_or(EntryIndexMemoryLookup::Miss, EntryIndexMemoryLookup::Location));
                }
                state.base_revision
            };
            let lookup = self
                .database
                .probe_memory(&encode_key(key))
                .map_err(|error| fixed_error("memory lookup", error))?;
            let state = read_lock(&self.state);
            if let Some(mutation) = overlay_mutation(&state, key) {
                return Ok(mutation
                    .location
                    .map_or(EntryIndexMemoryLookup::Miss, EntryIndexMemoryLookup::Location));
            }
            if state.base_revision == base_revision {
                return match lookup {
                    FixedLsmMemoryLookup::Value(value) => decode_location(value).map(EntryIndexMemoryLookup::Location),
                    FixedLsmMemoryLookup::Miss => Ok(EntryIndexMemoryLookup::Miss),
                    FixedLsmMemoryLookup::Unknown => Ok(EntryIndexMemoryLookup::Unknown),
                };
            }
        }
    }

    pub fn peek(&self, key: KeyDigest) -> Result<Option<EntryLocation>> {
        self.lookup(key)
    }

    pub fn probe(&self, key: KeyDigest, _priority: CachePriority) -> Result<(Option<EntryLocation>, bool)> {
        read_lock(&self.frequency).record(key_hash(key));
        Ok((self.lookup(key)?, true))
    }

    pub fn estimated_frequency(&self, key: KeyDigest) -> u8 {
        read_lock(&self.frequency).estimate(key_hash(key))
    }

    fn lookup(&self, key: KeyDigest) -> Result<Option<EntryLocation>> {
        self.lookup_with(key, || {
            self.database
                .get(&encode_key(key))
                .map_err(|error| fixed_error("lookup", error))
        })
    }

    fn lookup_with(
        &self,
        key: KeyDigest,
        mut durable_lookup: impl FnMut() -> Result<Option<[u8; fixed_lsm::VALUE_SIZE]>>,
    ) -> Result<Option<EntryLocation>> {
        loop {
            let base_revision = {
                let state = read_lock(&self.state);
                if let Some(mutation) = overlay_mutation(&state, key) {
                    return Ok(mutation.location);
                }
                state.base_revision
            };
            let value = durable_lookup()?;
            let state = read_lock(&self.state);
            if let Some(mutation) = overlay_mutation(&state, key) {
                return Ok(mutation.location);
            }
            if state.base_revision == base_revision {
                return value.map(decode_location).transpose();
            }
        }
    }

    pub fn insert_batch(&self, inserts: &[(KeyDigest, EntryLocation)]) -> Result<BatchInsertResult> {
        let mutation = mutex_lock(&self.mutations);
        let mut outcomes = Vec::with_capacity(inserts.len());
        for (key, location) in inserts.iter().copied() {
            let existing = self.lookup(key)?;
            if existing == Some(location) {
                outcomes.push(InsertOutcome::Updated);
                continue;
            }
            let mut state = write_lock(&self.state);
            if existing.is_none() && state.live_count >= self.live_capacity {
                return Err(Error::InvalidSuperblock(
                    "EntryIndex reached data capacity before allocation reclaimed an extent".to_string(),
                ));
            }
            state.active.insert(key, Mutation::insert(location));
            if existing.is_none() {
                state.live_count += 1;
                outcomes.push(InsertOutcome::Inserted);
            } else {
                outcomes.push(InsertOutcome::Updated);
            }
        }
        let live_count = read_lock(&self.state).live_count;
        self.maybe_resize_frequency(live_count);
        drop(mutation);
        Ok(BatchInsertResult {
            outcomes,
            ..BatchInsertResult::default()
        })
    }

    pub fn remove(&self, key: KeyDigest) -> Result<bool> {
        Ok(self.remove_batch(&[key])? == 1)
    }

    pub fn remove_batch(&self, keys: &[KeyDigest]) -> Result<usize> {
        let mutation = mutex_lock(&self.mutations);
        let mut removed = 0;
        for key in keys.iter().copied() {
            if self.lookup(key)?.is_none() {
                continue;
            }
            let mut state = write_lock(&self.state);
            state.active.insert(key, Mutation::remove());
            state.live_count = state.live_count.saturating_sub(1);
            removed += 1;
        }
        let live_count = read_lock(&self.state).live_count;
        self.maybe_resize_frequency(live_count);
        drop(mutation);
        Ok(removed)
    }

    pub fn checkpoint(&self) -> Result<()> {
        let Some(checkpoint) = self.prepare_checkpoint()? else {
            return Ok(());
        };
        self.persist_checkpoint(checkpoint)
    }

    pub fn prepare_checkpoint(&self) -> Result<Option<IndexCheckpoint>> {
        self.ensure_healthy()?;
        let _mutation = mutex_lock(&self.mutations);
        let mut state = write_lock(&self.state);
        if state.active.is_empty() {
            return Ok(None);
        }
        assert!(
            state.frozen.is_none(),
            "only one fixed-index checkpoint may be in flight"
        );
        let frozen = Arc::new(std::mem::take(&mut state.active));
        state.frozen = Some(frozen.clone());
        Ok(Some(IndexCheckpoint {
            frozen,
            live_count: state.live_count,
        }))
    }

    pub fn persist_checkpoint(&self, checkpoint: IndexCheckpoint) -> Result<()> {
        let mut batch = WriteBatch::with_capacity(checkpoint.frozen.len());
        for (key, mutation) in checkpoint.frozen.iter() {
            if let Some(location) = mutation.location {
                batch.put(encode_key(*key), location.encode());
            } else {
                batch.delete(encode_key(*key));
            }
        }
        if let Err(error) = self
            .database
            .write_with_user_state(&batch, WriteOptions::sync(), checkpoint.live_count)
        {
            self.restore_checkpoint(checkpoint);
            return Err(fixed_error("persist checkpoint", error));
        }
        #[cfg(test)]
        crate::store::crash_if_requested("entry_index_after_wal_sync");

        let mut state = write_lock(&self.state);
        let current = state
            .frozen
            .take()
            .expect("persisted fixed checkpoint must have a frozen overlay");
        assert!(
            Arc::ptr_eq(&current, &checkpoint.frozen),
            "persisted fixed checkpoint must install its own frozen overlay"
        );
        // An in-flight lookup may have missed this mutation in the old durable base and then find
        // no overlay after it is retired. Advancing the base revision makes that lookup retry.
        // Ordinary active-overlay writes do not advance it because the post-I/O overlay check sees
        // those mutations directly; keeping this checkpoint-scoped avoids read starvation under
        // unrelated high-churn writes.
        advance_base_revision(&mut state);
        Ok(())
    }

    pub fn abort_checkpoint(&self, checkpoint: IndexCheckpoint) {
        self.restore_checkpoint(checkpoint);
    }

    fn restore_checkpoint(&self, checkpoint: IndexCheckpoint) {
        let mut state = write_lock(&self.state);
        let current = state
            .frozen
            .take()
            .expect("aborted fixed checkpoint must have a frozen overlay");
        assert!(
            Arc::ptr_eq(&current, &checkpoint.frozen),
            "aborted fixed checkpoint must restore its own frozen overlay"
        );
        for (key, mutation) in checkpoint.frozen.iter() {
            state.active.entry(*key).or_insert(*mutation);
        }
    }

    fn maybe_resize_frequency(&self, live_count: u64) {
        let desired = frequency_counters_for_entries(live_count);
        let current = read_lock(&self.frequency).counters();
        if desired <= current && desired.saturating_mul(4) > current {
            return;
        }
        let mut frequency = write_lock(&self.frequency);
        let current = frequency.counters();
        if desired > current || desired.saturating_mul(4) <= current {
            // Frequency is an intentionally volatile heuristic. Resizing at power-of-two
            // cardinality boundaries may forget history, but avoids carrying stale temperature
            // across a radically different live set and keeps its aging window proportional.
            *frequency = FrequencySketch::new(desired);
        }
    }
}

fn frequency_counters_for_entries(entries: u64) -> usize {
    usize::try_from(entries / 4)
        .unwrap_or(MAX_FREQUENCY_COUNTERS)
        .clamp(MIN_FREQUENCY_COUNTERS, MAX_FREQUENCY_COUNTERS)
        .next_power_of_two()
        .min(MAX_FREQUENCY_COUNTERS)
}

fn decode_location(value: [u8; fixed_lsm::VALUE_SIZE]) -> Result<EntryLocation> {
    EntryLocation::decode(&value)
        .ok_or_else(|| Error::InvalidSuperblock("EntryIndex contains an invalid EntryLocation".to_string()))
}

fn overlay_mutation(state: &RuntimeState, key: KeyDigest) -> Option<Mutation> {
    state
        .active
        .get(&key)
        .copied()
        .or_else(|| state.frozen.as_ref().and_then(|frozen| frozen.get(&key)).copied())
}

fn advance_base_revision(state: &mut RuntimeState) {
    state.base_revision = state
        .base_revision
        .checked_add(1)
        .expect("EntryIndex base revision is exhausted");
}

fn encode_key(key: KeyDigest) -> [u8; 24] {
    *key.as_bytes()
}

fn key_hash(key: KeyDigest) -> u64 {
    XxHash3_64::oneshot_with_seed(HASH_SEED, &encode_key(key))
}

fn fixed_error(context: &'static str, error: fixed_lsm::Error) -> Error {
    match error {
        fixed_lsm::Error::InvalidOptions(reason) => Error::InvalidConfig(format!("fixed LSM {context}: {reason}")),
        fixed_lsm::Error::MissingDatabase(path) => Error::io(
            context,
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("FixedRecordLSM does not exist at {}", path.display()),
            ),
        ),
        fixed_lsm::Error::Corruption { path, reason } => {
            Error::InvalidSuperblock(format!("fixed LSM {context}: corrupt {}: {reason}", path.display()))
        }
        fixed_lsm::Error::Io {
            context: io_context,
            source,
        } => Error::io(io_context, source),
        error => Error::Index(format!("fixed LSM {context}: {error}")),
    }
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn read_lock<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(index: u64) -> KeyDigest {
        let mut bytes = [index as u8; 24];
        bytes[16..].copy_from_slice(&index.to_le_bytes());
        KeyDigest::new(bytes)
    }

    fn location(index: u64) -> EntryLocation {
        EntryLocation {
            data_offset: index,
            extent_generation: 1,
            stored_len: PAGE_SIZE as u32,
            content_digest: [index as u8; crate::format::CONTENT_DIGEST_SIZE],
            priority: CachePriority::Normal,
        }
    }

    fn create(root: &Path) -> EntryIndex {
        EntryIndex::create(root, 128, 1024 * 1024, 64 * 16, 1024 * 1024).unwrap()
    }

    #[test]
    fn frequency_sketch_tracks_live_cardinality_instead_of_layout_capacity() {
        assert_eq!(frequency_counters_for_entries(0), MIN_FREQUENCY_COUNTERS);
        assert_eq!(
            frequency_counters_for_entries((MIN_FREQUENCY_COUNTERS * 4) as u64),
            MIN_FREQUENCY_COUNTERS
        );
        assert_eq!(
            frequency_counters_for_entries((MIN_FREQUENCY_COUNTERS * 4 + 4) as u64),
            MIN_FREQUENCY_COUNTERS * 2
        );
        assert_eq!(frequency_counters_for_entries(u64::MAX), MAX_FREQUENCY_COUNTERS);
    }

    #[test]
    fn checkpoint_reopens_without_scanning_live_keys() {
        let directory = tempfile::tempdir().unwrap();
        let index = create(directory.path());
        let inserts = (0..32).map(|entry| (key(entry), location(entry))).collect::<Vec<_>>();
        index.insert_batch(&inserts).unwrap();
        index.checkpoint().unwrap();
        assert_eq!(index.stats().live_entries, 32);
        drop(index);

        let index = EntryIndex::open(directory.path(), 128, 1024 * 1024, 64 * 16, 1024 * 1024).unwrap();
        assert_eq!(index.stats().live_entries, 32);
        for entry in 0..32 {
            assert_eq!(index.peek(key(entry)).unwrap(), Some(location(entry)));
        }
    }

    #[test]
    fn memory_lookup_returns_locations_misses_and_unknown_sst_ranges() {
        let directory = tempfile::tempdir().unwrap();
        let index = create(directory.path());
        assert_eq!(index.lookup_memory(key(1)).unwrap(), EntryIndexMemoryLookup::Miss);

        index.insert_batch(&[(key(1), location(1))]).unwrap();
        assert_eq!(
            index.lookup_memory(key(1)).unwrap(),
            EntryIndexMemoryLookup::Location(location(1))
        );
        index.checkpoint().unwrap();
        assert_eq!(
            index.lookup_memory(key(1)).unwrap(),
            EntryIndexMemoryLookup::Location(location(1))
        );

        index.database.flush().unwrap();
        assert_eq!(index.lookup_memory(key(1)).unwrap(), EntryIndexMemoryLookup::Unknown);
        assert_eq!(index.lookup_memory(key(127)).unwrap(), EntryIndexMemoryLookup::Miss);
    }

    #[test]
    fn active_mutation_wins_over_inflight_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let index = create(directory.path());
        index.insert_batch(&[(key(1), location(1))]).unwrap();
        let checkpoint = index.prepare_checkpoint().unwrap().unwrap();
        let newer = EntryLocation {
            extent_generation: 2,
            ..location(1)
        };
        index.insert_batch(&[(key(1), newer)]).unwrap();
        index.persist_checkpoint(checkpoint).unwrap();
        assert_eq!(index.peek(key(1)).unwrap(), Some(newer));
        index.checkpoint().unwrap();
        drop(index);

        let index = EntryIndex::open(directory.path(), 128, 1024 * 1024, 64 * 16, 1024 * 1024).unwrap();
        assert_eq!(index.peek(key(1)).unwrap(), Some(newer));
    }

    #[test]
    fn lookup_retries_if_an_overlay_is_persisted_during_the_base_read() {
        let directory = tempfile::tempdir().unwrap();
        let index = create(directory.path());
        let expected = location(1);
        let mut injected = false;

        let actual = index
            .lookup_with(key(1), || {
                if !injected {
                    injected = true;
                    index.insert_batch(&[(key(1), expected)]).unwrap();
                    index.checkpoint().unwrap();
                    return Ok(None);
                }
                index
                    .database
                    .get(&encode_key(key(1)))
                    .map_err(|error| fixed_error("test lookup", error))
            })
            .unwrap();

        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn abort_restores_only_older_mutations() {
        let directory = tempfile::tempdir().unwrap();
        let index = create(directory.path());
        index.insert_batch(&[(key(1), location(1))]).unwrap();
        let checkpoint = index.prepare_checkpoint().unwrap().unwrap();
        let newer = EntryLocation {
            extent_generation: 2,
            ..location(1)
        };
        index.insert_batch(&[(key(1), newer)]).unwrap();
        index.abort_checkpoint(checkpoint);
        assert_eq!(index.peek(key(1)).unwrap(), Some(newer));
        index.checkpoint().unwrap();
    }
}
