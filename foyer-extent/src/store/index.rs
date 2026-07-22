use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use fixed_lsm::{FixedLsm, FixedLsmMemoryLookup, FixedLsmOptions, WriteBatch, WriteOptions};

#[cfg(test)]
use crate::format::PAGE_SIZE;
#[cfg(test)]
use crate::model::CachePriority;
use crate::{
    error::{Error, Result},
    model::KeyDigest,
    store::{
        format::EntryLocation,
        operation::{BatchInsertResult, InsertOutcome},
        pool::ExtentPool,
        stats::PhysicalWriteStats,
    },
};

pub(crate) const INDEX_DIRECTORY: &str = "index-lsm";

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EntryIndexStats {
    pub manifest_generation: u64,
    pub last_sequence: u64,
    pub indexed_entries_upper_bound: u64,
    pub pending_changes: u64,
    pub disk_capacity_bytes: u64,
    pub disk_used_bytes: u64,
    pub wal_bytes: u64,
    pub sst_files: u64,
    pub sst_bytes: u64,
    pub mutable_entries: u64,
    pub immutable_memtables: u64,
    pub cache_resident_bytes: u64,
    pub stale_location_checks: u64,
    pub stale_location_discards: u64,
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
    indexed_count: u64,
    base_revision: u64,
}

#[derive(Debug)]
pub struct IndexCheckpoint {
    frozen: Arc<HashMap<KeyDigest, Mutation>>,
    indexed_count: u64,
}

#[derive(Debug)]
pub struct EntryIndex {
    database: FixedLsm,
    state: RwLock<RuntimeState>,
    mutations: Mutex<()>,
}

impl EntryIndex {
    pub fn create(
        root: &Path,
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
        Ok(Self::from_parts(database))
    }

    pub fn open(root: &Path, capacity_bytes: u64, write_buffer_capacity: usize, cache_capacity: usize) -> Result<Self> {
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
        Ok(Self::from_parts(database))
    }

    fn from_parts(database: FixedLsm) -> Self {
        let indexed_count = database.user_state();
        Self {
            database,
            state: RwLock::new(RuntimeState {
                indexed_count,
                ..RuntimeState::default()
            }),
            mutations: Mutex::new(()),
        }
    }

    pub fn install_liveness_filter(&self, pool: Arc<ExtentPool>) {
        self.database
            .set_compaction_filter(Some(Arc::new(StaleLocationFilter { pool })));
    }

    pub fn allocated_size(&self) -> Result<u64> {
        Ok(self.database.disk_used_bytes())
    }

    pub fn physical_write_stats(&self) -> PhysicalWriteStats {
        let stats = self.database.stats();
        let index_runs = stats
            .wal_write_operations
            .saturating_add(stats.table_write_operations)
            .saturating_add(stats.manifest_write_operations);
        let index_bytes = stats
            .wal_write_bytes
            .saturating_add(stats.table_write_bytes)
            .saturating_add(stats.manifest_write_bytes);
        let index_syncs = stats
            .wal_sync_operations
            .saturating_add(stats.table_sync_operations)
            .saturating_add(stats.manifest_sync_operations);
        PhysicalWriteStats {
            index_runs,
            index_bytes,
            index_syncs,
            index_wal_runs: stats.wal_write_operations,
            index_wal_bytes: stats.wal_write_bytes,
            index_wal_syncs: stats.wal_sync_operations,
            index_sst_runs: stats.table_write_operations,
            index_sst_bytes: stats.table_write_bytes,
            index_sst_syncs: stats.table_sync_operations,
            index_manifest_runs: stats.manifest_write_operations,
            index_manifest_bytes: stats.manifest_write_bytes,
            index_manifest_syncs: stats.manifest_sync_operations,
            index_flushes: stats.flush_operations,
            index_compactions: stats.compaction_operations,
            index_compaction_input_bytes: stats.compaction_input_bytes,
            index_compaction_output_bytes: stats.compaction_output_bytes,
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
        let pending_changes = state
            .active
            .len()
            .saturating_add(state.frozen.as_ref().map_or(0, |frozen| frozen.len()))
            as u64;
        EntryIndexStats {
            manifest_generation: database.manifest_generation,
            last_sequence: database.next_sequence.saturating_sub(1),
            indexed_entries_upper_bound: state.indexed_count,
            pending_changes,
            disk_capacity_bytes: database.disk_capacity_bytes,
            disk_used_bytes: database.disk_used_bytes,
            wal_bytes: database.wal_bytes,
            sst_files: database.level_files.into_iter().sum(),
            sst_bytes: database.level_bytes.into_iter().sum(),
            mutable_entries: database.mutable_entries,
            immutable_memtables: database.immutable_memtables,
            cache_resident_bytes: database.cache_resident_bytes,
            stale_location_checks: database.compaction_filter_checks,
            stale_location_discards: database.compaction_filter_discards,
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
            // Cardinality is a soft upper bound, so classifying a write must never turn it into an
            // SST point read. Unknown means a table range may contain the key; conservatively
            // charge another indexed entry and let normal compaction reconcile stale locations.
            let existing = self.lookup_memory(key)?;
            if existing == EntryIndexMemoryLookup::Location(location) {
                outcomes.push(InsertOutcome::Updated);
                continue;
            }
            let mut state = write_lock(&self.state);
            state.active.insert(key, Mutation::insert(location));
            match existing {
                EntryIndexMemoryLookup::Location(_) => outcomes.push(InsertOutcome::Updated),
                EntryIndexMemoryLookup::Miss | EntryIndexMemoryLookup::Unknown => {
                    state.indexed_count = state.indexed_count.saturating_add(1);
                    outcomes.push(InsertOutcome::Inserted);
                }
            }
        }
        drop(mutation);
        Ok(BatchInsertResult {
            outcomes,
            ..BatchInsertResult::default()
        })
    }

    pub fn remove_batch(&self, keys: &[KeyDigest]) -> Result<usize> {
        let mutation = mutex_lock(&self.mutations);
        let mut removed = 0;
        for key in keys.iter().copied() {
            // A possible SST value is hidden with a tombstone without reading it. Keep the
            // cardinality charge when presence is unknown so the persisted count remains an upper
            // bound rather than an admission-critical exact value.
            let existing = self.lookup_memory(key)?;
            if existing == EntryIndexMemoryLookup::Miss {
                continue;
            }
            let mut state = write_lock(&self.state);
            state.active.insert(key, Mutation::remove());
            if matches!(existing, EntryIndexMemoryLookup::Location(_)) {
                state.indexed_count = state.indexed_count.saturating_sub(1);
            }
            removed += 1;
        }
        drop(mutation);
        Ok(removed)
    }

    #[cfg(test)]
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
            indexed_count: state.indexed_count,
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
            .write_with_user_state(&batch, WriteOptions::sync(), checkpoint.indexed_count)
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
}

#[derive(Debug)]
struct StaleLocationFilter {
    pool: Arc<ExtentPool>,
}

impl fixed_lsm::CompactionFilter for StaleLocationFilter {
    fn should_discard(&self, _key: &fixed_lsm::Key, value: &fixed_lsm::Value) -> bool {
        EntryLocation::decode(value).is_some_and(|location| !self.pool.location_is_live(location))
    }
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
        EntryIndex::create(root, 1024 * 1024, 64 * 16, 1024 * 1024).unwrap()
    }

    #[test]
    fn checkpoint_reopens_without_scanning_live_keys() {
        let directory = tempfile::tempdir().unwrap();
        let index = create(directory.path());
        let inserts = (0..32).map(|entry| (key(entry), location(entry))).collect::<Vec<_>>();
        index.insert_batch(&inserts).unwrap();
        index.checkpoint().unwrap();
        assert_eq!(index.stats().indexed_entries_upper_bound, 32);
        drop(index);

        let index = EntryIndex::open(directory.path(), 1024 * 1024, 64 * 16, 1024 * 1024).unwrap();
        assert_eq!(index.stats().indexed_entries_upper_bound, 32);
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
    fn sst_mutations_keep_cardinality_soft_without_point_reads() {
        let directory = tempfile::tempdir().unwrap();
        let index = create(directory.path());
        index
            .insert_batch(&[(key(1), location(1)), (key(2), location(2))])
            .unwrap();
        index.checkpoint().unwrap();
        index.database.flush().unwrap();
        assert_eq!(index.lookup_memory(key(1)).unwrap(), EntryIndexMemoryLookup::Unknown);
        assert_eq!(index.lookup_memory(key(2)).unwrap(), EntryIndexMemoryLookup::Unknown);

        let before = index.io_read_stats();
        let newer = EntryLocation {
            extent_generation: 2,
            ..location(1)
        };
        assert_eq!(
            index.insert_batch(&[(key(1), newer)]).unwrap().outcomes,
            vec![InsertOutcome::Inserted]
        );
        assert_eq!(index.remove_batch(&[key(2)]).unwrap(), 1);
        assert_eq!(index.io_read_stats(), before);
        assert_eq!(index.stats().indexed_entries_upper_bound, 3);
        assert_eq!(index.peek(key(1)).unwrap(), Some(newer));
        assert_eq!(index.peek(key(2)).unwrap(), None);
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

        let index = EntryIndex::open(directory.path(), 1024 * 1024, 64 * 16, 1024 * 1024).unwrap();
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
