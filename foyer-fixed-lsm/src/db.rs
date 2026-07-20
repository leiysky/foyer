use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    fs,
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
        mpsc,
    },
    thread::JoinHandle,
};

use fs4::fs_std::FileExt;

use crate::{
    cache::BlockCache,
    error::{Error, Result},
    format::{Key, MAX_SEQUENCE, RECORD_SIZE, Record, Value},
    manifest::{Manifest, ManifestTable},
    space::{DiskBudget, DiskReservation},
    table::{Table, TableIoCounters, TableIterator, parse_table_file_name, table_file_size, table_path},
    wal::{
        Wal, cleanup_empty_wal_files, cleanup_wal_files, cleanup_wal_files_through, replay_all, sync_all_wal_files,
        total_wal_bytes,
    },
};

const LEVEL_COUNT: usize = 7;
const L0_COMPACTION_TRIGGER: usize = 4;
const LEVEL_SIZE_MULTIPLIER: u64 = 10;
const DEFAULT_WRITE_BUFFER_CAPACITY: usize = 64 * 1024 * 1024;
const DEFAULT_CACHE_CAPACITY: usize = 1024 * 1024 * 1024;
const MAX_PENDING_FLUSHES: usize = 2;
const MINIMUM_OUTPUT_FILL_DIVISOR: usize = 4;
const MEMTABLE_TOMBSTONE_BIT: u64 = 1_u64 << 63;
const LOCK_FILE: &str = "LOCK";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedLsmOptions {
    pub write_buffer_capacity: usize,
    pub cache_capacity: usize,
    /// Soft capacity target for all files in the database directory, including transient WAL,
    /// flush, and compaction output. Usage is accounted beyond this target without rejecting I/O.
    pub max_disk_bytes: u64,
}

impl Default for FixedLsmOptions {
    fn default() -> Self {
        Self {
            write_buffer_capacity: DEFAULT_WRITE_BUFFER_CAPACITY,
            cache_capacity: DEFAULT_CACHE_CAPACITY,
            max_disk_bytes: u64::MAX,
        }
    }
}

impl FixedLsmOptions {
    fn validate(self) -> Result<Self> {
        if self.write_buffer_capacity < RECORD_SIZE {
            return Err(Error::InvalidOptions(format!(
                "write_buffer_capacity must be at least {RECORD_SIZE} bytes"
            )));
        }
        if self.max_disk_bytes == 0 {
            return Err(Error::InvalidOptions(
                "max_disk_bytes must be greater than zero".to_string(),
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    #[default]
    Buffered,
    Sync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteOptions {
    durability: Durability,
    wal: bool,
}

impl WriteOptions {
    pub const fn buffered() -> Self {
        Self {
            durability: Durability::Buffered,
            wal: true,
        }
    }

    pub const fn sync() -> Self {
        Self {
            durability: Durability::Sync,
            wal: true,
        }
    }

    pub const fn bulk_load() -> Self {
        Self {
            durability: Durability::Buffered,
            wal: false,
        }
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self::buffered()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchMutation {
    Put(Key, Value),
    Delete(Key),
}

#[derive(Debug, Default)]
pub struct WriteBatch {
    mutations: Vec<BatchMutation>,
}

impl WriteBatch {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            mutations: Vec::with_capacity(capacity),
        }
    }

    pub fn put(&mut self, key: Key, value: Value) {
        self.mutations.push(BatchMutation::Put(key, value));
    }

    pub fn delete(&mut self, key: Key) {
        self.mutations.push(BatchMutation::Delete(key));
    }

    pub fn len(&self) -> usize {
        self.mutations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FixedLsmStats {
    pub manifest_generation: u64,
    pub next_sequence: u64,
    pub writes: u64,
    pub user_state: u64,
    pub mutable_entries: u64,
    pub mutable_bytes: u64,
    pub immutable_memtables: u64,
    pub background_running: bool,
    pub background_failed: bool,
    pub disk_capacity_bytes: u64,
    pub disk_used_bytes: u64,
    pub wal_bytes: u64,
    pub wal_write_operations: u64,
    pub wal_write_bytes: u64,
    pub recovered_records: u64,
    pub discarded_wal_tail_bytes: u64,
    pub level_files: [u64; LEVEL_COUNT],
    pub level_bytes: [u64; LEVEL_COUNT],
    pub level_tombstones: [u64; LEVEL_COUNT],
    pub base_level: u32,
    pub level_targets: [u64; LEVEL_COUNT],
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_resident_bytes: u64,
    pub cache_data_resident_bytes: u64,
    pub cache_metadata_resident_bytes: u64,
    pub table_read_operations: u64,
    pub table_read_bytes: u64,
    pub table_write_operations: u64,
    pub table_write_bytes: u64,
    pub point_filter_checks: u64,
    pub point_filter_positives: u64,
    pub point_data_cache_hits: u64,
    pub point_data_reads: u64,
    pub point_false_positives: u64,
    pub flush_operations: u64,
    pub flush_output_bytes: u64,
    pub compaction_operations: u64,
    pub compaction_input_bytes: u64,
    pub compaction_output_bytes: u64,
    pub trivial_move_operations: u64,
}

/// Lightweight cumulative table-read counters.
///
/// Unlike [`FixedLsm::stats`], this snapshot is O(1): it does not inspect WAL files or lock the
/// writer, version, page-cache shards, or background-maintenance state.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FixedLsmReadStats {
    pub read_operations: u64,
    pub read_bytes: u64,
}

/// Result of a point lookup that is guaranteed not to perform table I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixedLsmMemoryLookup {
    Value(Value),
    Miss,
    Unknown,
}

#[derive(Debug, Default)]
struct MaintenanceCounters {
    flush_operations: AtomicU64,
    flush_output_bytes: AtomicU64,
    compaction_operations: AtomicU64,
    compaction_input_bytes: AtomicU64,
    compaction_output_bytes: AtomicU64,
    trivial_move_operations: AtomicU64,
}

impl MaintenanceCounters {
    fn record_flush(&self, output_bytes: u64) {
        self.flush_operations.fetch_add(1, AtomicOrdering::Relaxed);
        self.flush_output_bytes.fetch_add(output_bytes, AtomicOrdering::Relaxed);
    }

    fn record_compaction(&self, input_bytes: u64, output_bytes: u64, trivial_move: bool) {
        self.compaction_operations.fetch_add(1, AtomicOrdering::Relaxed);
        self.compaction_input_bytes
            .fetch_add(input_bytes, AtomicOrdering::Relaxed);
        self.compaction_output_bytes
            .fetch_add(output_bytes, AtomicOrdering::Relaxed);
        if trivial_move {
            self.trivial_move_operations.fetch_add(1, AtomicOrdering::Relaxed);
        }
    }
}

#[derive(Debug, Default)]
struct MemTableData {
    entries: HashMap<Key, MemValue>,
    mutation_bytes: u64,
    max_sequence: u64,
    user_state: u64,
}

#[derive(Debug, Clone, Copy)]
struct MemValue {
    value: Value,
    sequence_word: u64,
}

impl MemValue {
    fn from_record(record: Record) -> Self {
        Self {
            value: record.value.unwrap_or_default(),
            sequence_word: record.sequence | (u64::from(record.value.is_none()) * MEMTABLE_TOMBSTONE_BIT),
        }
    }

    fn record(self, key: Key) -> Record {
        Record {
            key,
            value: (self.sequence_word & MEMTABLE_TOMBSTONE_BIT == 0).then_some(self.value),
            sequence: self.sequence_word & MAX_SEQUENCE,
        }
    }
}

#[derive(Debug, Default)]
struct MemTable {
    data: RwLock<MemTableData>,
}

impl MemTable {
    fn get(&self, key: &Key) -> Option<Record> {
        rwlock_read(&self.data)
            .entries
            .get(key)
            .copied()
            .map(|entry| entry.record(*key))
    }

    fn apply(&self, records: &[Record], user_state: u64) {
        let mut data = rwlock_write(&self.data);
        for record in records {
            data.entries.insert(record.key, MemValue::from_record(*record));
            data.max_sequence = data.max_sequence.max(record.sequence);
        }
        data.mutation_bytes = data.mutation_bytes.saturating_add((records.len() * RECORD_SIZE) as u64);
        data.user_state = user_state;
    }

    fn sorted_records(&self) -> (Vec<Record>, u64, u64) {
        let data = rwlock_read(&self.data);
        let mut records = data
            .entries
            .iter()
            .map(|(key, entry)| entry.record(*key))
            .collect::<Vec<_>>();
        records.sort_unstable_by_key(|record| record.key);
        (records, data.max_sequence, data.user_state)
    }

    fn stats(&self) -> (u64, u64) {
        let data = rwlock_read(&self.data);
        (data.entries.len() as u64, data.mutation_bytes)
    }
}

#[derive(Debug)]
struct Version {
    levels: [Vec<Arc<Table>>; LEVEL_COUNT],
}

impl Version {
    fn empty() -> Self {
        Self {
            levels: std::array::from_fn(|_| Vec::new()),
        }
    }

    fn may_contain(&self, key: &Key) -> bool {
        self.levels.iter().flatten().any(|table| table.contains_range(key))
    }

    fn from_tables(tables: Vec<Arc<Table>>) -> Result<Self> {
        let mut version = Self::empty();
        for table in tables {
            let level = table.meta().level as usize;
            if level >= LEVEL_COUNT {
                return Err(Error::corruption(
                    table_path(Path::new("."), table.meta().file_id),
                    "SST level is outside the configured level count",
                ));
            }
            version.levels[level].push(table);
        }
        version.sort_and_validate()?;
        Ok(version)
    }

    fn get(&self, key: &Key) -> Result<Option<Record>> {
        // Flush sequence ranges order L0 newest-first. Every later compaction removes an upper
        // input only after merging it into the next level, so levels are searched newest-first and
        // the first exact hit (including a tombstone) is authoritative.
        for table in &self.levels[0] {
            if let Some(record) = table.get(key)? {
                return Ok(Some(record));
            }
        }
        for level in 1..LEVEL_COUNT {
            let tables = &self.levels[level];
            let candidate = tables.partition_point(|table| table.meta().largest < *key);
            if let Some(table) = tables.get(candidate)
                && table.contains_range(key)
                && let Some(record) = table.get(key)?
            {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    fn with_l0(&self, table: Arc<Table>) -> Result<Self> {
        let mut levels = self.levels.clone();
        levels[0].push(table);
        let mut version = Self { levels };
        version.sort_and_validate()?;
        Ok(version)
    }

    fn replace(&self, removed: &HashSet<u64>, outputs: Vec<Arc<Table>>) -> Result<Self> {
        let mut levels = std::array::from_fn(|_| Vec::new());
        for (level, tables) in self.levels.iter().enumerate() {
            for table in tables {
                if !removed.contains(&table.meta().file_id) {
                    levels[level].push(table.clone());
                }
            }
        }
        for table in outputs {
            levels[table.meta().level as usize].push(table);
        }
        let mut version = Self { levels };
        version.sort_and_validate()?;
        Ok(version)
    }

    fn manifest_tables(&self) -> Vec<ManifestTable> {
        let mut tables = Vec::new();
        for level in &self.levels {
            for table in level {
                tables.push(ManifestTable {
                    file_id: table.meta().file_id,
                    level: table.meta().level,
                });
            }
        }
        tables
    }

    fn stats(&self) -> ([u64; LEVEL_COUNT], [u64; LEVEL_COUNT], [u64; LEVEL_COUNT]) {
        let mut files = [0; LEVEL_COUNT];
        let mut bytes = [0; LEVEL_COUNT];
        let mut tombstones = [0; LEVEL_COUNT];
        for (level, tables) in self.levels.iter().enumerate() {
            files[level] = tables.len() as u64;
            bytes[level] = tables.iter().map(|table| table.meta().file_size).sum();
            tombstones[level] = tables.iter().map(|table| table.meta().tombstone_count).sum();
        }
        (files, bytes, tombstones)
    }

    fn sort_and_validate(&mut self) -> Result<()> {
        self.levels[0].sort_unstable_by_key(|table| std::cmp::Reverse(table.meta().max_sequence));
        for level in 1..LEVEL_COUNT {
            self.levels[level].sort_unstable_by_key(|table| table.meta().smallest);
            for pair in self.levels[level].windows(2) {
                if pair[0].meta().largest >= pair[1].meta().smallest {
                    return Err(Error::corruption(
                        table_path(Path::new("."), pair[1].meta().file_id),
                        format!("overlapping SST ranges in level {level}"),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ReadState {
    mutable: Arc<MemTable>,
    immutables: Vec<Arc<MemTable>>,
    version: Arc<Version>,
}

#[derive(Debug)]
struct WriterState {
    manifest: Manifest,
    next_sequence: u64,
    next_wal_id: u64,
    user_state: u64,
    wal: Wal,
}

#[derive(Debug)]
struct Inner {
    directory: PathBuf,
    options: FixedLsmOptions,
    disk_budget: DiskBudget,
    _lock: File,
    cache: Arc<BlockCache>,
    io: Arc<TableIoCounters>,
    maintenance_counters: MaintenanceCounters,
    state: RwLock<ReadState>,
    writer: Mutex<WriterState>,
    maintenance: Mutex<()>,
    next_file_id: AtomicU64,
    recovered_records: u64,
    discarded_wal_tail_bytes: u64,
    writes: AtomicU64,
    wal_write_operations: AtomicU64,
    wal_write_bytes: AtomicU64,
}

#[derive(Debug, Clone)]
struct FlushTask {
    frozen: Arc<MemTable>,
    table_id: u64,
    wal_id: u64,
}

#[derive(Debug, Default)]
struct BackgroundState {
    pending_flushes: usize,
    busy: bool,
    failure: Option<String>,
}

#[derive(Debug, Default)]
struct BackgroundStatus {
    state: Mutex<BackgroundState>,
    changed: Condvar,
}

#[derive(Debug)]
pub struct FixedLsm {
    inner: Arc<Inner>,
    background: Arc<BackgroundStatus>,
    flush_sender: Option<mpsc::Sender<FlushTask>>,
    worker: Option<JoinHandle<()>>,
}

impl FixedLsm {
    pub fn create(path: impl AsRef<Path>, options: FixedLsmOptions) -> Result<Self> {
        let directory = path.as_ref();
        let options = options.validate()?;
        fs::create_dir_all(directory).map_err(|error| Error::io("create database directory", error))?;
        let lock = lock_database(directory)?;
        if directory_has_database_files(directory)? {
            return Err(Error::AlreadyExists(directory.to_path_buf()));
        }
        let manifest = Manifest::initial();
        manifest.persist(directory, LEVEL_COUNT)?;
        let wal = Wal::create(directory, 1)?;
        let disk_budget = DiskBudget::new(options.max_disk_bytes, database_storage_bytes(directory)?);
        let cache = Arc::new(BlockCache::new(options.cache_capacity));
        let io = Arc::new(TableIoCounters::default());
        Self::from_inner(Inner {
            directory: directory.to_path_buf(),
            options,
            disk_budget,
            _lock: lock,
            cache,
            io,
            maintenance_counters: MaintenanceCounters::default(),
            state: RwLock::new(ReadState {
                mutable: Arc::new(MemTable::default()),
                immutables: Vec::new(),
                version: Arc::new(Version::empty()),
            }),
            writer: Mutex::new(WriterState {
                manifest,
                next_sequence: 1,
                next_wal_id: 2,
                user_state: 0,
                wal,
            }),
            maintenance: Mutex::new(()),
            next_file_id: AtomicU64::new(1),
            recovered_records: 0,
            discarded_wal_tail_bytes: 0,
            writes: AtomicU64::new(0),
            wal_write_operations: AtomicU64::new(0),
            wal_write_bytes: AtomicU64::new(0),
        })
    }

    pub fn open(path: impl AsRef<Path>, options: FixedLsmOptions) -> Result<Self> {
        let directory = path.as_ref();
        let options = options.validate()?;
        if !directory.is_dir() {
            return Err(Error::MissingDatabase(directory.to_path_buf()));
        }
        let lock = lock_database(directory)?;
        let manifest = Manifest::load(directory, LEVEL_COUNT)?;
        let cache = Arc::new(BlockCache::new(options.cache_capacity));
        let io = Arc::new(TableIoCounters::default());
        let mut tables = Vec::with_capacity(manifest.tables.len());
        for table in &manifest.tables {
            tables.push(Table::open(
                directory,
                table.file_id,
                table.level,
                cache.clone(),
                io.clone(),
            )?);
        }
        let version = Arc::new(Version::from_tables(tables)?);
        let recovered = replay_all(directory, manifest.flushed_sequence)?;
        let mut mutable = MemTableData::default();
        for record in &recovered.records {
            mutable.entries.insert(record.key, MemValue::from_record(*record));
            mutable.max_sequence = mutable.max_sequence.max(record.sequence);
            mutable.mutation_bytes = mutable.mutation_bytes.saturating_add(RECORD_SIZE as u64);
        }
        let next_sequence = manifest.next_sequence.max(recovered.max_sequence.saturating_add(1));
        let user_state = recovered.user_state.unwrap_or(manifest.user_state);
        mutable.user_state = user_state;
        let wal_id = recovered
            .highest_wal_id
            .checked_add(1)
            .filter(|id| *id > 0)
            .ok_or_else(|| Error::corruption(directory, "WAL id space is exhausted"))?;
        let wal = Wal::create(directory, wal_id)?;
        if recovered.records.is_empty() {
            let _ = cleanup_wal_files(directory, wal_id)?;
        } else {
            cleanup_empty_wal_files(directory, wal_id)?;
        }
        let maximum_file_id = maximum_table_file_id(directory)?;
        let next_file_id = manifest.next_file_id.max(maximum_file_id.saturating_add(1));
        let mut manifest = manifest;
        manifest.next_file_id = next_file_id;
        manifest.next_sequence = next_sequence;
        cleanup_orphan_tables(directory, &version)?;
        Manifest::cleanup_temporary_files(directory)?;
        let disk_budget = DiskBudget::new(options.max_disk_bytes, database_storage_bytes(directory)?);
        Self::from_inner(Inner {
            directory: directory.to_path_buf(),
            options,
            disk_budget,
            _lock: lock,
            cache,
            io,
            maintenance_counters: MaintenanceCounters::default(),
            state: RwLock::new(ReadState {
                mutable: Arc::new(MemTable {
                    data: RwLock::new(mutable),
                }),
                immutables: Vec::new(),
                version,
            }),
            writer: Mutex::new(WriterState {
                manifest,
                next_sequence,
                next_wal_id: wal_id
                    .checked_add(1)
                    .ok_or_else(|| Error::corruption(directory, "WAL id space is exhausted"))?,
                user_state,
                wal,
            }),
            maintenance: Mutex::new(()),
            next_file_id: AtomicU64::new(next_file_id),
            recovered_records: recovered.records.len() as u64,
            discarded_wal_tail_bytes: recovered.discarded_tail_bytes,
            writes: AtomicU64::new(0),
            wal_write_operations: AtomicU64::new(0),
            wal_write_bytes: AtomicU64::new(0),
        })
    }

    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        self.check_background_failure()?;
        let (mutable, immutables, version) = {
            let state = rwlock_read(&self.inner.state);
            (state.mutable.clone(), state.immutables.clone(), state.version.clone())
        };
        if let Some(record) = mutable.get(key) {
            return Ok(record.value);
        }
        for immutable in immutables.iter().rev() {
            if let Some(record) = immutable.get(key) {
                return Ok(record.value);
            }
        }
        Ok(version.get(key)?.and_then(|record| record.value))
    }

    /// Probe memtables and in-memory table ranges without issuing table I/O.
    pub fn probe_memory(&self, key: &Key) -> Result<FixedLsmMemoryLookup> {
        self.check_background_failure()?;
        let (mutable, immutables, version) = {
            let state = rwlock_read(&self.inner.state);
            (state.mutable.clone(), state.immutables.clone(), state.version.clone())
        };
        if let Some(record) = mutable.get(key) {
            return Ok(record
                .value
                .map_or(FixedLsmMemoryLookup::Miss, FixedLsmMemoryLookup::Value));
        }
        for immutable in immutables.iter().rev() {
            if let Some(record) = immutable.get(key) {
                return Ok(record
                    .value
                    .map_or(FixedLsmMemoryLookup::Miss, FixedLsmMemoryLookup::Value));
            }
        }
        Ok(if version.may_contain(key) {
            FixedLsmMemoryLookup::Unknown
        } else {
            FixedLsmMemoryLookup::Miss
        })
    }

    pub fn write(&self, batch: &WriteBatch, options: WriteOptions) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        self.check_background_failure()?;
        let wal_reservation = options.wal.then(|| self.reserve_wal(batch.len())).transpose()?;
        let mutation_bytes = {
            let mut writer = mutex_lock(&self.inner.writer);
            let records = encode_batch(batch, writer.next_sequence)?;
            if options.wal {
                // A failed append may have reached the file partially. Keep the reservation
                // conservatively; the database reports the I/O error and reopen recounts files.
                wal_reservation.unwrap().commit();
                let user_state = writer.user_state;
                let bytes = writer
                    .wal
                    .append(&records, user_state, options.durability == Durability::Sync)?;
                self.record_wal_write(bytes);
            }
            let mutable = rwlock_read(&self.inner.state).mutable.clone();
            mutable.apply(&records, writer.user_state);
            writer.next_sequence = records.last().unwrap().sequence.saturating_add(1);
            self.inner
                .writes
                .fetch_add(records.len() as u64, AtomicOrdering::Relaxed);
            mutable.stats().1
        };
        self.maybe_schedule_flush(mutation_bytes)
    }

    /// Applies a batch and atomically advances an application-defined durable cursor.
    ///
    /// The cursor is replayed with the batch from WAL and copied into the manifest when the
    /// containing memtable is flushed. It is intentionally opaque to the engine.
    pub fn write_with_user_state(&self, batch: &WriteBatch, options: WriteOptions, user_state: u64) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        self.check_background_failure()?;
        let wal_reservation = options.wal.then(|| self.reserve_wal(batch.len())).transpose()?;
        let mutation_bytes = {
            let mut writer = mutex_lock(&self.inner.writer);
            let records = encode_batch(batch, writer.next_sequence)?;
            if options.wal {
                // A failed append may have reached the file partially. Keep the reservation
                // conservatively; reopen reconciles the exact on-disk usage.
                wal_reservation.unwrap().commit();
                let bytes = writer
                    .wal
                    .append(&records, user_state, options.durability == Durability::Sync)?;
                self.record_wal_write(bytes);
            }
            let mutable = rwlock_read(&self.inner.state).mutable.clone();
            mutable.apply(&records, user_state);
            writer.next_sequence = records.last().unwrap().sequence.saturating_add(1);
            writer.user_state = user_state;
            self.inner
                .writes
                .fetch_add(records.len() as u64, AtomicOrdering::Relaxed);
            mutable.stats().1
        };
        self.maybe_schedule_flush(mutation_bytes)
    }

    pub fn user_state(&self) -> u64 {
        mutex_lock(&self.inner.writer).user_state
    }

    /// Return the bytes currently charged to the database disk budget.
    ///
    /// Unlike [`Self::stats`], this is an O(1) snapshot and does not inspect WAL files or lock the
    /// writer, version, page-cache, or background-maintenance state.
    pub fn disk_used_bytes(&self) -> u64 {
        self.inner.disk_budget.used()
    }

    /// Return cumulative physical table-read counters without collecting a full database snapshot.
    pub fn read_stats(&self) -> FixedLsmReadStats {
        let io = self.inner.io.snapshot();
        FixedLsmReadStats {
            read_operations: io.read_operations,
            read_bytes: io.read_bytes,
        }
    }

    pub fn put(&self, key: Key, value: Value, options: WriteOptions) -> Result<()> {
        self.write_single(BatchMutation::Put(key, value), options)
    }

    pub fn delete(&self, key: Key, options: WriteOptions) -> Result<()> {
        self.write_single(BatchMutation::Delete(key), options)
    }

    pub fn sync_wal(&self) -> Result<()> {
        self.check_background_failure()?;
        let _writer = mutex_lock(&self.inner.writer);
        sync_all_wal_files(&self.inner.directory)
    }

    pub fn ensure_healthy(&self) -> Result<()> {
        self.check_background_failure()
    }

    pub fn wait_for_maintenance(&self) -> Result<()> {
        self.wait_background()
    }

    pub fn flush(&self) -> Result<()> {
        self.wait_background()?;
        if self.schedule_flush(true)? {
            self.wait_background()?;
        }
        Ok(())
    }

    pub fn compact(&self) -> Result<()> {
        self.flush()?;
        let _maintenance = mutex_lock(&self.inner.maintenance);
        compact_until_stable(&self.inner, true, None)
    }

    pub fn stats(&self) -> FixedLsmStats {
        let background = mutex_lock(&self.background.state);
        let background_running = background.busy || background.pending_flushes > 0;
        let background_failed = background.failure.is_some();
        drop(background);
        let writer = mutex_lock(&self.inner.writer);
        let state = rwlock_read(&self.inner.state);
        let (mutable_entries, mutable_bytes) = state.mutable.stats();
        let (level_files, level_bytes, level_tombstones) = state.version.stats();
        let (base_level, level_targets) = dynamic_level_targets(&state.version, self.inner.options);
        let cache = self.inner.cache.stats();
        let io = self.inner.io.snapshot();
        FixedLsmStats {
            manifest_generation: writer.manifest.generation,
            next_sequence: writer.next_sequence,
            writes: self.inner.writes.load(AtomicOrdering::Relaxed),
            user_state: writer.user_state,
            mutable_entries,
            mutable_bytes,
            immutable_memtables: state.immutables.len() as u64,
            background_running,
            background_failed,
            disk_capacity_bytes: self.inner.disk_budget.capacity(),
            disk_used_bytes: self.inner.disk_budget.used(),
            wal_bytes: total_wal_bytes(&self.inner.directory),
            wal_write_operations: self.inner.wal_write_operations.load(AtomicOrdering::Relaxed),
            wal_write_bytes: self.inner.wal_write_bytes.load(AtomicOrdering::Relaxed),
            recovered_records: self.inner.recovered_records,
            discarded_wal_tail_bytes: self.inner.discarded_wal_tail_bytes,
            level_files,
            level_bytes,
            level_tombstones,
            base_level: base_level as u32,
            level_targets,
            cache_hits: cache.hits,
            cache_misses: cache.misses,
            cache_resident_bytes: cache.resident_bytes,
            cache_data_resident_bytes: cache.data_resident_bytes,
            cache_metadata_resident_bytes: cache.metadata_resident_bytes,
            table_read_operations: io.read_operations,
            table_read_bytes: io.read_bytes,
            table_write_operations: io.write_operations,
            table_write_bytes: io.write_bytes,
            point_filter_checks: cache.filter_accesses,
            point_filter_positives: cache.filter_positives,
            point_data_cache_hits: cache.data_hits,
            point_data_reads: io.point_data_reads,
            point_false_positives: io.point_false_positives,
            flush_operations: self
                .inner
                .maintenance_counters
                .flush_operations
                .load(AtomicOrdering::Relaxed),
            flush_output_bytes: self
                .inner
                .maintenance_counters
                .flush_output_bytes
                .load(AtomicOrdering::Relaxed),
            compaction_operations: self
                .inner
                .maintenance_counters
                .compaction_operations
                .load(AtomicOrdering::Relaxed),
            compaction_input_bytes: self
                .inner
                .maintenance_counters
                .compaction_input_bytes
                .load(AtomicOrdering::Relaxed),
            compaction_output_bytes: self
                .inner
                .maintenance_counters
                .compaction_output_bytes
                .load(AtomicOrdering::Relaxed),
            trivial_move_operations: self
                .inner
                .maintenance_counters
                .trivial_move_operations
                .load(AtomicOrdering::Relaxed),
        }
    }

    fn from_inner(inner: Inner) -> Result<Self> {
        let inner = Arc::new(inner);
        let background = Arc::new(BackgroundStatus::default());
        let (flush_sender, flush_receiver) = mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("fixed-lsm-maintenance".to_string())
            .spawn({
                let inner = inner.clone();
                let background = background.clone();
                let panic_status = background.clone();
                move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        background_worker(inner, background, flush_receiver);
                    }));
                    if result.is_err() {
                        record_background_failure(&panic_status, "maintenance worker panicked".to_string());
                    }
                }
            })
            .map_err(|error| Error::io("spawn fixed-lsm maintenance worker", error))?;
        Ok(Self {
            inner,
            background,
            flush_sender: Some(flush_sender),
            worker: Some(worker),
        })
    }

    fn write_single(&self, mutation: BatchMutation, options: WriteOptions) -> Result<()> {
        self.check_background_failure()?;
        let wal_reservation = options.wal.then(|| self.reserve_wal(1)).transpose()?;
        let mutation_bytes = {
            let mut writer = mutex_lock(&self.inner.writer);
            if writer.next_sequence == 0 || writer.next_sequence > MAX_SEQUENCE {
                return Err(Error::SequenceExhausted);
            }
            let record = match mutation {
                BatchMutation::Put(key, value) => Record::put(key, value, writer.next_sequence),
                BatchMutation::Delete(key) => Record::delete(key, writer.next_sequence),
            };
            if options.wal {
                // See the batched write path: retain capacity after an ambiguous append error.
                wal_reservation.unwrap().commit();
                let user_state = writer.user_state;
                let bytes = writer.wal.append(
                    std::slice::from_ref(&record),
                    user_state,
                    options.durability == Durability::Sync,
                )?;
                self.record_wal_write(bytes);
            }
            let mutable = rwlock_read(&self.inner.state).mutable.clone();
            mutable.apply(std::slice::from_ref(&record), writer.user_state);
            writer.next_sequence = record.sequence.saturating_add(1);
            self.inner.writes.fetch_add(1, AtomicOrdering::Relaxed);
            mutable.stats().1
        };
        self.maybe_schedule_flush(mutation_bytes)
    }

    fn reserve_wal(&self, record_count: usize) -> Result<DiskReservation<'_>> {
        let bytes = Wal::frame_size(record_count)?;
        self.inner.disk_budget.reserve(bytes)
    }

    fn record_wal_write(&self, bytes: u64) {
        self.inner.wal_write_operations.fetch_add(1, AtomicOrdering::Relaxed);
        self.inner.wal_write_bytes.fetch_add(bytes, AtomicOrdering::Relaxed);
    }

    fn maybe_schedule_flush(&self, mutation_bytes: u64) -> Result<()> {
        if mutation_bytes < self.inner.options.write_buffer_capacity as u64 {
            return Ok(());
        }
        if self.schedule_flush(false)? {
            return Ok(());
        }
        let backpressure = (self.inner.options.write_buffer_capacity as u64).saturating_mul(2);
        if mutation_bytes >= backpressure {
            self.wait_for_flush_capacity()?;
            self.schedule_flush(false)?;
        }
        Ok(())
    }

    fn schedule_flush(&self, force: bool) -> Result<bool> {
        self.check_background_failure()?;
        let task = {
            let mut writer = mutex_lock(&self.inner.writer);
            if mutex_lock(&self.background.state).pending_flushes >= MAX_PENDING_FLUSHES {
                return Ok(false);
            }
            let mutable = rwlock_read(&self.inner.state).mutable.clone();
            let (entries, mutation_bytes) = mutable.stats();
            if entries == 0 || !force && mutation_bytes < self.inner.options.write_buffer_capacity as u64 {
                return Ok(false);
            }
            let frozen_wal_id = writer.wal.id();
            let next_wal_id = writer.next_wal_id;
            let next_wal = Wal::create(&self.inner.directory, next_wal_id)?;
            writer.next_wal_id = next_wal_id
                .checked_add(1)
                .ok_or_else(|| Error::corruption(&self.inner.directory, "WAL id space exhausted"))?;
            writer.wal = next_wal;
            let table_id = allocate_file_id(&self.inner)?;
            let frozen = {
                let mut state = rwlock_write(&self.inner.state);
                let frozen = state.mutable.clone();
                state.immutables.push(frozen.clone());
                state.mutable = Arc::new(MemTable::default());
                frozen
            };
            let mut background = mutex_lock(&self.background.state);
            background.pending_flushes += 1;
            FlushTask {
                frozen,
                table_id,
                wal_id: frozen_wal_id,
            }
        };
        if self.flush_sender.as_ref().unwrap().send(task).is_err() {
            let reason = "maintenance worker stopped before accepting a flush".to_string();
            let mut background = mutex_lock(&self.background.state);
            background.pending_flushes = background.pending_flushes.saturating_sub(1);
            background.failure = Some(reason.clone());
            self.background.changed.notify_all();
            return Err(Error::Background(reason));
        }
        Ok(true)
    }

    fn wait_for_flush_capacity(&self) -> Result<()> {
        let mut background = mutex_lock(&self.background.state);
        while background.pending_flushes >= MAX_PENDING_FLUSHES && background.failure.is_none() {
            background = self
                .background
                .changed
                .wait(background)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        background
            .failure
            .clone()
            .map_or(Ok(()), |reason| Err(Error::Background(reason)))
    }

    fn wait_background(&self) -> Result<()> {
        let mut background = mutex_lock(&self.background.state);
        while (background.pending_flushes > 0 || background.busy) && background.failure.is_none() {
            background = self
                .background
                .changed
                .wait(background)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        background
            .failure
            .clone()
            .map_or(Ok(()), |reason| Err(Error::Background(reason)))
    }

    fn check_background_failure(&self) -> Result<()> {
        if let Some(reason) = mutex_lock(&self.background.state).failure.clone() {
            return Err(Error::Background(reason));
        }
        Ok(())
    }
}

impl Drop for FixedLsm {
    fn drop(&mut self) {
        let _ = self.wait_background();
        self.flush_sender.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn background_worker(inner: Arc<Inner>, background: Arc<BackgroundStatus>, receiver: mpsc::Receiver<FlushTask>) {
    while let Ok(first) = receiver.recv() {
        {
            let mut state = mutex_lock(&background.state);
            state.busy = true;
            background.changed.notify_all();
        }
        let mut next = Some(first);
        loop {
            while let Some(task) = next.take() {
                if let Err(error) = flush_memtable(inner.clone(), task) {
                    record_background_failure(&background, error.to_string());
                    return;
                }
                {
                    let mut state = mutex_lock(&background.state);
                    state.pending_flushes = state.pending_flushes.saturating_sub(1);
                    background.changed.notify_all();
                }
                next = receiver.try_recv().ok();
            }

            let result = {
                let _maintenance = mutex_lock(&inner.maintenance);
                compact_until_stable(&inner, false, Some(&background))
            };
            if let Err(error) = result {
                record_background_failure(&background, error.to_string());
                return;
            }
            next = receiver.try_recv().ok();
            if next.is_none() {
                break;
            }
        }
        let mut state = mutex_lock(&background.state);
        state.busy = false;
        background.changed.notify_all();
    }
}

fn record_background_failure(background: &BackgroundStatus, reason: String) {
    let mut state = mutex_lock(&background.state);
    state.failure = Some(reason);
    state.pending_flushes = 0;
    state.busy = false;
    background.changed.notify_all();
}

fn flush_memtable(inner: Arc<Inner>, task: FlushTask) -> Result<()> {
    let _maintenance = mutex_lock(&inner.maintenance);
    let (records, max_sequence, user_state) = task.frozen.sorted_records();
    if records.is_empty() {
        return Err(Error::Background("scheduled an empty immutable memtable".to_string()));
    }
    let table = create_table(inner.as_ref(), task.table_id, 0, &records)?;
    #[cfg(test)]
    crash_if_requested("fixed_lsm_after_flush_table");
    let table_bytes = table.meta().file_size;
    let current_version = rwlock_read(&inner.state).version.clone();
    let next_version = Arc::new(current_version.with_l0(table)?);
    let mut writer = mutex_lock(&inner.writer);
    let next_manifest = Manifest {
        generation: next_generation(writer.manifest.generation, &inner.directory)?,
        flushed_sequence: writer.manifest.flushed_sequence.max(max_sequence),
        next_sequence: writer.next_sequence,
        next_file_id: inner.next_file_id.load(AtomicOrdering::Acquire),
        user_state,
        tables: next_version.manifest_tables(),
    };
    persist_manifest(&inner, &next_manifest)?;
    #[cfg(test)]
    crash_if_requested("fixed_lsm_after_flush_manifest");
    {
        let mut state = rwlock_write(&inner.state);
        if !Arc::ptr_eq(&state.version, &current_version) {
            return Err(Error::Background(
                "durable version changed during memtable flush".to_string(),
            ));
        }
        state.version = next_version;
        state
            .immutables
            .retain(|immutable| !Arc::ptr_eq(immutable, &task.frozen));
    }
    writer.manifest = next_manifest;
    drop(writer);
    #[cfg(test)]
    crash_if_requested("fixed_lsm_after_flush_publish");
    let removed_wal_bytes = cleanup_wal_files_through(&inner.directory, task.wal_id)?;
    inner.disk_budget.release(removed_wal_bytes);
    inner.maintenance_counters.record_flush(table_bytes);
    Ok(())
}

#[derive(Debug)]
struct CompactionPlan {
    target_level: usize,
    inputs: Vec<Arc<Table>>,
    drop_tombstones: bool,
    trivial_move: bool,
}

fn compact_until_stable(inner: &Inner, force_l0: bool, background: Option<&BackgroundStatus>) -> Result<()> {
    loop {
        if let Some(background) = background {
            let mutable = rwlock_read(&inner.state).mutable.clone();
            if mutable.stats().1 >= inner.options.write_buffer_capacity as u64
                || mutex_lock(&background.state).pending_flushes > 0
            {
                return Ok(());
            }
        }
        let version = rwlock_read(&inner.state).version.clone();
        let Some(plan) = compaction_plan(&version, inner.options, force_l0) else {
            return Ok(());
        };
        run_compaction(inner, version, plan)?;
    }
}

fn run_compaction(inner: &Inner, current: Arc<Version>, plan: CompactionPlan) -> Result<()> {
    if plan.trivial_move {
        debug_assert_eq!(plan.inputs.len(), 1);
        let output = plan.inputs[0].at_level(plan.target_level as u32);
        publish_compaction(inner, current, &plan.inputs, vec![output], true)?;
        inner.maintenance_counters.record_compaction(0, 0, true);
        return Ok(());
    }
    let input_bytes = plan.inputs.iter().map(|table| table.meta().file_size).sum();
    let mut iterators = plan.inputs.iter().map(Table::iterator).collect::<Vec<_>>();
    let mut heap = BinaryHeap::new();
    for (source, iterator) in iterators.iter_mut().enumerate() {
        if let Some(record) = iterator.next_record()? {
            heap.push(MergeEntry { source, record });
        }
    }
    let target_records = (inner.options.write_buffer_capacity / RECORD_SIZE).max(1);
    let minimum_aligned_records = (target_records / MINIMUM_OUTPUT_FILL_DIVISOR).max(1);
    let mut pending = Vec::with_capacity(target_records);
    let mut outputs = Vec::new();
    let output_fences = compaction_output_fences(&current, plan.target_level);
    let mut output_fence = 0;
    while let Some(entry) = heap.pop() {
        let key = entry.record.key;
        let mut newest = entry.record;
        advance_source(entry.source, &mut iterators, &mut heap)?;
        while heap.peek().is_some_and(|entry| entry.record.key == key) {
            let entry = heap.pop().unwrap();
            if entry.record.sequence > newest.sequence {
                newest = entry.record;
            }
            advance_source(entry.source, &mut iterators, &mut heap)?;
        }
        if newest.value.is_none() && plan.drop_tombstones {
            continue;
        }
        while output_fence < output_fences.len() && newest.key > output_fences[output_fence] {
            if pending.len() >= minimum_aligned_records {
                outputs.push(write_compaction_table(inner, plan.target_level, &pending)?);
                pending.clear();
            }
            output_fence += 1;
        }
        pending.push(newest);
        if pending.len() == target_records {
            outputs.push(write_compaction_table(inner, plan.target_level, &pending)?);
            pending.clear();
        }
    }
    if !pending.is_empty() {
        outputs.push(write_compaction_table(inner, plan.target_level, &pending)?);
    }
    #[cfg(test)]
    crash_if_requested("fixed_lsm_after_compaction_tables");
    let output_bytes = outputs.iter().map(|table| table.meta().file_size).sum();
    publish_compaction(inner, current, &plan.inputs, outputs, false)?;
    inner
        .maintenance_counters
        .record_compaction(input_bytes, output_bytes, false);
    Ok(())
}

fn publish_compaction(
    inner: &Inner,
    current: Arc<Version>,
    inputs: &[Arc<Table>],
    outputs: Vec<Arc<Table>>,
    _trivial_move: bool,
) -> Result<()> {
    let removed = inputs.iter().map(|table| table.meta().file_id).collect::<HashSet<_>>();
    let retained = outputs.iter().map(|table| table.meta().file_id).collect::<HashSet<_>>();
    let next_version = Arc::new(current.replace(&removed, outputs)?);
    let mut writer = mutex_lock(&inner.writer);
    let next_manifest = Manifest {
        generation: next_generation(writer.manifest.generation, &inner.directory)?,
        flushed_sequence: writer.manifest.flushed_sequence,
        next_sequence: writer.next_sequence,
        next_file_id: inner.next_file_id.load(AtomicOrdering::Acquire),
        user_state: writer.manifest.user_state,
        tables: next_version.manifest_tables(),
    };
    persist_manifest(inner, &next_manifest)?;
    #[cfg(test)]
    if !_trivial_move {
        crash_if_requested("fixed_lsm_after_compaction_manifest");
    }
    {
        let mut state = rwlock_write(&inner.state);
        if !Arc::ptr_eq(&state.version, &current) {
            return Err(Error::Background(
                "durable version changed during compaction".to_string(),
            ));
        }
        state.version = next_version;
    }
    writer.manifest = next_manifest;
    drop(writer);
    #[cfg(test)]
    if !_trivial_move {
        crash_if_requested("fixed_lsm_after_compaction_publish");
    }
    let mut removed_bytes = 0_u64;
    for file_id in removed {
        if !retained.contains(&file_id) {
            let path = table_path(&inner.directory, file_id);
            let bytes = fs::metadata(&path)
                .map_err(|error| Error::io("stat obsolete SST file", error))?
                .len();
            fs::remove_file(path).map_err(|error| Error::io("remove obsolete SST file", error))?;
            removed_bytes = removed_bytes
                .checked_add(bytes)
                .ok_or_else(|| Error::InvalidOptions("removed SST bytes overflow u64".to_string()))?;
        }
    }
    if removed_bytes > 0 {
        crate::format::sync_directory(&inner.directory)?;
        inner.disk_budget.release(removed_bytes);
    }
    Ok(())
}

fn write_compaction_table(inner: &Inner, level: usize, records: &[Record]) -> Result<Arc<Table>> {
    let file_id = allocate_file_id(inner)?;
    create_table(inner, file_id, level as u32, records)
}

fn create_table(inner: &Inner, file_id: u64, level: u32, records: &[Record]) -> Result<Arc<Table>> {
    let bytes = table_file_size(records.len())
        .ok_or_else(|| Error::InvalidOptions("SST size overflows u64 or contains no records".to_string()))?;
    let reservation = inner.disk_budget.reserve(bytes)?;
    // Once creation begins, an I/O error can leave a partial or fully renamed file. Retain the
    // conservative reservation; the background pipeline becomes unhealthy and reopen recounts.
    reservation.commit();
    let table = Table::create(
        &inner.directory,
        file_id,
        level,
        records,
        inner.cache.clone(),
        inner.io.clone(),
    )?;
    debug_assert_eq!(table.meta().file_size, bytes);
    Ok(table)
}

fn persist_manifest(inner: &Inner, manifest: &Manifest) -> Result<()> {
    let replaced_bytes = manifest.replaced_file_size(&inner.directory)?;
    let reservation = inner.disk_budget.reserve(manifest.encoded_size(LEVEL_COUNT)?)?;
    // The temporary manifest may survive any failed I/O after creation. Keep its reservation on
    // error; callers poison the background/checkpoint path and reopen reconciles exact usage.
    reservation.commit();
    manifest.persist(&inner.directory, LEVEL_COUNT)?;
    inner.disk_budget.release(replaced_bytes);
    Ok(())
}

fn compaction_output_fences(version: &Version, target_level: usize) -> Vec<Key> {
    version.levels[target_level + 1..]
        .iter()
        .find(|tables| !tables.is_empty())
        .map(|tables| tables.iter().map(|table| table.meta().largest).collect())
        .unwrap_or_default()
}

fn compaction_plan(version: &Version, options: FixedLsmOptions, force_l0: bool) -> Option<CompactionPlan> {
    let (base_level, targets) = dynamic_level_targets(version, options);
    if version.levels[0].len() >= L0_COMPACTION_TRIGGER || force_l0 && !version.levels[0].is_empty() {
        if let Some(source) = version.levels[0]
            .iter()
            .rev()
            .find(|source| {
                version.levels[base_level]
                    .iter()
                    .all(|table| !overlaps(table, source.meta().smallest, source.meta().largest))
            })
            .cloned()
        {
            let drop_tombstones = base_level + 1 == LEVEL_COUNT;
            let trivial_move = !drop_tombstones || source.meta().tombstone_count == 0;
            return Some(CompactionPlan {
                target_level: base_level,
                inputs: vec![source],
                drop_tombstones,
                trivial_move,
            });
        }
        let mut inputs = version.levels[0].clone();
        let smallest = inputs.iter().map(|table| table.meta().smallest).min()?;
        let largest = inputs.iter().map(|table| table.meta().largest).max()?;
        inputs.extend(
            version.levels[base_level]
                .iter()
                .filter(|table| overlaps(table, smallest, largest))
                .cloned(),
        );
        return Some(CompactionPlan {
            target_level: base_level,
            inputs,
            drop_tombstones: base_level + 1 == LEVEL_COUNT,
            trivial_move: false,
        });
    }
    for (level, target) in targets.iter().copied().enumerate().take(LEVEL_COUNT - 1).skip(1) {
        let bytes = version.levels[level]
            .iter()
            .map(|table| compensated_table_size(table))
            .sum::<u64>();
        if bytes == 0 || level >= base_level && bytes <= target {
            continue;
        }
        let target_level = if level < base_level { base_level } else { level + 1 };
        let target_tables = &version.levels[target_level];
        let source = version.levels[level]
            .iter()
            .min_by(|left, right| {
                let left_overlap = overlapping_bytes(left, target_tables);
                let right_overlap = overlapping_bytes(right, target_tables);
                overlap_ratio_order(
                    left_overlap,
                    compensated_table_size(left),
                    right_overlap,
                    compensated_table_size(right),
                )
                .then_with(|| right.meta().file_size.cmp(&left.meta().file_size))
                .then_with(|| left.meta().file_id.cmp(&right.meta().file_id))
            })?
            .clone();
        let overlaps = version.levels[target_level]
            .iter()
            .filter(|table| overlaps(table, source.meta().smallest, source.meta().largest))
            .cloned()
            .collect::<Vec<_>>();
        if overlaps.is_empty() {
            let drop_tombstones = target_level + 1 == LEVEL_COUNT;
            let trivial_move = !drop_tombstones || source.meta().tombstone_count == 0;
            return Some(CompactionPlan {
                target_level,
                inputs: vec![source],
                drop_tombstones,
                trivial_move,
            });
        }
        let mut inputs = vec![source];
        inputs.extend(overlaps);
        return Some(CompactionPlan {
            target_level,
            inputs,
            drop_tombstones: target_level + 1 == LEVEL_COUNT,
            trivial_move: false,
        });
    }
    None
}

fn dynamic_level_targets(version: &Version, options: FixedLsmOptions) -> (usize, [u64; LEVEL_COUNT]) {
    let bottom_bytes = version.levels[LEVEL_COUNT - 1]
        .iter()
        .map(|table| table.meta().file_size)
        .sum::<u64>();
    dynamic_level_targets_for_bottom_bytes(bottom_bytes, options)
}

fn dynamic_level_targets_for_bottom_bytes(bottom_bytes: u64, options: FixedLsmOptions) -> (usize, [u64; LEVEL_COUNT]) {
    let minimum_base = (options.write_buffer_capacity as u64).saturating_mul(L0_COMPACTION_TRIGGER as u64);
    let minimum_dynamic_target = minimum_base.div_ceil(LEVEL_SIZE_MULTIPLIER);
    let mut targets = [0; LEVEL_COUNT];
    targets[LEVEL_COUNT - 1] = bottom_bytes.max(minimum_base);
    let mut base_level = LEVEL_COUNT - 1;
    let mut target = targets[LEVEL_COUNT - 1];
    for level in (1..LEVEL_COUNT - 1).rev() {
        target /= LEVEL_SIZE_MULTIPLIER;
        if target < minimum_dynamic_target {
            break;
        }
        targets[level] = target;
        base_level = level;
    }
    (base_level, targets)
}

fn overlaps(table: &Table, smallest: Key, largest: Key) -> bool {
    table.meta().smallest <= largest && smallest <= table.meta().largest
}

fn overlapping_bytes(source: &Table, targets: &[Arc<Table>]) -> u64 {
    targets
        .iter()
        .filter(|target| overlaps(target, source.meta().smallest, source.meta().largest))
        .map(|target| target.meta().file_size)
        .sum()
}

fn compensated_table_size(table: &Table) -> u64 {
    table
        .meta()
        .file_size
        .saturating_add(table.meta().tombstone_count.saturating_mul(RECORD_SIZE as u64))
}

fn overlap_ratio_order(left_overlap: u64, left_size: u64, right_overlap: u64, right_size: u64) -> Ordering {
    (u128::from(left_overlap) * u128::from(right_size)).cmp(&(u128::from(right_overlap) * u128::from(left_size)))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MergeEntry {
    source: usize,
    record: Record,
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .record
            .key
            .cmp(&self.record.key)
            .then_with(|| other.source.cmp(&self.source))
    }
}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn advance_source(source: usize, iterators: &mut [TableIterator], heap: &mut BinaryHeap<MergeEntry>) -> Result<()> {
    if let Some(record) = iterators[source].next_record()? {
        heap.push(MergeEntry { source, record });
    }
    Ok(())
}

fn allocate_file_id(inner: &Inner) -> Result<u64> {
    inner
        .next_file_id
        .fetch_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |file_id| {
            file_id.checked_add(1)
        })
        .map_err(|_| Error::corruption(&inner.directory, "SST file id space is exhausted"))
}

fn next_generation(generation: u64, directory: &Path) -> Result<u64> {
    generation
        .checked_add(1)
        .ok_or_else(|| Error::corruption(directory, "manifest generation space is exhausted"))
}

fn encode_batch(batch: &WriteBatch, first_sequence: u64) -> Result<Vec<Record>> {
    if first_sequence == 0 || first_sequence > MAX_SEQUENCE {
        return Err(Error::SequenceExhausted);
    }
    let last_sequence = first_sequence
        .checked_add(batch.len() as u64 - 1)
        .filter(|sequence| *sequence <= MAX_SEQUENCE)
        .ok_or(Error::SequenceExhausted)?;
    let mut records = Vec::with_capacity(batch.len());
    for (index, mutation) in batch.mutations.iter().copied().enumerate() {
        let sequence = first_sequence + index as u64;
        let record = match mutation {
            BatchMutation::Put(key, value) => Record::put(key, value, sequence),
            BatchMutation::Delete(key) => Record::delete(key, sequence),
        };
        records.push(record);
    }
    debug_assert_eq!(records.last().unwrap().sequence, last_sequence);
    Ok(records)
}

fn lock_database(directory: &Path) -> Result<File> {
    let path = directory.join(LOCK_FILE);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| Error::io("open database lock", error))?;
    let locked = FileExt::try_lock_exclusive(&file).map_err(|error| Error::io("lock database", error))?;
    if !locked {
        return Err(Error::DatabaseLocked(directory.to_path_buf()));
    }
    Ok(file)
}

fn directory_has_database_files(directory: &Path) -> Result<bool> {
    for entry in fs::read_dir(directory).map_err(|error| Error::io("list database directory", error))? {
        let entry = entry.map_err(|error| Error::io("read database directory entry", error))?;
        if entry.file_name() != LOCK_FILE {
            return Ok(true);
        }
    }
    Ok(false)
}

fn database_storage_bytes(directory: &Path) -> Result<u64> {
    let mut bytes = 0_u64;
    for entry in fs::read_dir(directory).map_err(|error| Error::io("list database storage", error))? {
        let entry = entry.map_err(|error| Error::io("read database storage entry", error))?;
        if !entry
            .file_type()
            .map_err(|error| Error::io("read database storage entry type", error))?
            .is_file()
        {
            continue;
        }
        let metadata = entry
            .metadata()
            .map_err(|error| Error::io("stat database storage entry", error))?;
        bytes = bytes
            .checked_add(metadata.len())
            .ok_or_else(|| Error::InvalidOptions("database storage size overflows u64".to_string()))?;
    }
    Ok(bytes)
}

fn maximum_table_file_id(directory: &Path) -> Result<u64> {
    let mut maximum = 0;
    for entry in fs::read_dir(directory).map_err(|error| Error::io("list SST files", error))? {
        let entry = entry.map_err(|error| Error::io("read SST directory entry", error))?;
        if let Some((file_id, _)) = entry.file_name().to_str().and_then(parse_table_file_name) {
            maximum = maximum.max(file_id);
        }
    }
    Ok(maximum)
}

fn cleanup_orphan_tables(directory: &Path, version: &Version) -> Result<()> {
    let live = version
        .levels
        .iter()
        .flatten()
        .map(|table| table.meta().file_id)
        .collect::<HashSet<_>>();
    let mut removed = false;
    for entry in fs::read_dir(directory).map_err(|error| Error::io("list orphan SST files", error))? {
        let entry = entry.map_err(|error| Error::io("read orphan SST entry", error))?;
        let Some((file_id, temporary)) = entry.file_name().to_str().and_then(parse_table_file_name) else {
            continue;
        };
        if temporary || !live.contains(&file_id) {
            fs::remove_file(entry.path()).map_err(|error| Error::io("remove orphan SST file", error))?;
            removed = true;
        }
    }
    if removed {
        crate::format::sync_directory(directory)?;
    }
    Ok(())
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn rwlock_read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn rwlock_write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
fn crash_if_requested(point: &str) {
    if std::env::var("FIXED_LSM_CRASH_AT").as_deref() == Ok(point) {
        std::process::exit(86);
    }
}

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, process::Command, sync::Arc};

    use crate::{
        db::{
            FixedLsm, FixedLsmMemoryLookup, FixedLsmOptions, MemValue, WriteBatch, WriteOptions,
            database_storage_bytes, dynamic_level_targets_for_bottom_bytes, mutex_lock, overlap_ratio_order,
        },
        error::Error,
        format::DATA_BLOCK_SIZE,
        wal::wal_path,
    };

    fn key(index: u64) -> [u8; 24] {
        let mut key = [0; 24];
        key[..8].copy_from_slice(&index.to_be_bytes());
        key
    }

    fn value(index: u64, generation: u64) -> [u8; 32] {
        let mut value = [0; 32];
        value[..8].copy_from_slice(&index.to_le_bytes());
        value[8..16].copy_from_slice(&generation.to_le_bytes());
        value
    }

    fn test_options() -> FixedLsmOptions {
        FixedLsmOptions {
            write_buffer_capacity: 64 * 16,
            cache_capacity: 1024 * 1024,
            ..FixedLsmOptions::default()
        }
    }

    #[test]
    fn memtable_value_does_not_duplicate_the_key() {
        assert_eq!(size_of::<MemValue>(), 40);
    }

    #[test]
    fn writes_flush_recover_and_delete() {
        let directory = tempfile::tempdir().unwrap();
        let db = FixedLsm::create(directory.path(), test_options()).unwrap();
        let mut batch = WriteBatch::with_capacity(40);
        for index in 0..40 {
            batch.put(key(index), value(index, 1));
        }
        db.write(&batch, WriteOptions::sync()).unwrap();
        db.flush().unwrap();

        let mut update = WriteBatch::default();
        update.put(key(7), value(7, 2));
        update.delete(key(8));
        db.write(&update, WriteOptions::sync()).unwrap();
        assert_eq!(db.get(&key(7)).unwrap(), Some(value(7, 2)));
        assert_eq!(db.get(&key(8)).unwrap(), None);
        drop(db);

        let db = FixedLsm::open(directory.path(), test_options()).unwrap();
        assert_eq!(db.get(&key(0)).unwrap(), Some(value(0, 1)));
        assert_eq!(db.get(&key(7)).unwrap(), Some(value(7, 2)));
        assert_eq!(db.get(&key(8)).unwrap(), None);
        assert_eq!(db.stats().recovered_records, 2);
    }

    #[test]
    fn repeated_reopen_removes_obsolete_empty_wals() {
        let directory = tempfile::tempdir().unwrap();
        let db = FixedLsm::create(directory.path(), test_options()).unwrap();
        db.put(key(1), value(1, 1), WriteOptions::sync()).unwrap();
        drop(db);

        let db = FixedLsm::open(directory.path(), test_options()).unwrap();
        assert_eq!(db.get(&key(1)).unwrap(), Some(value(1, 1)));
        assert!(wal_path(directory.path(), 1).is_file());
        assert!(wal_path(directory.path(), 2).is_file());
        drop(db);

        let db = FixedLsm::open(directory.path(), test_options()).unwrap();
        assert_eq!(db.get(&key(1)).unwrap(), Some(value(1, 1)));
        assert!(wal_path(directory.path(), 1).is_file());
        assert!(!wal_path(directory.path(), 2).exists());
        assert!(wal_path(directory.path(), 3).is_file());
    }

    #[test]
    fn memory_probe_distinguishes_values_misses_and_possible_table_reads() {
        let directory = tempfile::tempdir().unwrap();
        let db = FixedLsm::create(directory.path(), test_options()).unwrap();
        assert_eq!(db.probe_memory(&key(1)).unwrap(), FixedLsmMemoryLookup::Miss);

        db.put(key(1), value(1, 1), WriteOptions::buffered()).unwrap();
        assert_eq!(
            db.probe_memory(&key(1)).unwrap(),
            FixedLsmMemoryLookup::Value(value(1, 1))
        );
        db.flush().unwrap();
        assert_eq!(db.probe_memory(&key(1)).unwrap(), FixedLsmMemoryLookup::Unknown);
        assert_eq!(db.probe_memory(&key(2)).unwrap(), FixedLsmMemoryLookup::Miss);

        db.delete(key(1), WriteOptions::buffered()).unwrap();
        assert_eq!(db.probe_memory(&key(1)).unwrap(), FixedLsmMemoryLookup::Miss);
    }

    #[test]
    fn disk_budget_soft_limit_does_not_reject_a_wal_batch() {
        let directory = tempfile::tempdir().unwrap();
        let options = FixedLsmOptions {
            max_disk_bytes: 512,
            ..test_options()
        };
        let db = FixedLsm::create(directory.path(), options).unwrap();
        let before = db.stats().disk_used_bytes;
        let mut batch = WriteBatch::with_capacity(16);
        for index in 0..16 {
            batch.put(key(index), value(index, 1));
        }

        db.write(&batch, WriteOptions::sync()).unwrap();
        let stats = db.stats();
        assert!(stats.disk_used_bytes > stats.disk_capacity_bytes);
        assert!(stats.disk_used_bytes > before);
        assert_eq!(stats.writes, 16);
        assert_eq!(db.get(&key(0)).unwrap(), Some(value(0, 1)));
    }

    #[test]
    fn disk_budget_soft_limit_does_not_reject_compaction_output() {
        let directory = tempfile::tempdir().unwrap();
        let options = FixedLsmOptions {
            max_disk_bytes: 1,
            ..test_options()
        };
        let db = FixedLsm::create(directory.path(), options).unwrap();
        for run in 0..5 {
            let mut batch = WriteBatch::with_capacity(16);
            for index in 0..16 {
                batch.put(key(index), value(index, run + 1));
            }
            db.write(&batch, WriteOptions::sync()).unwrap();
            db.flush().unwrap();
        }
        db.compact().unwrap();
        let stats = db.stats();
        assert!(stats.disk_used_bytes > stats.disk_capacity_bytes);
        assert!(!stats.background_failed);
        assert!(db.ensure_healthy().is_ok());
        assert_eq!(db.get(&key(0)).unwrap(), Some(value(0, 5)));
    }

    #[test]
    fn disk_budget_matches_live_files_after_maintenance() {
        let directory = tempfile::tempdir().unwrap();
        let db = FixedLsm::create(directory.path(), test_options()).unwrap();
        for run in 0..6 {
            let mut batch = WriteBatch::with_capacity(16);
            for index in run * 16..(run + 1) * 16 {
                batch.put(key(index), value(index, 1));
            }
            db.write(&batch, WriteOptions::sync()).unwrap();
            db.flush().unwrap();
        }
        db.compact().unwrap();

        let expected = database_storage_bytes(directory.path()).unwrap();
        assert_eq!(db.stats().disk_used_bytes, expected);
        drop(db);

        let reopened = FixedLsm::open(directory.path(), test_options()).unwrap();
        assert_eq!(reopened.stats().disk_used_bytes, expected);
    }

    #[test]
    fn user_state_is_atomic_with_wal_and_manifest_publication() {
        let directory = tempfile::tempdir().unwrap();
        let db = FixedLsm::create(directory.path(), test_options()).unwrap();
        let mut first = WriteBatch::default();
        first.put(key(1), value(1, 1));
        db.write_with_user_state(&first, WriteOptions::sync(), 41).unwrap();
        assert_eq!(db.user_state(), 41);
        drop(db);

        let db = FixedLsm::open(directory.path(), test_options()).unwrap();
        assert_eq!(db.user_state(), 41);
        assert_eq!(db.get(&key(1)).unwrap(), Some(value(1, 1)));
        let mut second = WriteBatch::default();
        second.put(key(2), value(2, 1));
        db.write_with_user_state(&second, WriteOptions::buffered(), 42).unwrap();
        db.flush().unwrap();
        assert_eq!(db.user_state(), 42);
        drop(db);

        let db = FixedLsm::open(directory.path(), test_options()).unwrap();
        assert_eq!(db.user_state(), 42);
        assert_eq!(db.get(&key(2)).unwrap(), Some(value(2, 1)));
    }

    #[test]
    fn synced_wal_survives_process_exit() {
        let directory = tempfile::tempdir().unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("db::tests::synced_wal_survives_process_exit_child")
            .env("FIXED_LSM_CRASH_TEST_DIRECTORY", directory.path())
            .status()
            .unwrap();
        assert!(status.success());

        let db = FixedLsm::open(directory.path(), test_options()).unwrap();
        for index in 0..32 {
            assert_eq!(db.get(&key(index)).unwrap(), Some(value(index, 1)));
        }
        assert_eq!(db.stats().recovered_records, 32);
    }

    #[test]
    fn synced_wal_survives_process_exit_child() {
        let Ok(directory) = std::env::var("FIXED_LSM_CRASH_TEST_DIRECTORY") else {
            return;
        };
        let db = FixedLsm::create(directory, test_options()).unwrap();
        let _maintenance = mutex_lock(&db.inner.maintenance);
        for first in [0, 16] {
            let mut batch = WriteBatch::with_capacity(16);
            for index in first..first + 16 {
                batch.put(key(index), value(index, 1));
            }
            db.write(&batch, WriteOptions::buffered()).unwrap();
        }
        assert_eq!(db.stats().immutable_memtables, 2);
        db.sync_wal().unwrap();
        std::process::exit(0);
    }

    #[test]
    fn process_crash_during_flush_recovers_the_synced_batch() {
        for crash_at in [
            "fixed_lsm_after_flush_table",
            "fixed_lsm_after_flush_manifest",
            "fixed_lsm_after_flush_publish",
        ] {
            let directory = tempfile::tempdir().unwrap();
            let status = Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("db::tests::flush_crash_child")
                .env("FIXED_LSM_FAILPOINT_DIRECTORY", directory.path())
                .env("FIXED_LSM_CRASH_AT", crash_at)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "child did not crash at {crash_at}");

            let db = FixedLsm::open(directory.path(), test_options()).unwrap();
            for index in 0..32 {
                assert_eq!(db.get(&key(index)).unwrap(), Some(value(index, 1)));
            }
        }
    }

    #[test]
    fn flush_crash_child() {
        let Ok(directory) = std::env::var("FIXED_LSM_FAILPOINT_DIRECTORY") else {
            return;
        };
        let db = FixedLsm::create(directory, test_options()).unwrap();
        let mut batch = WriteBatch::with_capacity(32);
        for index in 0..32 {
            batch.put(key(index), value(index, 1));
        }
        db.write(&batch, WriteOptions::sync()).unwrap();
        db.flush().unwrap();
        panic!("flush crash failpoint was not reached");
    }

    #[test]
    fn process_crash_during_compaction_recovers_the_newest_values() {
        for crash_at in [
            "fixed_lsm_after_compaction_tables",
            "fixed_lsm_after_compaction_manifest",
            "fixed_lsm_after_compaction_publish",
        ] {
            let directory = tempfile::tempdir().unwrap();
            let status = Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("db::tests::compaction_crash_child")
                .env("FIXED_LSM_FAILPOINT_DIRECTORY", directory.path())
                .env("FIXED_LSM_CRASH_AT", crash_at)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "child did not crash at {crash_at}");

            let db = FixedLsm::open(directory.path(), test_options()).unwrap();
            for index in 0..16 {
                assert_eq!(
                    db.get(&key(index)).unwrap(),
                    Some(value(index, 5)),
                    "recovered stale value after {crash_at}"
                );
            }
        }
    }

    #[test]
    fn compaction_crash_child() {
        let Ok(directory) = std::env::var("FIXED_LSM_FAILPOINT_DIRECTORY") else {
            return;
        };
        let db = FixedLsm::create(directory, test_options()).unwrap();
        for generation in 1..=5 {
            let mut batch = WriteBatch::with_capacity(16);
            for index in 0..16 {
                batch.put(key(index), value(index, generation));
            }
            db.write(&batch, WriteOptions::sync()).unwrap();
            db.flush().unwrap();
        }
        db.compact().unwrap();
        panic!("compaction crash failpoint was not reached");
    }

    #[test]
    fn upper_tombstone_stops_before_reading_an_older_level() {
        let directory = tempfile::tempdir().unwrap();
        let db = FixedLsm::create(directory.path(), test_options()).unwrap();
        db.put(key(1), value(1, 1), WriteOptions::buffered()).unwrap();
        db.flush().unwrap();
        db.compact().unwrap();
        db.delete(key(1), WriteOptions::buffered()).unwrap();
        db.flush().unwrap();

        let before = db.stats().table_read_bytes;
        assert_eq!(db.get(&key(1)).unwrap(), None);
        let read_bytes = db.stats().table_read_bytes - before;
        assert_eq!(read_bytes, 3 * DATA_BLOCK_SIZE as u64);
        let lightweight = db.read_stats();
        let full = db.stats();
        assert_eq!(lightweight.read_operations, full.table_read_operations);
        assert_eq!(lightweight.read_bytes, full.table_read_bytes);
    }

    #[test]
    fn bottom_compaction_purges_tombstone_and_older_value() {
        let directory = tempfile::tempdir().unwrap();
        let db = FixedLsm::create(directory.path(), test_options()).unwrap();
        db.put(key(1), value(1, 1), WriteOptions::buffered()).unwrap();
        db.flush().unwrap();
        db.compact().unwrap();

        db.delete(key(1), WriteOptions::buffered()).unwrap();
        db.flush().unwrap();
        db.compact().unwrap();
        assert_eq!(db.get(&key(1)).unwrap(), None);
        assert_eq!(db.stats().level_files.iter().sum::<u64>(), 0);
        drop(db);

        let db = FixedLsm::open(directory.path(), test_options()).unwrap();
        assert_eq!(db.get(&key(1)).unwrap(), None);
        assert_eq!(db.stats().level_files.iter().sum::<u64>(), 0);
    }

    #[test]
    fn nonoverlapping_tombstone_is_rewritten_before_bottom_level() {
        let directory = tempfile::tempdir().unwrap();
        let db = FixedLsm::create(directory.path(), test_options()).unwrap();
        db.delete(key(1), WriteOptions::buffered()).unwrap();
        db.flush().unwrap();
        assert_eq!(db.stats().level_files[0], 1);

        db.compact().unwrap();
        assert_eq!(db.get(&key(1)).unwrap(), None);
        assert_eq!(db.stats().level_files.iter().sum::<u64>(), 0);
    }

    #[test]
    fn compaction_keeps_the_newest_sequence_under_concurrent_reads() {
        let directory = tempfile::tempdir().unwrap();
        let db = Arc::new(FixedLsm::create(directory.path(), test_options()).unwrap());
        for generation in 1..=6 {
            let mut batch = WriteBatch::with_capacity(16);
            for index in 0..16 {
                batch.put(key(index), value(index, generation));
            }
            db.write(&batch, WriteOptions::buffered()).unwrap();
            db.flush().unwrap();
        }
        let reader = {
            let db = db.clone();
            std::thread::spawn(move || {
                for _ in 0..1_000 {
                    assert_eq!(db.get(&key(3)).unwrap(), Some(value(3, 6)));
                }
            })
        };
        db.compact().unwrap();
        reader.join().unwrap();
        assert_eq!(db.get(&key(3)).unwrap(), Some(value(3, 6)));
        assert!(db.stats().level_files[0] < 4);
    }

    #[test]
    fn background_flush_keeps_newer_active_writes_visible() {
        let directory = tempfile::tempdir().unwrap();
        let db = FixedLsm::create(directory.path(), test_options()).unwrap();
        let mut first = WriteBatch::with_capacity(32);
        for index in 0..32 {
            first.put(key(index), value(index, 1));
        }
        db.write(&first, WriteOptions::buffered()).unwrap();
        let mut newer = WriteBatch::default();
        newer.put(key(3), value(3, 2));
        db.write(&newer, WriteOptions::sync()).unwrap();
        assert_eq!(db.get(&key(3)).unwrap(), Some(value(3, 2)));
        db.flush().unwrap();
        assert_eq!(db.get(&key(3)).unwrap(), Some(value(3, 2)));
    }

    #[test]
    fn database_directory_has_single_process_ownership() {
        let directory = tempfile::tempdir().unwrap();
        let _db = FixedLsm::create(directory.path(), test_options()).unwrap();
        assert!(matches!(
            FixedLsm::open(directory.path(), test_options()),
            Err(Error::DatabaseLocked(_))
        ));
    }

    #[test]
    fn non_overlapping_tables_change_levels_without_rewrite() {
        let directory = tempfile::tempdir().unwrap();
        let db = FixedLsm::create(directory.path(), test_options()).unwrap();
        for run in 0..8 {
            let mut batch = WriteBatch::with_capacity(16);
            for index in run * 16..(run + 1) * 16 {
                batch.put(key(index), value(index, 1));
            }
            db.write(&batch, WriteOptions::buffered()).unwrap();
            db.flush().unwrap();
        }
        db.compact().unwrap();
        let stats = db.stats();
        assert_eq!(stats.table_write_operations, 8);
        assert!(stats.level_files[1..].iter().sum::<u64>() > 0);
        drop(db);

        let db = FixedLsm::open(directory.path(), test_options()).unwrap();
        assert_eq!(db.get(&key(0)).unwrap(), Some(value(0, 1)));
        assert_eq!(db.get(&key(127)).unwrap(), Some(value(127, 1)));
    }

    #[test]
    fn compaction_picker_compares_overlap_without_floating_point() {
        assert_eq!(overlap_ratio_order(0, 1, 1, 100), Ordering::Less);
        assert_eq!(overlap_ratio_order(64, 64, 64, 4), Ordering::Less);
        assert_eq!(overlap_ratio_order(128, 64, 64, 32), Ordering::Equal);
    }

    #[test]
    fn dynamic_levels_keep_a_small_base_above_the_large_levels() {
        let options = FixedLsmOptions::default();
        let total = 6_560_000_000;
        let (base, targets) = dynamic_level_targets_for_bottom_bytes(total, options);
        assert_eq!(base, 4);
        assert_eq!(targets[4], 65_600_000);
        assert_eq!(targets[5], 656_000_000);
        assert_eq!(targets[6], total);
    }
}
