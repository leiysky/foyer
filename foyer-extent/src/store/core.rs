#[cfg(test)]
use std::sync::atomic::{AtomicU8, Ordering};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

#[cfg(test)]
use crate::format::stored_entry_checksum;
#[cfg(test)]
use crate::model::CachePriority;
use crate::{
    error::{Error, Result},
    format::{ContentDigest, stored_entry_len, value_digest},
    model::{EntryKey, KeyDigest},
    store::{
        checkpoint::{CheckpointCoordinator, CheckpointStats},
        config::{ExtentStoreConfig, ExtentStoreOptions},
        format::{EntryLocation, StoreLayout},
        index::{EntryIndex, EntryIndexMemoryLookup, EntryIndexReadStats, EntryIndexStats, INDEX_DIRECTORY},
        io::IoSchedulerStats,
        operation::{BatchInsertResult, EntryInsert, GetResult, InsertOutcome},
        pool::{DATA_FILE, EntryAllocation, EntryWrite, ExtentPool, LEGACY_SLOT_OWNER_FILE, STATE_FILE},
        reclaim::{AllocationDecision, ReclaimResult, Reclaimer},
        stats::{ExtentLayoutStats, ExtentOccupancy, PhysicalWriteStats},
    },
};

#[derive(Debug)]
pub struct ExtentStore {
    index: Arc<EntryIndex>,
    pool: Arc<ExtentPool>,
    layout: StoreLayout,
    options: ExtentStoreOptions,
    mutations: Arc<Mutex<()>>,
    checkpoints: CheckpointCoordinator,
    #[cfg(test)]
    injected_fault: AtomicU8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreparedGet {
    Location(EntryLocation),
    Miss,
    Unknown(KeyDigest),
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum InjectedFault {
    WriteNoSpace = 1,
    WriteZero = 2,
    Sync = 3,
}

impl ExtentStore {
    pub fn create(path: impl AsRef<Path>, config: ExtentStoreConfig) -> Result<Self> {
        validate_options(config.options)?;
        let layout = StoreLayout::create(config)?;
        validate_layout_options(layout, config.options)?;
        let root = path.as_ref();
        fs::create_dir_all(root).map_err(|error| Error::io("create extent store directory", error))?;
        let index = EntryIndex::create(
            root,
            layout.index_capacity_bytes,
            config.options.index_write_buffer_size,
            config.options.index_cache_size,
        )?;
        let pool = ExtentPool::create(
            root,
            layout,
            config.options.direct_io,
            config.options.write_concurrency,
            config.options.io_read_priority_duration,
            config.options.read_run_size,
            config.options.write_run_size,
        )?;
        Self::from_parts(index, pool, config.options)
    }

    pub fn recreate(path: impl AsRef<Path>, config: ExtentStoreConfig) -> Result<Self> {
        let root = path.as_ref();
        fs::create_dir_all(root).map_err(|error| Error::io("create extent store directory", error))?;
        remove_owned_directory(&root.join(INDEX_DIRECTORY))?;
        for file in [DATA_FILE, LEGACY_SLOT_OWNER_FILE, STATE_FILE] {
            remove_owned_file(&root.join(file))?;
        }
        Self::create(root, config)
    }

    pub fn open_with_options(path: impl AsRef<Path>, options: ExtentStoreOptions) -> Result<Self> {
        validate_options(options)?;
        let root = path.as_ref();
        let pool = ExtentPool::open(
            root,
            options.direct_io,
            options.write_concurrency,
            options.io_read_priority_duration,
            options.read_run_size,
            options.write_run_size,
        )?;
        validate_layout_options(pool.layout(), options)?;
        let index = EntryIndex::open(
            root,
            pool.layout().index_capacity_bytes,
            options.index_write_buffer_size,
            options.index_cache_size,
        )?;
        let store = Self::from_parts(index, pool, options)?;
        store.reclaimer().recover_pending()?;
        Ok(store)
    }

    fn from_parts(index: EntryIndex, pool: ExtentPool, options: ExtentStoreOptions) -> Result<Self> {
        let layout = pool.layout();
        let index = Arc::new(index);
        let pool = Arc::new(pool);
        index.install_liveness_filter(pool.clone());
        let mutations = Arc::new(Mutex::new(()));
        let dirty_bytes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let checkpoints = CheckpointCoordinator::new(index.clone(), pool.clone(), mutations.clone(), dirty_bytes)?;
        Ok(Self {
            index,
            pool,
            layout,
            options,
            mutations,
            checkpoints,
            #[cfg(test)]
            injected_fault: AtomicU8::new(0),
        })
    }

    pub(crate) const fn layout(&self) -> StoreLayout {
        self.layout
    }

    pub const fn file_size(&self) -> u64 {
        self.layout.total_file_size
    }

    pub fn allocated_size(&self) -> Result<u64> {
        self.index
            .allocated_size()?
            .checked_add(self.pool.allocated_size()?)
            .ok_or_else(|| Error::InvalidConfig("extent store allocated size overflows u64".to_string()))
    }

    pub fn physical_write_stats(&self) -> PhysicalWriteStats {
        let mut stats = self.pool.physical_write_stats();
        stats.merge(self.index.physical_write_stats());
        stats
    }

    pub fn layout_stats(&self, configured_capacity_bytes: u64) -> ExtentLayoutStats {
        ExtentLayoutStats {
            configured_capacity_bytes,
            planned_file_bytes: self.layout.total_file_size,
            data_file_bytes: self.layout.data_file_size,
            usable_payload_bytes: u64::from(self.layout.extent_count - 1) * self.layout.extent_size as u64,
            index_soft_capacity_bytes: self.layout.index_capacity_bytes,
            extent_size_bytes: self.layout.extent_size as u64,
            physical_extents: u64::from(self.layout.extent_count),
            usable_extents: u64::from(self.layout.extent_count - 1),
            entry_charge_bytes: self.layout.entry_charge as u64,
            planned_live_entries: self.layout.planned_max_entries,
            maximum_live_entries: self.layout.maximum_entries,
        }
    }

    pub fn io_scheduler_stats(&self) -> IoSchedulerStats {
        self.pool.io_scheduler_stats()
    }

    pub fn entry_index_stats(&self) -> EntryIndexStats {
        self.index.stats()
    }

    pub fn entry_index_read_stats(&self) -> EntryIndexReadStats {
        self.index.read_stats()
    }

    pub fn entry_index_io_read_stats(&self) -> EntryIndexReadStats {
        self.index.io_read_stats()
    }

    pub fn checkpoint_stats(&self) -> CheckpointStats {
        self.checkpoints.stats()
    }

    pub fn extent_occupancy(&self) -> ExtentOccupancy {
        self.pool.extent_occupancy(self.priority_capacity_floors())
    }

    #[cfg(all(test, target_os = "linux"))]
    pub const fn direct_io(&self) -> bool {
        self.options.direct_io
    }

    #[cfg(test)]
    pub fn get(&self, key: &EntryKey) -> Result<Option<Vec<u8>>> {
        self.get_with_stats(key).map(|result| result.value)
    }

    #[cfg(test)]
    pub fn get_with_stats(&self, key: &EntryKey) -> Result<GetResult> {
        let prepared = self.prepare_get(key)?;
        self.get_prepared(key, prepared)
    }

    pub(crate) fn prepare_get(&self, key: &EntryKey) -> Result<PreparedGet> {
        let key_digest = KeyDigest::for_key(key);
        Ok(match self.index.lookup_memory(key_digest)? {
            EntryIndexMemoryLookup::Location(location) => PreparedGet::Location(location),
            EntryIndexMemoryLookup::Miss => PreparedGet::Miss,
            EntryIndexMemoryLookup::Unknown => PreparedGet::Unknown(key_digest),
        })
    }

    #[cfg(test)]
    pub(crate) fn get_prepared(&self, key: &EntryKey, prepared: PreparedGet) -> Result<GetResult> {
        let Some(location) = self.resolve_prepared(prepared)? else {
            return Ok(GetResult::default());
        };
        self.read_location(key, location)
    }

    /// Resolve only the fixed-index portion of a prepared lookup. This deliberately performs no
    /// payload I/O so metadata misses cannot consume the payload-read admission budget.
    pub(crate) fn resolve_prepared(&self, prepared: PreparedGet) -> Result<Option<EntryLocation>> {
        match prepared {
            PreparedGet::Location(location) => Ok(Some(location)),
            PreparedGet::Miss => Ok(None),
            PreparedGet::Unknown(key_digest) => self.index.peek(key_digest),
        }
    }

    pub(crate) fn read_location(&self, key: &EntryKey, location: EntryLocation) -> Result<GetResult> {
        let stored = self.pool.read_entry(key, location)?;
        Ok(GetResult {
            priority: stored.value.as_ref().map(|_| location.priority),
            value: stored.value,
            data_frames: stored.data_frames,
            data_runs: stored.data_runs,
            data_bytes: stored.data_bytes,
        })
    }

    #[cfg(test)]
    pub fn insert(&self, key: &EntryKey, value: &[u8], priority: CachePriority) -> Result<InsertOutcome> {
        let result = self.insert_batch_with_stats(&[EntryInsert::new(key, value, priority)])?;
        Ok(result
            .outcomes
            .into_iter()
            .next()
            .expect("single extent insert must have one outcome"))
    }

    #[cfg(test)]
    pub fn insert_batch(&self, inserts: &[EntryInsert<'_>]) -> Result<Vec<InsertOutcome>> {
        self.insert_batch_with_stats(inserts).map(|result| result.outcomes)
    }

    pub fn insert_batch_with_stats(&self, inserts: &[EntryInsert<'_>]) -> Result<BatchInsertResult> {
        #[cfg(test)]
        self.inject_insert_fault()?;
        for insert in inserts {
            self.validate_entry(insert.key, insert.value)?;
        }
        if inserts.is_empty() {
            return Ok(BatchInsertResult::default());
        }

        let mutation = mutex_lock(&self.mutations);
        self.checkpoints.ensure_healthy()?;
        let mut outcomes = vec![None; inserts.len()];
        let mut reclaim = ReclaimResult::default();
        let mut write_runs = 0usize;
        let mut written_bytes = 0usize;
        let mut published_bytes = 0usize;
        let mut input_index = 0usize;

        while input_index < inserts.len() {
            let mut known: HashMap<KeyDigest, KnownInsert<'_>> = HashMap::new();
            let mut pending = Vec::new();
            let mut protected_extents = HashSet::new();

            while input_index < inserts.len() {
                let insert = inserts[input_index];
                let key_digest = KeyDigest::for_key(insert.key);
                let stored_len =
                    stored_entry_len(insert.key, insert.value).expect("validated stored entry length must fit usize");
                let content_digest = value_digest(insert.value);
                let (would_admit, exact_match) = if let Some(known) = known.get(&key_digest) {
                    (
                        true,
                        known.location.stored_len as usize == stored_len
                            && known.location.content_digest == content_digest
                            && insert.priority == known.location.priority
                            && insert.key == known.key,
                    )
                } else {
                    let exact_match = matches!(
                        self.index.lookup_memory(key_digest)?,
                        EntryIndexMemoryLookup::Location(current)
                            if self.pool.location_is_live(current)
                                && current.stored_len as usize == stored_len
                                && current.content_digest == content_digest
                                && insert.priority == current.priority
                    );
                    (true, exact_match)
                };
                if exact_match {
                    outcomes[input_index] = Some(InsertOutcome::Updated);
                    input_index += 1;
                    continue;
                } else if !would_admit {
                    outcomes[input_index] = Some(InsertOutcome::Rejected);
                    input_index += 1;
                    continue;
                }

                let stored_priority = insert.priority;
                let allocation = match self
                    .reclaimer()
                    .allocate(stored_priority, stored_len, &protected_extents)?
                {
                    AllocationDecision::Allocated(allocation, reclaimed) => {
                        reclaim.merge(reclaimed);
                        allocation
                    }
                    AllocationDecision::FlushRequired(reclaimed) => {
                        reclaim.merge(reclaimed);
                        break;
                    }
                    AllocationDecision::Rejected(reclaimed) => {
                        reclaim.merge(reclaimed);
                        outcomes[input_index] = Some(InsertOutcome::Rejected);
                        input_index += 1;
                        continue;
                    }
                };
                let location = EntryLocation {
                    data_offset: allocation.data_offset,
                    extent_generation: allocation.extent_generation,
                    stored_len: u32::try_from(stored_len).expect("validated stored entry length must fit u32"),
                    content_digest,
                    priority: stored_priority,
                };
                known.insert(
                    key_digest,
                    KnownInsert {
                        location,
                        key: insert.key,
                    },
                );
                protected_extents.insert(allocation.extent);
                pending.push(PendingInsert {
                    input_index,
                    allocation,
                    key: insert.key,
                    key_digest,
                    value: insert.value,
                    content_digest,
                });
                input_index += 1;
            }

            if pending.is_empty() {
                assert_eq!(
                    input_index,
                    inserts.len(),
                    "an ExtentStore batch boundary must publish at least one pending insert"
                );
                continue;
            }

            let writes = pending
                .iter()
                .map(|pending| EntryWrite {
                    allocation: pending.allocation,
                    key: pending.key,
                    value: pending.value,
                    content_digest: pending.content_digest,
                })
                .collect::<Vec<_>>();
            let physical = self.pool.write_batch(&writes)?;
            let index_inserts = pending
                .iter()
                .zip(&physical.locations)
                .map(|(pending, location)| (pending.key_digest, *location))
                .collect::<Vec<_>>();
            let indexed = self.index.insert_batch(&index_inserts)?;
            for (pending, outcome) in pending.iter().zip(indexed.outcomes) {
                published_bytes = published_bytes.saturating_add(pending.allocation.stored_len as usize);
                outcomes[pending.input_index] = Some(outcome);
            }
            write_runs = write_runs
                .saturating_add(physical.data_runs)
                .saturating_add(indexed.write_runs);
            written_bytes = written_bytes
                .saturating_add(physical.data_bytes)
                .saturating_add(indexed.written_bytes);
        }

        if published_bytes > 0 {
            self.checkpoints.record_publication(published_bytes)?;
        }
        let checkpoint_target = self.checkpoints.published_epoch();
        if self.checkpoints.dirty_bytes() >= self.options.checkpoint_bytes {
            self.checkpoints.request_background(checkpoint_target)?;
        }
        drop(mutation);

        Ok(BatchInsertResult {
            outcomes: outcomes
                .into_iter()
                .map(|outcome| outcome.expect("every extent insert must have an outcome"))
                .collect(),
            write_runs: write_runs.saturating_add(reclaim.write_runs),
            written_bytes: written_bytes.saturating_add(reclaim.written_bytes),
            reclaim: reclaim.stats,
        })
    }

    #[cfg(test)]
    pub fn remove(&self, key: &EntryKey) -> Result<bool> {
        Ok(self.remove_batch(std::slice::from_ref(key))? == 1)
    }

    pub fn remove_batch(&self, keys: &[EntryKey]) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mutation = mutex_lock(&self.mutations);
        self.checkpoints.ensure_healthy()?;
        let digests = keys.iter().map(KeyDigest::for_key).collect::<Vec<_>>();
        let removed = self.index.remove_batch(&digests)?;
        if removed > 0 {
            let published_bytes = removed.saturating_mul(self.layout.entry_charge);
            let target = self.checkpoints.record_publication(published_bytes)?;
            if self.checkpoints.dirty_bytes() >= self.options.checkpoint_bytes {
                self.checkpoints.request_background(target)?;
            }
        }
        drop(mutation);
        Ok(removed)
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.index.ensure_healthy()?;
        self.checkpoints.checkpoint()
    }

    pub fn request_checkpoint(&self) -> Result<()> {
        self.index.ensure_healthy()?;
        self.checkpoints.request_background(self.checkpoints.published_epoch())
    }

    pub fn sync(&self) -> Result<()> {
        #[cfg(test)]
        if self.injected_fault.swap(0, Ordering::AcqRel) == InjectedFault::Sync as u8 {
            return Err(Error::io(
                "injected extent sync",
                std::io::Error::other("injected fdatasync failure"),
            ));
        }
        self.checkpoint()?;
        self.index.wait_for_maintenance()
    }

    #[cfg(test)]
    pub(crate) fn inject_fault(&self, fault: InjectedFault) {
        self.injected_fault.store(fault as u8, Ordering::Release);
    }

    #[cfg(test)]
    fn inject_insert_fault(&self) -> Result<()> {
        match self.injected_fault.load(Ordering::Acquire) {
            fault if fault == InjectedFault::WriteNoSpace as u8 => {
                self.injected_fault.store(0, Ordering::Release);
                Err(Error::io(
                    "injected extent data write",
                    std::io::Error::new(std::io::ErrorKind::StorageFull, "injected no-space write"),
                ))
            }
            fault if fault == InjectedFault::WriteZero as u8 => {
                self.injected_fault.store(0, Ordering::Release);
                Err(Error::io(
                    "injected extent data write",
                    std::io::Error::new(std::io::ErrorKind::WriteZero, "injected short write"),
                ))
            }
            _ => Ok(()),
        }
    }

    fn validate_entry(&self, key: &EntryKey, value: &[u8]) -> Result<()> {
        if value.is_empty() {
            return Err(Error::EmptyValue);
        }
        let Some(stored_len) = stored_entry_len(key, value) else {
            return Err(Error::StoredEntryTooLarge {
                len: usize::MAX,
                maximum: self.pool.layout().extent_size,
            });
        };
        if stored_len > self.pool.layout().extent_size {
            return Err(Error::StoredEntryTooLarge {
                len: stored_len,
                maximum: self.pool.layout().extent_size,
            });
        }
        Ok(())
    }

    fn reclaimer(&self) -> Reclaimer<'_> {
        Reclaimer::new(&self.pool, &self.checkpoints, self.priority_capacity_floors())
    }

    fn priority_capacity_floors(&self) -> [u32; 3] {
        self.options
            .priority_capacity_floors
            .extent_floors(self.layout.extent_count.saturating_sub(1))
    }
}

fn remove_owned_file(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io("remove previous extent store file", error)),
    }
}

fn remove_owned_directory(path: &Path) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(Error::io("inspect previous EntryIndex", error)),
    };
    let result = if metadata.file_type().is_symlink() || !metadata.is_dir() {
        fs::remove_file(path)
    } else {
        fs::remove_dir_all(path)
    };
    result.map_err(|error| Error::io("remove previous EntryIndex", error))
}

#[derive(Debug, Clone, Copy)]
struct PendingInsert<'a> {
    input_index: usize,
    allocation: EntryAllocation,
    key: &'a EntryKey,
    key_digest: KeyDigest,
    value: &'a [u8],
    content_digest: ContentDigest,
}

#[derive(Debug, Clone, Copy)]
struct KnownInsert<'a> {
    location: EntryLocation,
    key: &'a EntryKey,
}

fn validate_options(options: ExtentStoreOptions) -> Result<()> {
    if !(1..=64).contains(&options.write_concurrency) {
        return Err(Error::InvalidConfig(
            "extent write_concurrency must be between 1 and 64".to_string(),
        ));
    }
    if options.read_run_size == 0 {
        return Err(Error::InvalidConfig(
            "extent read_run_size must be greater than zero".to_string(),
        ));
    }
    if options.write_run_size == 0 {
        return Err(Error::InvalidConfig(
            "extent write_run_size must be greater than zero".to_string(),
        ));
    }
    if options.checkpoint_bytes == 0 {
        return Err(Error::InvalidConfig(
            "extent checkpoint_bytes must be greater than zero".to_string(),
        ));
    }
    if options.index_write_buffer_size < fixed_lsm::KEY_SIZE + fixed_lsm::VALUE_SIZE + 8 {
        return Err(Error::InvalidConfig(
            "extent index_write_buffer_size must fit one fixed-LSM record".to_string(),
        ));
    }
    if options.index_cache_size == 0 {
        return Err(Error::InvalidConfig(
            "extent index_cache_size must be greater than zero".to_string(),
        ));
    }
    let priority_capacity_floors = options.priority_capacity_floors;
    if priority_capacity_floors.high_percent() > 100
        || priority_capacity_floors.normal_percent() > 100
        || u16::from(priority_capacity_floors.high_percent()) + u16::from(priority_capacity_floors.normal_percent())
            > 100
    {
        return Err(Error::InvalidConfig(
            "extent high and normal priority capacity floors must each be at most 100 percent and sum to at most 100"
                .to_string(),
        ));
    }
    #[cfg(not(target_os = "linux"))]
    if options.direct_io {
        return Err(Error::InvalidConfig(
            "direct I/O is only supported on Linux".to_string(),
        ));
    }
    Ok(())
}

fn validate_layout_options(layout: StoreLayout, options: ExtentStoreOptions) -> Result<()> {
    if options.extent_size != layout.extent_size {
        return Err(Error::InvalidConfig(format!(
            "configured extent_size ({}) does not match the stored layout ({})",
            options.extent_size, layout.extent_size
        )));
    }
    if !options.read_run_size.is_multiple_of(crate::format::PAGE_SIZE) {
        return Err(Error::InvalidConfig(format!(
            "extent read_run_size must be a multiple of page size ({})",
            crate::format::PAGE_SIZE
        )));
    }
    if !options.write_run_size.is_multiple_of(crate::format::PAGE_SIZE) {
        return Err(Error::InvalidConfig(format!(
            "extent write_run_size must be a multiple of page size ({})",
            crate::format::PAGE_SIZE
        )));
    }
    let usable_extents = layout.extent_count.saturating_sub(1);
    let capacity_floors = options.priority_capacity_floors.extent_floors(usable_extents);
    if capacity_floors.into_iter().sum::<u32>() > usable_extents {
        return Err(Error::InvalidConfig(
            "extent priority capacity floors exceed usable extent capacity".to_string(),
        ));
    }
    Ok(())
}

fn mutex_lock<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, mpsc},
        time::Duration,
    };

    use tempfile::tempdir;

    use super::*;
    use crate::{format::PAGE_SIZE, store::pool::AllocationResult};

    fn key(index: u64) -> EntryKey {
        let mut bytes = [index as u8; 24];
        bytes[16..].copy_from_slice(&index.to_le_bytes());
        EntryKey::new(bytes).unwrap()
    }

    fn full_frame_value(byte: u8) -> Vec<u8> {
        let key = key(0);
        let envelope = stored_entry_len(&key, &[]).unwrap();
        vec![byte; PAGE_SIZE - envelope]
    }

    fn options() -> ExtentStoreOptions {
        ExtentStoreOptions::default()
            .with_extent_size(PAGE_SIZE * 8)
            .with_index_write_buffer_size(PAGE_SIZE * 4)
            .with_index_cache_size(1024 * 1024)
            .with_checkpoint_bytes(usize::MAX)
    }

    fn store(root: &Path, capacity: u64) -> ExtentStore {
        ExtentStore::create(
            root,
            ExtentStoreConfig::new(capacity)
                .with_entry_charge(PAGE_SIZE)
                .with_options(options()),
        )
        .unwrap()
    }

    #[test]
    fn insert_get_update_remove_and_reopen() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 4 * 1024 * 1024);
        assert_eq!(
            store.insert(&key(1), &[1; 100], CachePriority::Normal).unwrap(),
            InsertOutcome::Inserted
        );
        assert_eq!(store.get(&key(1)).unwrap(), Some(vec![1; 100]));
        assert_eq!(
            store.insert(&key(1), &[2; 200], CachePriority::High).unwrap(),
            InsertOutcome::Updated
        );
        assert_eq!(store.get(&key(1)).unwrap(), Some(vec![2; 200]));
        store.insert(&key(2), &[3; 50], CachePriority::Low).unwrap();
        assert!(store.remove(&key(2)).unwrap());
        store.sync().unwrap();
        let layout = store.pool.layout();
        drop(store);

        let reopened = ExtentStore::open_with_options(dir.path(), options()).unwrap();
        assert_eq!(reopened.pool.layout(), layout);
        assert_eq!(reopened.get(&key(1)).unwrap(), Some(vec![2; 200]));
        assert!(reopened.get(&key(2)).unwrap().is_none());
    }

    #[test]
    fn crc_collision_does_not_suppress_a_value_update() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 4 * 1024 * 1024);
        let first_key = key(1);
        let batched_key = key(2);
        let old = vec![0x11; 512];
        let mut replacement = vec![0x22; 512];
        replacement[508..].copy_from_slice(&[0xce, 0xe0, 0x15, 0xb2]);
        assert_ne!(old, replacement);
        assert_eq!(
            stored_entry_checksum(&first_key, &old),
            stored_entry_checksum(&first_key, &replacement)
        );
        assert_eq!(
            stored_entry_checksum(&batched_key, &old),
            stored_entry_checksum(&batched_key, &replacement)
        );
        assert_ne!(value_digest(&old), value_digest(&replacement));

        store.insert(&first_key, &old, CachePriority::Normal).unwrap();
        store.insert(&first_key, &replacement, CachePriority::Normal).unwrap();
        store
            .insert_batch(&[
                EntryInsert::new(&batched_key, &old, CachePriority::Normal),
                EntryInsert::new(&batched_key, &replacement, CachePriority::Normal),
            ])
            .unwrap();
        assert_eq!(store.get(&first_key).unwrap(), Some(replacement.clone()));
        assert_eq!(store.get(&batched_key).unwrap(), Some(replacement.clone()));
        store.sync().unwrap();
        drop(store);

        let reopened = ExtentStore::open_with_options(dir.path(), options()).unwrap();
        assert_eq!(reopened.get(&first_key).unwrap(), Some(replacement.clone()));
        assert_eq!(reopened.get(&batched_key).unwrap(), Some(replacement));
    }

    #[test]
    fn prepared_get_reuses_memory_lookup_and_returns_definitive_misses() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 4 * 1024 * 1024);
        let entry_key = key(7);
        assert_eq!(store.prepare_get(&entry_key).unwrap(), PreparedGet::Miss);

        store.insert(&entry_key, &[7; 128], CachePriority::Normal).unwrap();
        let prepared = store.prepare_get(&entry_key).unwrap();
        assert!(matches!(prepared, PreparedGet::Location(_)));
        assert_eq!(
            store.get_prepared(&entry_key, prepared).unwrap().value,
            Some(vec![7; 128])
        );

        assert!(store.remove(&entry_key).unwrap());
        assert_eq!(store.prepare_get(&entry_key).unwrap(), PreparedGet::Miss);
    }

    #[test]
    fn allocated_size_tracks_index_budget_without_file_scans() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 4 * 1024 * 1024);
        let before = store.allocated_size().unwrap();
        store.insert(&key(1), &[1; 100], CachePriority::Normal).unwrap();
        store.sync().unwrap();
        let after = store.allocated_size().unwrap();
        assert!(after > before);
        assert_eq!(store.allocated_size().unwrap(), after);
    }

    #[test]
    fn reopen_rejects_a_different_extent_size() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 4 * 1024 * 1024);
        store.sync().unwrap();
        drop(store);

        let error = ExtentStore::open_with_options(dir.path(), options().with_extent_size(PAGE_SIZE * 16)).unwrap_err();
        assert!(matches!(error, Error::InvalidConfig(_)));
    }

    #[test]
    fn index_preserves_extent_publication_and_reopens() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 32 * 1024 * 1024);
        assert_eq!(
            store.insert(&key(1), &[1; 100], CachePriority::Normal).unwrap(),
            InsertOutcome::Inserted
        );
        assert_eq!(
            store
                .insert(&key(2), &[2; PAGE_SIZE + 17], CachePriority::High)
                .unwrap(),
            InsertOutcome::Inserted
        );
        store.checkpoint().unwrap();
        assert_eq!(store.entry_index_stats().indexed_entries_upper_bound, 2);
        drop(store);

        let store = ExtentStore::open_with_options(
            dir.path(),
            options()
                .with_index_write_buffer_size(PAGE_SIZE * 4)
                .with_index_cache_size(1024 * 1024),
        )
        .unwrap();
        assert_eq!(store.get(&key(1)).unwrap(), Some(vec![1; 100]));
        assert_eq!(store.get(&key(2)).unwrap(), Some(vec![2; PAGE_SIZE + 17]));
        assert!(store.remove(&key(1)).unwrap());
        store.checkpoint().unwrap();
        assert_eq!(store.entry_index_stats().indexed_entries_upper_bound, 1);
        drop(store);

        let store = ExtentStore::open_with_options(
            dir.path(),
            options()
                .with_index_write_buffer_size(PAGE_SIZE * 4)
                .with_index_cache_size(1024 * 1024),
        )
        .unwrap();
        assert_eq!(store.get(&key(1)).unwrap(), None);
        assert_eq!(store.get(&key(2)).unwrap(), Some(vec![2; PAGE_SIZE + 17]));
    }

    #[test]
    fn multi_frame_entry_respects_read_run_limit_and_reopens() {
        let dir = tempdir().unwrap();
        let options = options().with_read_run_size(PAGE_SIZE);
        let store = ExtentStore::create(
            dir.path(),
            ExtentStoreConfig::new(4 * 1024 * 1024)
                .with_entry_charge(PAGE_SIZE)
                .with_options(options),
        )
        .unwrap();
        let value = (0..PAGE_SIZE * 3 + 17)
            .map(|offset| (offset % 251) as u8)
            .collect::<Vec<_>>();

        assert_eq!(
            store.insert(&key(7), &value, CachePriority::High).unwrap(),
            InsertOutcome::Inserted
        );
        let stats = store.physical_write_stats();
        assert_eq!(stats.data_bytes, (PAGE_SIZE * 4) as u64);
        let read = store.get_with_stats(&key(7)).unwrap();
        assert_eq!(read.value, Some(value.clone()));
        assert_eq!(read.data_frames, 4);
        assert_eq!(read.data_runs, 4);

        store.sync().unwrap();
        drop(store);
        let reopened = ExtentStore::open_with_options(dir.path(), options).unwrap();
        assert_eq!(reopened.get(&key(7)).unwrap(), Some(value));
    }

    #[test]
    fn small_entries_share_a_write_frame() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 4 * 1024 * 1024);
        let keys = (0..8).map(key).collect::<Vec<_>>();
        let values = (0..8).map(|index| vec![index as u8; 100]).collect::<Vec<_>>();
        let inserts = keys
            .iter()
            .zip(&values)
            .map(|(key, value)| EntryInsert::new(key, value, CachePriority::Normal))
            .collect::<Vec<_>>();

        let result = store.insert_batch_with_stats(&inserts).unwrap();
        assert!(
            result
                .outcomes
                .iter()
                .all(|outcome| *outcome == InsertOutcome::Inserted)
        );
        let writes = store.physical_write_stats();
        assert_eq!(writes.data_runs, 1);
        assert_eq!(writes.data_bytes, PAGE_SIZE as u64);
        let occupancy = store.extent_occupancy();
        assert_eq!(occupancy.used_entries(CachePriority::Normal), keys.len() as u64);
        assert_eq!(occupancy.used_bytes(CachePriority::Normal), PAGE_SIZE as u64);

        let locations = keys
            .iter()
            .map(|key| store.index.peek(KeyDigest::for_key(key)).unwrap().unwrap())
            .collect::<Vec<_>>();
        for pair in locations.windows(2) {
            assert_eq!(pair[1].data_offset, pair[0].data_offset + u64::from(pair[0].stored_len));
        }
        for (key, value) in keys.iter().zip(values) {
            assert_eq!(store.get(key).unwrap(), Some(value));
        }
    }

    #[test]
    fn concurrent_entry_writers_preserve_every_value() {
        let dir = tempdir().unwrap();
        let store = Arc::new(store(dir.path(), 8 * 1024 * 1024));
        let concurrency = 4u64;
        let entries_per_writer = 32u64;

        std::thread::scope(|scope| {
            for writer in 0..concurrency {
                let store = store.clone();
                scope.spawn(move || {
                    for entry in 0..entries_per_writer {
                        let index = writer * entries_per_writer + entry;
                        let len = PAGE_SIZE + (index as usize % (PAGE_SIZE * 2));
                        let value = vec![index as u8; len];
                        store.insert(&key(index), &value, CachePriority::Normal).unwrap();
                        assert_eq!(store.get(&key(index)).unwrap(), Some(value));
                    }
                });
            }
        });
        store.sync().unwrap();
        for index in 0..concurrency * entries_per_writer {
            let len = PAGE_SIZE + (index as usize % (PAGE_SIZE * 2));
            assert_eq!(store.get(&key(index)).unwrap(), Some(vec![index as u8; len]));
        }
    }

    #[test]
    fn checkpoint_threshold_counts_published_stored_bytes() {
        let dir = tempdir().unwrap();
        let first_key = key(1);
        let second_key = key(2);
        let first_value = vec![1; 700];
        let second_value = vec![2; 1_300];
        let first_bytes = stored_entry_len(&first_key, &first_value).unwrap();
        let second_bytes = stored_entry_len(&second_key, &second_value).unwrap();
        let checkpoint_options = options().with_checkpoint_bytes(first_bytes + second_bytes);
        let store = ExtentStore::create(
            dir.path(),
            ExtentStoreConfig::new(4 * 1024 * 1024)
                .with_entry_charge(PAGE_SIZE)
                .with_options(checkpoint_options),
        )
        .unwrap();
        store.checkpoints.pause_after_capture();

        store.insert(&first_key, &first_value, CachePriority::Normal).unwrap();
        assert_eq!(store.checkpoint_stats().dirty_bytes, first_bytes);
        assert_eq!(store.checkpoint_stats().requested_epoch, 0);

        store.insert(&second_key, &second_value, CachePriority::Normal).unwrap();
        store.checkpoints.wait_until_captured();
        let checkpoint = store.checkpoint_stats();
        assert_eq!(checkpoint.published_epoch, 2);
        assert_eq!(checkpoint.requested_epoch, 2);
        store.checkpoints.resume_checkpoint();
        store.sync().unwrap();
    }

    #[test]
    fn checkpoint_epoch_does_not_block_later_publication() {
        let dir = tempdir().unwrap();
        let checkpoint_options = options().with_checkpoint_bytes(1);
        let store = Arc::new(
            ExtentStore::create(
                dir.path(),
                ExtentStoreConfig::new(4 * 1024 * 1024)
                    .with_entry_charge(PAGE_SIZE)
                    .with_options(checkpoint_options),
            )
            .unwrap(),
        );
        store.checkpoints.pause_after_capture();
        store.insert(&key(1), &[1; PAGE_SIZE], CachePriority::Normal).unwrap();
        store.checkpoints.wait_until_captured();

        let (sent, received) = mpsc::channel();
        let writer = {
            let store = store.clone();
            std::thread::spawn(move || {
                let result = store.insert(&key(2), &[2; PAGE_SIZE], CachePriority::Normal);
                sent.send(result).unwrap();
            })
        };
        assert_ne!(
            received.recv_timeout(Duration::from_secs(1)).unwrap().unwrap(),
            InsertOutcome::Rejected
        );
        assert_eq!(store.get(&key(2)).unwrap(), Some(vec![2; PAGE_SIZE]));
        store.checkpoints.resume_checkpoint();
        writer.join().unwrap();
        store.sync().unwrap();
        drop(store);

        let reopened = ExtentStore::open_with_options(dir.path(), checkpoint_options).unwrap();
        assert_eq!(reopened.get(&key(1)).unwrap(), Some(vec![1; PAGE_SIZE]));
        assert_eq!(reopened.get(&key(2)).unwrap(), Some(vec![2; PAGE_SIZE]));
    }

    #[test]
    fn reclaim_waits_for_an_in_flight_epoch_before_generation_reuse() {
        let dir = tempdir().unwrap();
        let store = Arc::new(store(dir.path(), 2 * 1024 * 1024));
        let entries = store.pool.layout().planned_max_entries;
        let initial = full_frame_value(7);
        for index in 0..entries - 1 {
            assert_ne!(
                store.insert(&key(index), &initial, CachePriority::Low).unwrap(),
                InsertOutcome::Rejected
            );
        }
        store.sync().unwrap();

        store.checkpoints.pause_after_capture();
        let updated = full_frame_value(8);
        assert_ne!(
            store.insert(&key(0), &updated, CachePriority::Low).unwrap(),
            InsertOutcome::Rejected
        );
        store
            .checkpoints
            .request_background(store.checkpoints.published_epoch())
            .unwrap();
        store.checkpoints.wait_until_captured();

        let (sent, received) = mpsc::channel();
        let writer = {
            let store = store.clone();
            let value = full_frame_value(9);
            std::thread::spawn(move || {
                let result = store.insert(&key(100_000), &value, CachePriority::High);
                sent.send(result).unwrap();
            })
        };
        let early = received.recv_timeout(Duration::from_millis(50));
        store.checkpoints.resume_checkpoint();
        assert!(
            early.is_err(),
            "reclaim reused a generation before its checkpoint became durable"
        );
        let result = received
            .recv_timeout(Duration::from_secs(30))
            .expect("reclaim did not resume after its checkpoint became durable");
        assert_ne!(result.unwrap(), InsertOutcome::Rejected);
        writer.join().unwrap();
        store.sync().unwrap();
        assert_eq!(store.get(&key(100_000)).unwrap(), Some(full_frame_value(9)));
    }

    #[test]
    fn checkpoint_failure_is_retained_and_rejects_later_mutations() {
        let dir = tempdir().unwrap();
        let checkpoint_options = options().with_checkpoint_bytes(1);
        let store = ExtentStore::create(
            dir.path(),
            ExtentStoreConfig::new(4 * 1024 * 1024)
                .with_entry_charge(PAGE_SIZE)
                .with_options(checkpoint_options),
        )
        .unwrap();
        store.checkpoints.fail_after_capture();
        store.insert(&key(1), &[1; PAGE_SIZE], CachePriority::Normal).unwrap();
        assert!(matches!(store.sync(), Err(Error::CheckpointFailed(_))));
        assert_eq!(store.get(&key(1)).unwrap(), Some(vec![1; PAGE_SIZE]));
        assert!(matches!(
            store.insert(&key(2), &[2; PAGE_SIZE], CachePriority::Normal),
            Err(Error::CheckpointFailed(_))
        ));
        drop(store);

        let reopened = ExtentStore::open_with_options(dir.path(), checkpoint_options).unwrap();
        assert!(reopened.get(&key(1)).unwrap().is_none());
    }

    #[test]
    fn physical_write_stats_separate_payload_and_metadata() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 4 * 1024 * 1024);
        let values = [full_frame_value(1), full_frame_value(2)];
        store
            .insert_batch(&[
                EntryInsert::new(&key(1), &values[0], CachePriority::Normal),
                EntryInsert::new(&key(2), &values[1], CachePriority::Normal),
            ])
            .unwrap();

        let before_checkpoint = store.physical_write_stats();
        assert_eq!(before_checkpoint.data_runs, 1);
        assert_eq!(before_checkpoint.data_bytes, (PAGE_SIZE * 2) as u64);
        assert_eq!(before_checkpoint.data_syncs, 0);
        assert_eq!(before_checkpoint.index_runs, 0);
        assert_eq!(before_checkpoint.index_syncs, 0);
        assert_eq!(before_checkpoint.allocator_runs, 0);
        assert_eq!(before_checkpoint.allocator_syncs, 0);

        store.checkpoint().unwrap();
        let after_checkpoint = store.physical_write_stats();
        assert!(after_checkpoint.index_runs > 0);
        assert!(after_checkpoint.index_bytes > 0);
        assert_eq!(after_checkpoint.index_syncs, 1);
        assert_eq!(after_checkpoint.index_wal_runs, 1);
        assert!(after_checkpoint.index_wal_bytes > 0);
        assert_eq!(after_checkpoint.index_wal_syncs, 1);
        assert_eq!(after_checkpoint.index_sst_runs, 0);
        assert_eq!(after_checkpoint.index_manifest_runs, 0);
        assert_eq!(after_checkpoint.allocator_runs, 1);
        assert_eq!(after_checkpoint.allocator_syncs, 1);
        assert_eq!(
            after_checkpoint.allocator_bytes,
            store.pool.layout().state_copy_size as u64
        );
        assert_eq!(
            after_checkpoint.total_bytes(),
            after_checkpoint.data_bytes + after_checkpoint.index_bytes + after_checkpoint.allocator_bytes
        );
        assert_eq!(after_checkpoint.data_syncs, 1);
        assert_eq!(after_checkpoint.total_syncs(), 3);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn direct_io_handles_partial_frames_and_reopens() {
        let dir = tempdir().unwrap();
        let options = options().with_direct_io(true);
        let store = match ExtentStore::create(
            dir.path(),
            ExtentStoreConfig::new(2 * 1024 * 1024)
                .with_entry_charge(PAGE_SIZE)
                .with_options(options),
        ) {
            Ok(store) => store,
            Err(Error::Io { source, .. })
                if matches!(
                    source.kind(),
                    std::io::ErrorKind::InvalidInput | std::io::ErrorKind::Unsupported
                ) =>
            {
                return;
            }
            Err(error) => panic!("failed to create direct I/O extent store: {error}"),
        };

        assert!(store.direct_io());
        let value = vec![9; PAGE_SIZE / 2 + 17];
        let full = full_frame_value(8);
        let first_key = key(1);
        let second_key = key(2);
        let result = store
            .insert_batch_with_stats(&[
                EntryInsert::new(&first_key, &value, CachePriority::Normal),
                EntryInsert::new(&second_key, &full, CachePriority::Normal),
            ])
            .unwrap();
        assert!(
            result
                .outcomes
                .iter()
                .all(|outcome| *outcome == InsertOutcome::Inserted)
        );
        let partial_read = store.get_with_stats(&key(1)).unwrap();
        assert_eq!(partial_read.value, Some(value.clone()));
        assert_eq!(partial_read.data_frames, 1);
        assert_eq!(partial_read.data_runs, 1);
        assert_eq!(partial_read.data_bytes, PAGE_SIZE);
        let full_read = store.get_with_stats(&key(2)).unwrap();
        assert_eq!(full_read.value, Some(full.clone()));
        assert_eq!(full_read.data_frames, 2);
        assert_eq!(full_read.data_runs, 1);
        assert_eq!(full_read.data_bytes, PAGE_SIZE * 2);
        store.sync().unwrap();
        drop(store);

        let store = ExtentStore::open_with_options(dir.path(), options).unwrap();
        assert_eq!(store.get(&key(1)).unwrap(), Some(value));
        assert_eq!(store.get(&key(2)).unwrap(), Some(full));
    }

    #[test]
    fn batch_is_sequential_and_preserves_same_key_order() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 4 * 1024 * 1024);
        let first = vec![1; 256];
        let second = vec![2; 512];
        let third = vec![3; 768];
        let result = store
            .insert_batch_with_stats(&[
                EntryInsert::new(&key(1), &first, CachePriority::Low),
                EntryInsert::new(&key(2), &second, CachePriority::Normal),
                EntryInsert::new(&key(1), &third, CachePriority::High),
            ])
            .unwrap();
        assert_eq!(result.outcomes.len(), 3);
        assert_eq!(store.get(&key(1)).unwrap(), Some(third));
        assert_eq!(store.get(&key(2)).unwrap(), Some(second));
        assert!(result.write_runs <= 8);
    }

    #[test]
    fn batch_larger_than_capacity_never_reuses_an_unpublished_extent() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 2 * 1024 * 1024);
        let inserts = store.pool.layout().planned_max_entries as usize * 2;
        let values = (0..inserts)
            .map(|index| vec![(index % 251) as u8; 16])
            .collect::<Vec<_>>();
        let keys = (0..inserts).map(|index| key(index as u64)).collect::<Vec<_>>();
        let batch = values
            .iter()
            .zip(&keys)
            .map(|(value, key)| EntryInsert::new(key, value, CachePriority::Normal))
            .collect::<Vec<_>>();

        let result = store.insert_batch_with_stats(&batch).unwrap();
        assert_eq!(result.outcomes.len(), inserts);
        for (index, expected) in values.iter().enumerate() {
            if let Some(value) = store.get(&key(index as u64)).unwrap() {
                assert_eq!(&value, expected);
            }
        }
    }

    #[test]
    fn checkpoint_turns_reclaimed_overlay_locations_into_tombstones() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 2 * 1024 * 1024);
        let inserts = store.pool.layout().planned_max_entries as usize * 2;
        let values = (0..inserts)
            .map(|index| vec![(index % 251) as u8; 16])
            .collect::<Vec<_>>();
        let keys = (0..inserts).map(|index| key(index as u64)).collect::<Vec<_>>();
        let batch = values
            .iter()
            .zip(&keys)
            .map(|(value, key)| EntryInsert::new(key, value, CachePriority::Normal))
            .collect::<Vec<_>>();

        store.insert_batch_with_stats(&batch).unwrap();
        store.sync().unwrap();
        drop(store);

        let reopened = ExtentStore::open_with_options(dir.path(), options()).unwrap();
        for key in &keys {
            if let Some(location) = reopened.index.peek(KeyDigest::for_key(key)).unwrap() {
                assert!(
                    reopened.pool.location_is_live(location),
                    "checkpoint persisted a reclaimed overlay location for {key:?}"
                );
            }
        }
    }

    #[test]
    fn reclaim_does_not_force_a_payload_sync() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 2 * 1024 * 1024);
        let entries = store.pool.layout().planned_max_entries as usize;
        let keys = (0..entries).map(|index| key(index as u64)).collect::<Vec<_>>();
        let value = full_frame_value(7);
        let batch = keys
            .iter()
            .map(|key| EntryInsert::new(key, &value, CachePriority::Low))
            .collect::<Vec<_>>();
        let populated = store.insert_batch_with_stats(&batch).unwrap();
        assert!(
            populated
                .outcomes
                .iter()
                .all(|outcome| *outcome == InsertOutcome::Inserted)
        );

        let before = store.physical_write_stats();
        let incoming_key = key(entries as u64 + 1);
        let result = store
            .insert_batch_with_stats(&[EntryInsert::new(&incoming_key, &value, CachePriority::High)])
            .unwrap();
        assert_eq!(result.reclaim.total_reclaimed_extents(), 1);
        assert_eq!(result.outcomes, vec![InsertOutcome::Inserted]);
        let after = store.physical_write_stats();
        assert_eq!(after.data_syncs - before.data_syncs, 0);
        assert_eq!(store.get(&incoming_key).unwrap(), Some(value));
        store.checkpoint().unwrap();
        let checkpointed = store.physical_write_stats();
        assert_eq!(checkpointed.data_syncs - after.data_syncs, 1);
    }

    #[test]
    fn hot_update_batch_larger_than_capacity_preserves_fifo_values() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 512 * 1024);
        let entries = store.pool.layout().planned_max_entries as usize;
        for index in 0..entries {
            store
                .insert(&key(index as u64), &[7; 16], CachePriority::Normal)
                .unwrap();
        }
        for index in 0..entries {
            for _ in 0..3 {
                assert_eq!(store.get(&key(index as u64)).unwrap(), Some(vec![7; 16]));
            }
        }

        let values = (0..entries * 2)
            .map(|index| vec![(index / entries + 1) as u8; 16])
            .collect::<Vec<_>>();
        let keys = (0..entries).map(|index| key(index as u64)).collect::<Vec<_>>();
        let batch = values
            .iter()
            .enumerate()
            .map(|(index, value)| EntryInsert::new(&keys[index % entries], value, CachePriority::Normal))
            .collect::<Vec<_>>();
        let result = store.insert_batch_with_stats(&batch).unwrap();
        assert_eq!(result.outcomes.len(), batch.len());

        for index in 0..entries {
            if let Some(value) = store.get(&key(index as u64)).unwrap() {
                assert_eq!(value, vec![2; 16]);
            }
        }
    }

    #[test]
    fn randomized_churn_never_returns_a_stale_or_wrong_value() {
        const KEY_COUNT: usize = 64;
        const OPERATIONS: usize = 384;

        fn random(state: &mut u64) -> u64 {
            let mut value = *state;
            value ^= value << 13;
            value ^= value >> 7;
            value ^= value << 17;
            *state = value;
            value
        }

        fn assert_valid_hit(store: &ExtentStore, model: &[Option<Vec<u8>>], index: usize) {
            if let Some(actual) = store.get(&key(index as u64)).unwrap() {
                let expected = model[index]
                    .as_ref()
                    .expect("a removed or never-inserted key returned a value");
                assert_eq!(&actual, expected, "key {index} returned a stale value");
            }
        }

        let dir = tempdir().unwrap();
        let mut store = store(dir.path(), 512 * 1024);
        let mut model = vec![None; KEY_COUNT];
        let mut rng = 0x243f_6a88_85a3_08d3_u64;

        for operation in 0..OPERATIONS {
            if operation == OPERATIONS / 2 {
                store.sync().unwrap();
                drop(store);
                store = ExtentStore::open_with_options(dir.path(), options()).unwrap();
            }

            let index = random(&mut rng) as usize % KEY_COUNT;
            match random(&mut rng) % 10 {
                0..=6 => {
                    let len = 16 + random(&mut rng) as usize % (PAGE_SIZE * 2);
                    let mut value = vec![random(&mut rng) as u8; len];
                    value[..8].copy_from_slice(&(operation as u64).to_le_bytes());
                    let priority = match random(&mut rng) % 10 {
                        0 => CachePriority::High,
                        1..=6 => CachePriority::Normal,
                        _ => CachePriority::Low,
                    };
                    if store.insert(&key(index as u64), &value, priority).unwrap() != InsertOutcome::Rejected {
                        model[index] = Some(value);
                    }
                }
                7 => {
                    store.remove(&key(index as u64)).unwrap();
                    model[index] = None;
                }
                _ => assert_valid_hit(&store, &model, index),
            }

            assert_valid_hit(&store, &model, index);
            assert_valid_hit(&store, &model, random(&mut rng) as usize % KEY_COUNT);
            if operation % 48 == 47 {
                store.checkpoint().unwrap();
            }
        }

        store.sync().unwrap();
        for index in 0..KEY_COUNT {
            assert_valid_hit(&store, &model, index);
        }
    }

    #[test]
    fn low_priority_cannot_reclaim_protected_data() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 2 * 1024 * 1024);
        let entries = store.pool.layout().planned_max_entries as usize;
        for index in 0..entries {
            store.insert(&key(index as u64), &[1; 16], CachePriority::High).unwrap();
        }
        assert_eq!(
            store.insert(&key(10_000), &[2; 16], CachePriority::Low).unwrap(),
            InsertOutcome::Rejected
        );
        assert!(store.get(&key(0)).unwrap().is_some());
        assert_ne!(
            store.insert(&key(10_001), &[3; 16], CachePriority::High).unwrap(),
            InsertOutcome::Rejected
        );
    }

    #[test]
    fn priority_floors_prevent_starvation_and_shared_capacity_prefers_high() {
        let dir = tempdir().unwrap();
        let store = ExtentStore::create(
            dir.path(),
            ExtentStoreConfig::new(2 * 1024 * 1024)
                .with_entry_charge(PAGE_SIZE)
                .with_options(options().with_priority_capacity_floors(25, 50)),
        )
        .unwrap();
        let layout = store.pool.layout();
        let usable_extents = layout.extent_count - 1;
        let value = vec![1; layout.extent_size - stored_entry_len(&key(0), &[]).unwrap()];
        let high_floor = store.extent_occupancy().capacity_floor_extents(CachePriority::High);
        let normal_floor = store.extent_occupancy().capacity_floor_extents(CachePriority::Normal);

        for index in 0..usable_extents as usize {
            assert_ne!(
                store.insert(&key(index as u64), &value, CachePriority::High).unwrap(),
                InsertOutcome::Rejected
            );
        }
        assert_eq!(
            store.extent_occupancy().occupied_extents(CachePriority::High),
            usable_extents
        );

        for index in 0..normal_floor as usize {
            assert_ne!(
                store
                    .insert(&key(100_000 + index as u64), &value, CachePriority::Normal)
                    .unwrap(),
                InsertOutcome::Rejected
            );
        }
        let occupancy = store.extent_occupancy();
        assert_eq!(occupancy.occupied_extents(CachePriority::Normal), normal_floor);
        assert_eq!(
            occupancy.occupied_extents(CachePriority::High),
            usable_extents - normal_floor
        );

        assert_ne!(
            store.insert(&key(300_000), &value, CachePriority::Normal).unwrap(),
            InsertOutcome::Rejected
        );
        let occupancy = store.extent_occupancy();
        assert_eq!(occupancy.occupied_extents(CachePriority::Normal), normal_floor);
        assert_eq!(
            occupancy.occupied_extents(CachePriority::High),
            usable_extents - normal_floor
        );

        let second = tempdir().unwrap();
        let store = ExtentStore::create(
            second.path(),
            ExtentStoreConfig::new(2 * 1024 * 1024)
                .with_entry_charge(PAGE_SIZE)
                .with_options(options().with_priority_capacity_floors(25, 50)),
        )
        .unwrap();
        for index in 0..usable_extents as usize {
            assert_ne!(
                store
                    .insert(&key(400_000 + index as u64), &value, CachePriority::Normal)
                    .unwrap(),
                InsertOutcome::Rejected
            );
        }
        for index in 0..(usable_extents - normal_floor) as usize {
            assert_ne!(
                store
                    .insert(&key(500_000 + index as u64), &value, CachePriority::High)
                    .unwrap(),
                InsertOutcome::Rejected
            );
        }
        let occupancy = store.extent_occupancy();
        assert_eq!(occupancy.occupied_extents(CachePriority::Normal), normal_floor);
        assert_eq!(
            occupancy.occupied_extents(CachePriority::High),
            usable_extents - normal_floor
        );
        assert!(high_floor <= occupancy.occupied_extents(CachePriority::High));
    }

    #[test]
    fn invalid_priority_capacity_is_rejected() {
        let dir = tempdir().unwrap();
        let error = ExtentStore::create(
            dir.path(),
            ExtentStoreConfig::new(2 * 1024 * 1024)
                .with_entry_charge(PAGE_SIZE)
                .with_options(options().with_priority_capacity_floors(40, 61)),
        )
        .unwrap_err();
        assert!(matches!(error, Error::InvalidConfig(_)));
    }

    #[test]
    fn lower_priority_current_is_reclaimed_before_sealed_normal_data() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 512 * 1024);
        let layout = store.pool.layout();
        store.insert(&key(0), &[1; 16], CachePriority::Low).unwrap();

        let normal_entries =
            (layout.extent_count as usize - 2).saturating_mul(layout.planned_entries_per_extent as usize);
        for index in 0..normal_entries {
            assert_ne!(
                store
                    .insert(&key(index as u64 + 1), &[2; 16], CachePriority::Normal)
                    .unwrap(),
                InsertOutcome::Rejected
            );
        }

        let result = store
            .insert_batch_with_stats(&[
                EntryInsert::new(&key(0), &[9; 16], CachePriority::Low),
                EntryInsert::new(&key(100_000), &[3; 16], CachePriority::Normal),
            ])
            .unwrap();
        assert!(
            result
                .outcomes
                .iter()
                .all(|outcome| *outcome != InsertOutcome::Rejected)
        );
        assert_eq!(result.reclaim.reclaimed_extents(CachePriority::Low), 1);
        assert!(result.reclaim.invalidated_entries(CachePriority::Low) >= 1);
        assert!(result.reclaim.invalidated_bytes(CachePriority::Low) >= PAGE_SIZE);
        assert_eq!(store.get(&key(0)).unwrap(), None);
        assert_eq!(store.get(&key(1)).unwrap(), Some(vec![2; 16]));
    }

    #[test]
    fn reclaim_invalidates_a_whole_extent_without_reading_or_copying_it() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 2 * 1024 * 1024);
        let layout = store.pool.layout();
        let entries = layout.planned_max_entries as usize;
        for index in 0..entries {
            assert_ne!(
                store
                    .insert(&key(index as u64), &[5; 16], CachePriority::Normal)
                    .unwrap(),
                InsertOutcome::Rejected
            );
        }
        for _ in 0..3 {
            assert_eq!(store.get(&key(0)).unwrap(), Some(vec![5; 16]));
        }

        let index_reads_before = store.entry_index_io_read_stats();
        let writes_before = store.physical_write_stats();
        let incoming = key(entries as u64);
        let result = store
            .insert_batch_with_stats(&[EntryInsert::new(&incoming, &[6; 16], CachePriority::Normal)])
            .unwrap();
        let index_reads_after = store.entry_index_io_read_stats();
        let writes_after = store.physical_write_stats();

        assert_ne!(result.outcomes[0], InsertOutcome::Rejected);
        assert_eq!(result.reclaim.total_reclaimed_extents(), 1);
        assert_eq!(
            result.reclaim.total_invalidated_entries(),
            layout.planned_entries_per_extent as usize
        );
        assert_eq!(result.reclaim.total_invalidated_bytes(), layout.extent_size);
        assert_eq!(index_reads_after, index_reads_before);
        assert_eq!(writes_after.allocator_runs - writes_before.allocator_runs, 1);
        assert_eq!(writes_after.allocator_syncs - writes_before.allocator_syncs, 1);
        assert_eq!(writes_after.index_runs - writes_before.index_runs, 0);
        assert_eq!(store.get(&key(0)).unwrap(), None);
        assert_eq!(store.get(&incoming).unwrap(), Some(vec![6; 16]));
    }

    #[test]
    fn reopen_completes_an_interrupted_reclaim_transaction() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 2 * 1024 * 1024);
        let entries = store.pool.layout().planned_max_entries as usize;
        for index in 0..entries {
            assert_ne!(
                store
                    .insert(&key(index as u64), &[7; 16], CachePriority::Normal)
                    .unwrap(),
                InsertOutcome::Rejected
            );
        }
        store.sync().unwrap();
        assert_eq!(
            store.pool.allocate(CachePriority::Normal, 1).unwrap(),
            AllocationResult::ReclaimRequired
        );
        let (victim, is_current) = store.pool.reclaim_candidates().oldest(CachePriority::Normal).unwrap();
        assert!(!is_current);
        store.pool.begin_reclaim(victim).unwrap();
        assert!(store.pool.pending_reclaim().is_some());
        drop(store);

        let reopened = ExtentStore::open_with_options(dir.path(), options()).unwrap();
        assert!(reopened.pool.pending_reclaim().is_none());
        assert_ne!(
            reopened.insert(&key(100_000), &[8; 16], CachePriority::Normal).unwrap(),
            InsertOutcome::Rejected
        );
        reopened.sync().unwrap();
        assert_eq!(reopened.get(&key(100_000)).unwrap(), Some(vec![8; 16]));
    }

    #[test]
    fn process_crash_during_checkpoint_preserves_committed_entries() {
        for crash_at in [
            "extent_after_payload_sync",
            "extent_after_allocator_state",
            "entry_index_after_wal_sync",
            "extent_after_index_checkpoint",
        ] {
            let dir = tempdir().unwrap();
            let store = store(dir.path(), 32 * 1024 * 1024);
            store.insert(&key(1), &[1; 32], CachePriority::Normal).unwrap();
            store.sync().unwrap();
            drop(store);

            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("store::core::tests::checkpoint_crash_child")
                .arg("--nocapture")
                .env("EXTENT_STORE_CRASH_AT", crash_at)
                .env("EXTENT_STORE_CRASH_PATH", dir.path())
                .output()
                .unwrap();
            assert!(!output.status.success(), "child did not crash at {crash_at}");

            let reopened = ExtentStore::open_with_options(
                dir.path(),
                options()
                    .with_index_write_buffer_size(PAGE_SIZE * 4)
                    .with_index_cache_size(1024 * 1024),
            )
            .unwrap();
            assert_eq!(reopened.get(&key(1)).unwrap(), Some(vec![1; 32]));
            let recovered = reopened.get(&key(2)).unwrap();
            assert!(recovered.is_none() || recovered == Some(vec![2; 32]));
        }
    }

    #[test]
    fn checkpoint_crash_child() {
        let Ok(path) = std::env::var("EXTENT_STORE_CRASH_PATH") else {
            return;
        };
        let store = ExtentStore::open_with_options(path, options()).unwrap();
        store.insert(&key(2), &[2; 32], CachePriority::Normal).unwrap();
        store.sync().unwrap();
        panic!("crash failpoint was not reached");
    }

    #[test]
    fn process_crash_after_generation_invalidation_recovers_a_valid_store() {
        let dir = tempdir().unwrap();
        let store = store(dir.path(), 2 * 1024 * 1024);
        let entries = store.pool.layout().planned_max_entries;
        for index in 0..entries {
            assert_ne!(
                store.insert(&key(index), &[7; 16], CachePriority::Normal).unwrap(),
                InsertOutcome::Rejected
            );
        }
        store.sync().unwrap();
        drop(store);

        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("store::core::tests::reclaim_crash_child")
            .arg("--nocapture")
            .env("EXTENT_STORE_CRASH_AT", "extent_reclaim_after_generation_invalidation")
            .env("EXTENT_STORE_CRASH_PATH", dir.path())
            .output()
            .unwrap();
        assert!(!output.status.success(), "child did not crash after invalidation");

        let reopened = ExtentStore::open_with_options(dir.path(), options()).unwrap();
        assert!(reopened.pool.pending_reclaim().is_none());
        for index in 0..entries {
            let recovered = reopened.get(&key(index)).unwrap();
            assert!(recovered.is_none() || recovered == Some(vec![7; 16]));
        }
        assert_ne!(
            reopened.insert(&key(200_000), &[9; 16], CachePriority::Normal).unwrap(),
            InsertOutcome::Rejected
        );
        reopened.sync().unwrap();
        assert_eq!(reopened.get(&key(200_000)).unwrap(), Some(vec![9; 16]));
    }

    #[test]
    fn reclaim_crash_child() {
        let Ok(path) = std::env::var("EXTENT_STORE_CRASH_PATH") else {
            return;
        };
        let store = ExtentStore::open_with_options(path, options()).unwrap();
        store.insert(&key(100_000), &[8; 16], CachePriority::Normal).unwrap();
        panic!("crash failpoint was not reached");
    }
}
