use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

#[cfg(test)]
use std::sync::atomic::{AtomicU8, Ordering};

#[cfg(test)]
use crate::model::CachePriority;
#[cfg(test)]
use crate::segment::reclaim::promotion_limit;
use crate::{
    error::{Error, Result},
    format::{blob_checksum, stored_blob_len},
    model::{BlobKey, KeyDigest},
    segment::{
        checkpoint::{CheckpointCoordinator, CheckpointStats},
        config::{SegmentEngineConfig, SegmentEngineOptions},
        format::{SegmentLayout, SegmentLocation},
        index::{INDEX_DIRECTORY, IndexReadStats, IndexStats, SegmentIndex},
        io::IoSchedulerStats,
        operation::{BatchInsertResult, BlobInsert, GetResult, InsertOutcome},
        reclaim::{AllocationDecision, ReclaimResult, Reclaimer},
        stats::{PhysicalWriteStats, PriorityOccupancy},
        store::{DATA_FILE, OWNER_FILE, STATE_FILE, SegmentAllocation, SegmentStore, SegmentWrite},
    },
};

#[derive(Debug)]
pub struct SegmentEngine {
    index: Arc<SegmentIndex>,
    store: Arc<SegmentStore>,
    layout: SegmentLayout,
    options: SegmentEngineOptions,
    mutations: Arc<Mutex<()>>,
    checkpoints: CheckpointCoordinator,
    #[cfg(test)]
    injected_fault: AtomicU8,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum InjectedFault {
    WriteNoSpace = 1,
    WriteZero = 2,
    Sync = 3,
}

impl SegmentEngine {
    pub fn create(path: impl AsRef<Path>, config: SegmentEngineConfig) -> Result<Self> {
        validate_options(config.options)?;
        let layout = SegmentLayout::create(config)?;
        validate_layout_options(layout, config.options)?;
        let root = path.as_ref();
        fs::create_dir_all(root).map_err(|error| Error::io("create segment engine directory", error))?;
        let index = SegmentIndex::create(
            root,
            layout.usable_entries,
            layout.index_capacity_bytes,
            config.options.index_write_buffer_size,
            config.options.index_cache_size,
        )?;
        let store = SegmentStore::create(
            root,
            layout,
            config.options.direct_io,
            config.options.write_concurrency,
            config.options.io_read_priority_duration,
            config.options.read_run_size,
            config.options.write_run_size,
        )?;
        Self::from_parts(index, store, config.options)
    }

    pub fn recreate(path: impl AsRef<Path>, config: SegmentEngineConfig) -> Result<Self> {
        let root = path.as_ref();
        fs::create_dir_all(root).map_err(|error| Error::io("create segment engine directory", error))?;
        remove_owned_directory(&root.join(INDEX_DIRECTORY))?;
        for file in [DATA_FILE, OWNER_FILE, STATE_FILE] {
            remove_owned_file(&root.join(file))?;
        }
        Self::create(root, config)
    }

    pub fn open_with_options(path: impl AsRef<Path>, options: SegmentEngineOptions) -> Result<Self> {
        validate_options(options)?;
        let root = path.as_ref();
        let store = SegmentStore::open(
            root,
            options.direct_io,
            options.write_concurrency,
            options.io_read_priority_duration,
            options.read_run_size,
            options.write_run_size,
        )?;
        validate_layout_options(store.layout(), options)?;
        let index = SegmentIndex::open(
            root,
            store.layout().usable_entries,
            store.layout().index_capacity_bytes,
            options.index_write_buffer_size,
            options.index_cache_size,
        )?;
        if index.file_size() != store.layout().index_capacity_bytes {
            return Err(Error::InvalidSuperblock(
                "segment index layout does not match allocator state".to_string(),
            ));
        }
        let engine = Self::from_parts(index, store, options)?;
        engine.reclaimer().recover_pending()?;
        Ok(engine)
    }

    fn from_parts(index: SegmentIndex, store: SegmentStore, options: SegmentEngineOptions) -> Result<Self> {
        let layout = store.layout();
        let index = Arc::new(index);
        let store = Arc::new(store);
        let mutations = Arc::new(Mutex::new(()));
        let dirty_changes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let checkpoints = CheckpointCoordinator::new(index.clone(), store.clone(), mutations.clone(), dirty_changes)?;
        Ok(Self {
            index,
            store,
            layout,
            options,
            mutations,
            checkpoints,
            #[cfg(test)]
            injected_fault: AtomicU8::new(0),
        })
    }

    pub const fn slot_size(&self) -> usize {
        self.layout.slot_size
    }

    pub const fn file_size(&self) -> u64 {
        self.layout.total_file_size
    }

    pub fn allocated_size(&self) -> Result<u64> {
        self.index
            .allocated_size()?
            .checked_add(self.store.allocated_size()?)
            .ok_or_else(|| Error::InvalidConfig("segment engine allocated size overflows u64".to_string()))
    }

    pub fn physical_write_stats(&self) -> PhysicalWriteStats {
        let mut stats = self.store.physical_write_stats();
        stats.merge(self.index.physical_write_stats());
        stats
    }

    pub fn io_scheduler_stats(&self) -> IoSchedulerStats {
        self.store.io_scheduler_stats()
    }

    pub fn index_stats(&self) -> IndexStats {
        self.index.stats()
    }

    pub fn index_read_stats(&self) -> IndexReadStats {
        self.index.read_stats()
    }

    pub fn checkpoint_stats(&self) -> CheckpointStats {
        self.checkpoints.stats()
    }

    pub fn priority_occupancy(&self) -> PriorityOccupancy {
        self.store.priority_occupancy(self.priority_capacity_floors())
    }

    #[cfg(all(test, target_os = "linux"))]
    pub const fn direct_io(&self) -> bool {
        self.options.direct_io
    }

    #[cfg(test)]
    pub fn get(&self, key: &BlobKey) -> Result<Option<Vec<u8>>> {
        self.get_with_stats(key).map(|result| result.value)
    }

    pub fn get_with_stats(&self, key: &BlobKey) -> Result<GetResult> {
        let key_digest = KeyDigest::for_key(key);
        let Some(location) = self.index.get(key_digest)? else {
            return Ok(GetResult::default());
        };
        let stored = self.store.get_blob(key, location)?;
        Ok(GetResult {
            priority: stored.value.as_ref().map(|_| location.priority),
            value: stored.value,
            data_slots: stored.data_slots,
            data_runs: stored.data_runs,
            data_bytes: stored.data_bytes,
        })
    }

    #[cfg(test)]
    pub fn insert(&self, key: &BlobKey, value: &[u8], priority: CachePriority) -> Result<InsertOutcome> {
        let result = self.insert_batch_with_stats(&[BlobInsert::new(key, value, priority)])?;
        Ok(result
            .outcomes
            .into_iter()
            .next()
            .expect("single segment insert must have one outcome"))
    }

    #[cfg(test)]
    pub fn insert_batch(&self, inserts: &[BlobInsert<'_>]) -> Result<Vec<InsertOutcome>> {
        self.insert_batch_with_stats(inserts).map(|result| result.outcomes)
    }

    pub fn insert_batch_with_stats(&self, inserts: &[BlobInsert<'_>]) -> Result<BatchInsertResult> {
        #[cfg(test)]
        self.inject_insert_fault()?;
        for insert in inserts {
            self.validate_blob(insert.key, insert.value)?;
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
        let mut published_changes = 0usize;
        let mut input_index = 0usize;

        while input_index < inserts.len() {
            let mut known = HashMap::new();
            let mut pending = Vec::new();
            let mut protected_segments = HashSet::new();

            while input_index < inserts.len() {
                let insert = inserts[input_index];
                let key_digest = KeyDigest::for_key(insert.key);
                let stored_len =
                    stored_blob_len(insert.key, insert.value).expect("validated stored blob length must fit usize");
                let checksum = blob_checksum(insert.key, insert.value);
                let (current, would_admit) = if let Some(location) = known.get(&key_digest) {
                    (Some(*location), true)
                } else {
                    self.index.probe(key_digest, insert.priority)?
                };
                if let Some(current) = current {
                    if current.stored_len as usize == stored_len
                        && current.checksum == checksum
                        && insert.priority == current.priority
                    {
                        outcomes[input_index] = Some(InsertOutcome::Updated);
                        input_index += 1;
                        continue;
                    }
                } else if !would_admit {
                    outcomes[input_index] = Some(InsertOutcome::Rejected);
                    input_index += 1;
                    continue;
                }

                let stored_priority = insert.priority;
                let slots = self.slots_for_len(stored_len);
                let allocation = match self.reclaimer().allocate(stored_priority, slots, &protected_segments)? {
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
                let location = SegmentLocation {
                    physical_slot: allocation.physical_slot,
                    segment_generation: allocation.segment_generation,
                    stored_len: u32::try_from(stored_len).expect("validated stored blob length must fit u32"),
                    checksum,
                    priority: stored_priority,
                };
                known.insert(key_digest, location);
                protected_segments.insert(allocation.segment);
                pending.push(PendingInsert {
                    input_index,
                    allocation,
                    key: insert.key,
                    key_digest,
                    value: insert.value,
                    checksum,
                });
                input_index += 1;
            }

            if pending.is_empty() {
                assert_eq!(
                    input_index,
                    inserts.len(),
                    "a segment batch boundary must publish at least one pending insert"
                );
                continue;
            }

            let writes = pending
                .iter()
                .map(|pending| SegmentWrite {
                    allocation: pending.allocation,
                    key: pending.key,
                    key_digest: pending.key_digest,
                    value: pending.value,
                    checksum: pending.checksum,
                })
                .collect::<Vec<_>>();
            let physical = self.store.write_batch(&writes)?;
            let index_inserts = pending
                .iter()
                .zip(&physical.locations)
                .map(|(pending, location)| (pending.key_digest, *location))
                .collect::<Vec<_>>();
            let indexed = self.index.insert_batch(&index_inserts)?;
            let mut changed_slots = 0usize;
            for (pending, outcome) in pending.iter().zip(indexed.outcomes) {
                if outcome != InsertOutcome::Rejected {
                    changed_slots = changed_slots.saturating_add(pending.allocation.slots as usize);
                }
                outcomes[pending.input_index] = Some(outcome);
            }
            published_changes = published_changes.saturating_add(changed_slots);
            write_runs = write_runs
                .saturating_add(physical.data_runs)
                .saturating_add(physical.owner_runs)
                .saturating_add(indexed.write_runs);
            written_bytes = written_bytes
                .saturating_add(physical.data_bytes)
                .saturating_add(physical.owner_bytes)
                .saturating_add(indexed.written_bytes);
        }

        if published_changes > 0 {
            // Payload and owner durability is the publication fence. Checkpoint epochs therefore
            // persist only immutable allocator/index metadata and never race fdatasync with later
            // buffered writes to the same monolithic files.
            self.store.sync_payload()?;
            #[cfg(test)]
            crate::segment::crash_if_requested("segment_after_payload_sync");
            self.checkpoints.record_publication(published_changes)?;
        }
        let checkpoint_target = self.checkpoints.published_epoch();
        if self.checkpoints.dirty_changes() >= self.options.checkpoint_changes {
            self.checkpoints.request_background(checkpoint_target)?;
        }
        drop(mutation);

        Ok(BatchInsertResult {
            outcomes: outcomes
                .into_iter()
                .map(|outcome| outcome.expect("every segment insert must have an outcome"))
                .collect(),
            write_runs: write_runs.saturating_add(reclaim.write_runs),
            written_bytes: written_bytes.saturating_add(reclaim.written_bytes),
            reclaim: reclaim.stats,
        })
    }

    pub fn remove(&self, key: &BlobKey) -> Result<bool> {
        let mutation = mutex_lock(&self.mutations);
        self.checkpoints.ensure_healthy()?;
        let removed = self.index.remove(KeyDigest::for_key(key))?;
        if removed {
            let target = self.checkpoints.record_publication(1)?;
            if self.checkpoints.dirty_changes() >= self.options.checkpoint_changes {
                self.checkpoints.request_background(target)?;
            }
            drop(mutation);
        }
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
                "injected segment sync",
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
                    "injected segment data write",
                    std::io::Error::new(std::io::ErrorKind::StorageFull, "injected no-space write"),
                ))
            }
            fault if fault == InjectedFault::WriteZero as u8 => {
                self.injected_fault.store(0, Ordering::Release);
                Err(Error::io(
                    "injected segment data write",
                    std::io::Error::new(std::io::ErrorKind::WriteZero, "injected short write"),
                ))
            }
            _ => Ok(()),
        }
    }

    fn validate_blob(&self, key: &BlobKey, value: &[u8]) -> Result<()> {
        if value.is_empty() {
            return Err(Error::EmptyValue);
        }
        let Some(stored_len) = stored_blob_len(key, value) else {
            return Err(Error::ValueTooLarge {
                len: usize::MAX,
                maximum: self.store.layout().segment_size,
            });
        };
        if stored_len > self.store.layout().segment_size {
            return Err(Error::ValueTooLarge {
                len: stored_len,
                maximum: self.store.layout().segment_size,
            });
        }
        Ok(())
    }

    fn slots_for_len(&self, len: usize) -> u32 {
        u32::try_from(len.div_ceil(self.slot_size())).expect("a validated segment value must use at most u32 slots")
    }

    fn reclaimer(&self) -> Reclaimer<'_> {
        Reclaimer::new(
            &self.index,
            &self.store,
            &self.checkpoints,
            self.slot_size(),
            self.priority_capacity_floors(),
            self.options.hot_frequency,
            self.options.low_hot_frequency,
        )
    }

    fn priority_capacity_floors(&self) -> [u32; 3] {
        self.options
            .priority_capacity_floors
            .segment_floors(self.layout.segment_count.saturating_sub(1))
    }
}

fn remove_owned_file(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io("remove previous segment engine file", error)),
    }
}

fn remove_owned_directory(path: &Path) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(Error::io("inspect previous segment index", error)),
    };
    let result = if metadata.file_type().is_symlink() || !metadata.is_dir() {
        fs::remove_file(path)
    } else {
        fs::remove_dir_all(path)
    };
    result.map_err(|error| Error::io("remove previous segment index", error))
}

#[derive(Debug, Clone, Copy)]
struct PendingInsert<'a> {
    input_index: usize,
    allocation: SegmentAllocation,
    key: &'a BlobKey,
    key_digest: KeyDigest,
    value: &'a [u8],
    checksum: u32,
}

fn validate_options(options: SegmentEngineOptions) -> Result<()> {
    if !(1..=64).contains(&options.write_concurrency) {
        return Err(Error::InvalidConfig(
            "segment write_concurrency must be between 1 and 64".to_string(),
        ));
    }
    if options.read_run_size == 0 {
        return Err(Error::InvalidConfig(
            "segment read_run_size must be greater than zero".to_string(),
        ));
    }
    if options.write_run_size == 0 {
        return Err(Error::InvalidConfig(
            "segment write_run_size must be greater than zero".to_string(),
        ));
    }
    if options.checkpoint_changes == 0 {
        return Err(Error::InvalidConfig(
            "segment checkpoint_changes must be greater than zero".to_string(),
        ));
    }
    if options.index_write_buffer_size < fixed_lsm::KEY_SIZE + fixed_lsm::VALUE_SIZE + 8 {
        return Err(Error::InvalidConfig(
            "segment index_write_buffer_size must fit one fixed-LSM record".to_string(),
        ));
    }
    if options.index_cache_size == 0 {
        return Err(Error::InvalidConfig(
            "segment index_cache_size must be greater than zero".to_string(),
        ));
    }
    if !(1..=15).contains(&options.hot_frequency) {
        return Err(Error::InvalidConfig(
            "segment hot_frequency must be between 1 and 15".to_string(),
        ));
    }
    if !(1..=15).contains(&options.low_hot_frequency) {
        return Err(Error::InvalidConfig(
            "segment low_hot_frequency must be between 1 and 15".to_string(),
        ));
    }
    let priority_capacity_floors = options.priority_capacity_floors;
    if priority_capacity_floors.high_percent() > 100
        || priority_capacity_floors.normal_percent() > 100
        || u16::from(priority_capacity_floors.high_percent()) + u16::from(priority_capacity_floors.normal_percent())
            > 100
    {
        return Err(Error::InvalidConfig(
            "segment high and normal priority capacity floors must each be at most 100 percent and sum to at most 100"
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

fn validate_layout_options(layout: SegmentLayout, options: SegmentEngineOptions) -> Result<()> {
    if options.segment_size != layout.segment_size {
        return Err(Error::InvalidConfig(format!(
            "configured segment_size ({}) does not match the stored layout ({})",
            options.segment_size, layout.segment_size
        )));
    }
    if !options.read_run_size.is_multiple_of(layout.slot_size) {
        return Err(Error::InvalidConfig(format!(
            "segment read_run_size must be a multiple of slot_size ({})",
            layout.slot_size
        )));
    }
    if !options.write_run_size.is_multiple_of(layout.slot_size) {
        return Err(Error::InvalidConfig(format!(
            "segment write_run_size must be a multiple of slot_size ({})",
            layout.slot_size
        )));
    }
    let usable_segments = layout.segment_count.saturating_sub(1);
    let capacity_floors = options.priority_capacity_floors.segment_floors(usable_segments);
    if capacity_floors.into_iter().sum::<u32>() > usable_segments {
        return Err(Error::InvalidConfig(
            "segment priority capacity floors exceed usable segment capacity".to_string(),
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
    use crate::{
        format::PAGE_SIZE,
        segment::{format::OWNER_RECORD_SIZE, store::SegmentAllocationResult},
    };

    fn key(index: u64) -> BlobKey {
        let mut bytes = [index as u8; 24];
        bytes[16..].copy_from_slice(&index.to_le_bytes());
        BlobKey::new(bytes).unwrap()
    }

    fn full_slot_value(byte: u8) -> Vec<u8> {
        let key = key(0);
        let envelope = stored_blob_len(&key, &[]).unwrap();
        vec![byte; PAGE_SIZE - envelope]
    }

    fn options() -> SegmentEngineOptions {
        SegmentEngineOptions::default()
            .with_segment_size(PAGE_SIZE * 8)
            .with_index_write_buffer_size(PAGE_SIZE * 4)
            .with_index_cache_size(1024 * 1024)
            .with_checkpoint_changes(usize::MAX)
    }

    fn engine(root: &Path, capacity: u64) -> SegmentEngine {
        SegmentEngine::create(
            root,
            SegmentEngineConfig::new(capacity)
                .with_slot_size(PAGE_SIZE)
                .with_options(options()),
        )
        .unwrap()
    }

    #[test]
    fn insert_get_update_remove_and_reopen() {
        let dir = tempdir().unwrap();
        let engine = engine(dir.path(), 4 * 1024 * 1024);
        assert_eq!(
            engine.insert(&key(1), &[1; 100], CachePriority::Normal).unwrap(),
            InsertOutcome::Inserted
        );
        assert_eq!(engine.get(&key(1)).unwrap(), Some(vec![1; 100]));
        assert_eq!(
            engine.insert(&key(1), &[2; 200], CachePriority::High).unwrap(),
            InsertOutcome::Updated
        );
        assert_eq!(engine.get(&key(1)).unwrap(), Some(vec![2; 200]));
        engine.insert(&key(2), &[3; 50], CachePriority::Low).unwrap();
        assert!(engine.remove(&key(2)).unwrap());
        engine.sync().unwrap();
        let layout = engine.store.layout();
        drop(engine);

        let reopened = SegmentEngine::open_with_options(dir.path(), options()).unwrap();
        assert_eq!(reopened.store.layout(), layout);
        assert_eq!(reopened.get(&key(1)).unwrap(), Some(vec![2; 200]));
        assert!(reopened.get(&key(2)).unwrap().is_none());
    }

    #[test]
    fn allocated_size_tracks_index_budget_without_directory_scans() {
        let dir = tempdir().unwrap();
        let engine = engine(dir.path(), 4 * 1024 * 1024);
        let before = engine.allocated_size().unwrap();
        engine.insert(&key(1), &[1; 100], CachePriority::Normal).unwrap();
        engine.sync().unwrap();
        let after = engine.allocated_size().unwrap();
        assert!(after > before);
        assert_eq!(engine.allocated_size().unwrap(), after);
    }

    #[test]
    fn reopen_rejects_a_different_segment_size() {
        let dir = tempdir().unwrap();
        let engine = engine(dir.path(), 4 * 1024 * 1024);
        engine.sync().unwrap();
        drop(engine);

        let error =
            SegmentEngine::open_with_options(dir.path(), options().with_segment_size(PAGE_SIZE * 16)).unwrap_err();
        assert!(matches!(error, Error::InvalidConfig(_)));
    }

    #[test]
    fn index_preserves_segment_publication_and_reopens() {
        let dir = tempdir().unwrap();
        let engine = engine(dir.path(), 32 * 1024 * 1024);
        assert_eq!(
            engine.insert(&key(1), &[1; 100], CachePriority::Normal).unwrap(),
            InsertOutcome::Inserted
        );
        assert_eq!(
            engine
                .insert(&key(2), &[2; PAGE_SIZE + 17], CachePriority::High)
                .unwrap(),
            InsertOutcome::Inserted
        );
        engine.checkpoint().unwrap();
        assert_eq!(engine.index_stats().live_entries, 2);
        drop(engine);

        let engine = SegmentEngine::open_with_options(
            dir.path(),
            options()
                .with_index_write_buffer_size(PAGE_SIZE * 4)
                .with_index_cache_size(1024 * 1024),
        )
        .unwrap();
        assert_eq!(engine.get(&key(1)).unwrap(), Some(vec![1; 100]));
        assert_eq!(engine.get(&key(2)).unwrap(), Some(vec![2; PAGE_SIZE + 17]));
        assert!(engine.remove(&key(1)).unwrap());
        engine.checkpoint().unwrap();
        assert_eq!(engine.index_stats().live_entries, 1);
        drop(engine);

        let engine = SegmentEngine::open_with_options(
            dir.path(),
            options()
                .with_index_write_buffer_size(PAGE_SIZE * 4)
                .with_index_cache_size(1024 * 1024),
        )
        .unwrap();
        assert_eq!(engine.get(&key(1)).unwrap(), None);
        assert_eq!(engine.get(&key(2)).unwrap(), Some(vec![2; PAGE_SIZE + 17]));
    }

    #[test]
    fn multi_slot_blob_respects_read_run_limit_and_reopens() {
        let dir = tempdir().unwrap();
        let options = options().with_read_run_size(PAGE_SIZE);
        let engine = SegmentEngine::create(
            dir.path(),
            SegmentEngineConfig::new(4 * 1024 * 1024)
                .with_slot_size(PAGE_SIZE)
                .with_options(options),
        )
        .unwrap();
        let value = (0..PAGE_SIZE * 3 + 17)
            .map(|offset| (offset % 251) as u8)
            .collect::<Vec<_>>();

        assert_eq!(
            engine.insert(&key(7), &value, CachePriority::High).unwrap(),
            InsertOutcome::Inserted
        );
        let stats = engine.physical_write_stats();
        assert_eq!(stats.data_bytes, (PAGE_SIZE * 4) as u64);
        assert_eq!(stats.owner_bytes, (OWNER_RECORD_SIZE * 4) as u64);
        let read = engine.get_with_stats(&key(7)).unwrap();
        assert_eq!(read.value, Some(value.clone()));
        assert_eq!(read.data_slots, 4);
        assert_eq!(read.data_runs, 4);

        engine.sync().unwrap();
        drop(engine);
        let reopened = SegmentEngine::open_with_options(dir.path(), options).unwrap();
        assert_eq!(reopened.get(&key(7)).unwrap(), Some(value));
    }

    #[test]
    fn concurrent_blob_writers_preserve_every_value() {
        let dir = tempdir().unwrap();
        let engine = Arc::new(engine(dir.path(), 8 * 1024 * 1024));
        let concurrency = 4u64;
        let entries_per_writer = 32u64;

        std::thread::scope(|scope| {
            for writer in 0..concurrency {
                let engine = engine.clone();
                scope.spawn(move || {
                    for entry in 0..entries_per_writer {
                        let index = writer * entries_per_writer + entry;
                        let len = PAGE_SIZE + (index as usize % (PAGE_SIZE * 2));
                        let value = vec![index as u8; len];
                        engine.insert(&key(index), &value, CachePriority::Normal).unwrap();
                        assert_eq!(engine.get(&key(index)).unwrap(), Some(value));
                    }
                });
            }
        });
        engine.sync().unwrap();
        for index in 0..concurrency * entries_per_writer {
            let len = PAGE_SIZE + (index as usize % (PAGE_SIZE * 2));
            assert_eq!(engine.get(&key(index)).unwrap(), Some(vec![index as u8; len]));
        }
    }

    #[test]
    fn checkpoint_epoch_does_not_block_later_publication() {
        let dir = tempdir().unwrap();
        let checkpoint_options = options().with_checkpoint_changes(1);
        let engine = Arc::new(
            SegmentEngine::create(
                dir.path(),
                SegmentEngineConfig::new(4 * 1024 * 1024)
                    .with_slot_size(PAGE_SIZE)
                    .with_options(checkpoint_options),
            )
            .unwrap(),
        );
        engine.checkpoints.pause_after_capture();
        engine.insert(&key(1), &[1; PAGE_SIZE], CachePriority::Normal).unwrap();
        engine.checkpoints.wait_until_captured();

        let (sent, received) = mpsc::channel();
        let writer = {
            let engine = engine.clone();
            std::thread::spawn(move || {
                let result = engine.insert(&key(2), &[2; PAGE_SIZE], CachePriority::Normal);
                sent.send(result).unwrap();
            })
        };
        assert_ne!(
            received.recv_timeout(Duration::from_secs(1)).unwrap().unwrap(),
            InsertOutcome::Rejected
        );
        assert_eq!(engine.get(&key(2)).unwrap(), Some(vec![2; PAGE_SIZE]));
        engine.checkpoints.resume_checkpoint();
        writer.join().unwrap();
        engine.sync().unwrap();
        drop(engine);

        let reopened = SegmentEngine::open_with_options(dir.path(), checkpoint_options).unwrap();
        assert_eq!(reopened.get(&key(1)).unwrap(), Some(vec![1; PAGE_SIZE]));
        assert_eq!(reopened.get(&key(2)).unwrap(), Some(vec![2; PAGE_SIZE]));
    }

    #[test]
    fn reclaim_waits_for_an_in_flight_epoch_before_generation_reuse() {
        let dir = tempdir().unwrap();
        let engine = Arc::new(engine(dir.path(), 2 * 1024 * 1024));
        let entries = engine.store.layout().usable_entries;
        let initial = full_slot_value(7);
        for index in 0..entries - 1 {
            assert_ne!(
                engine.insert(&key(index), &initial, CachePriority::Low).unwrap(),
                InsertOutcome::Rejected
            );
        }
        engine.sync().unwrap();

        engine.checkpoints.pause_after_capture();
        let updated = full_slot_value(8);
        assert_eq!(
            engine.insert(&key(0), &updated, CachePriority::Low).unwrap(),
            InsertOutcome::Updated
        );
        engine
            .checkpoints
            .request_background(engine.checkpoints.published_epoch())
            .unwrap();
        engine.checkpoints.wait_until_captured();

        let (sent, received) = mpsc::channel();
        let writer = {
            let engine = engine.clone();
            let value = full_slot_value(9);
            std::thread::spawn(move || {
                let result = engine.insert(&key(100_000), &value, CachePriority::High);
                sent.send(result).unwrap();
            })
        };
        let early = received.recv_timeout(Duration::from_millis(50));
        engine.checkpoints.resume_checkpoint();
        assert!(
            early.is_err(),
            "reclaim reused a generation before its checkpoint became durable"
        );
        assert_ne!(
            received.recv_timeout(Duration::from_secs(1)).unwrap().unwrap(),
            InsertOutcome::Rejected
        );
        writer.join().unwrap();
        engine.sync().unwrap();
        assert_eq!(engine.get(&key(100_000)).unwrap(), Some(full_slot_value(9)));
    }

    #[test]
    fn checkpoint_failure_is_retained_and_rejects_later_mutations() {
        let dir = tempdir().unwrap();
        let checkpoint_options = options().with_checkpoint_changes(1);
        let engine = SegmentEngine::create(
            dir.path(),
            SegmentEngineConfig::new(4 * 1024 * 1024)
                .with_slot_size(PAGE_SIZE)
                .with_options(checkpoint_options),
        )
        .unwrap();
        engine.checkpoints.fail_after_capture();
        engine.insert(&key(1), &[1; PAGE_SIZE], CachePriority::Normal).unwrap();
        assert!(matches!(engine.sync(), Err(Error::CheckpointFailed(_))));
        assert_eq!(engine.get(&key(1)).unwrap(), Some(vec![1; PAGE_SIZE]));
        assert!(matches!(
            engine.insert(&key(2), &[2; PAGE_SIZE], CachePriority::Normal),
            Err(Error::CheckpointFailed(_))
        ));
        drop(engine);

        let reopened = SegmentEngine::open_with_options(dir.path(), checkpoint_options).unwrap();
        assert!(reopened.get(&key(1)).unwrap().is_none());
    }

    #[test]
    fn physical_write_stats_separate_payload_and_metadata() {
        let dir = tempdir().unwrap();
        let engine = engine(dir.path(), 4 * 1024 * 1024);
        let values = [full_slot_value(1), full_slot_value(2)];
        engine
            .insert_batch(&[
                BlobInsert::new(&key(1), &values[0], CachePriority::Normal),
                BlobInsert::new(&key(2), &values[1], CachePriority::Normal),
            ])
            .unwrap();

        let before_checkpoint = engine.physical_write_stats();
        assert_eq!(before_checkpoint.data_runs, 1);
        assert_eq!(before_checkpoint.data_bytes, (PAGE_SIZE * 2) as u64);
        assert_eq!(before_checkpoint.owner_runs, 1);
        assert_eq!(before_checkpoint.owner_bytes, (OWNER_RECORD_SIZE * 2) as u64);
        assert_eq!(before_checkpoint.index_runs, 0);
        assert_eq!(before_checkpoint.allocator_runs, 0);

        engine.checkpoint().unwrap();
        let after_checkpoint = engine.physical_write_stats();
        assert!(after_checkpoint.index_runs > 0);
        assert!(after_checkpoint.index_bytes > 0);
        assert_eq!(after_checkpoint.allocator_runs, 1);
        assert_eq!(
            after_checkpoint.allocator_bytes,
            engine.store.layout().state_copy_size as u64
        );
        assert_eq!(
            after_checkpoint.total_bytes(),
            after_checkpoint.data_bytes
                + after_checkpoint.owner_bytes
                + after_checkpoint.index_bytes
                + after_checkpoint.allocator_bytes
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn direct_io_handles_partial_slots_and_reopens() {
        let dir = tempdir().unwrap();
        let options = options().with_direct_io(true);
        let engine = match SegmentEngine::create(
            dir.path(),
            SegmentEngineConfig::new(2 * 1024 * 1024)
                .with_slot_size(PAGE_SIZE)
                .with_options(options),
        ) {
            Ok(engine) => engine,
            Err(Error::Io { source, .. })
                if matches!(
                    source.kind(),
                    std::io::ErrorKind::InvalidInput | std::io::ErrorKind::Unsupported
                ) =>
            {
                return;
            }
            Err(error) => panic!("failed to create direct I/O segment engine: {error}"),
        };

        assert!(engine.direct_io());
        let value = vec![9; PAGE_SIZE / 2 + 17];
        let full = full_slot_value(8);
        engine.insert(&key(1), &value, CachePriority::Normal).unwrap();
        engine.insert(&key(2), &full, CachePriority::Normal).unwrap();
        let partial_read = engine.get_with_stats(&key(1)).unwrap();
        assert_eq!(partial_read.value, Some(value.clone()));
        assert_eq!(partial_read.data_slots, 1);
        assert_eq!(partial_read.data_runs, 1);
        assert_eq!(partial_read.data_bytes, PAGE_SIZE);
        let full_read = engine.get_with_stats(&key(2)).unwrap();
        assert_eq!(full_read.value, Some(full.clone()));
        assert_eq!(full_read.data_slots, 1);
        assert_eq!(full_read.data_runs, 1);
        assert_eq!(full_read.data_bytes, PAGE_SIZE);
        engine.sync().unwrap();
        drop(engine);

        let engine = SegmentEngine::open_with_options(dir.path(), options).unwrap();
        assert_eq!(engine.get(&key(1)).unwrap(), Some(value));
        assert_eq!(engine.get(&key(2)).unwrap(), Some(full));
    }

    #[test]
    fn batch_is_sequential_and_preserves_same_key_order() {
        let dir = tempdir().unwrap();
        let engine = engine(dir.path(), 4 * 1024 * 1024);
        let first = vec![1; 256];
        let second = vec![2; 512];
        let third = vec![3; 768];
        let result = engine
            .insert_batch_with_stats(&[
                BlobInsert::new(&key(1), &first, CachePriority::Low),
                BlobInsert::new(&key(2), &second, CachePriority::Normal),
                BlobInsert::new(&key(1), &third, CachePriority::High),
            ])
            .unwrap();
        assert_eq!(result.outcomes.len(), 3);
        assert_eq!(engine.get(&key(1)).unwrap(), Some(third));
        assert_eq!(engine.get(&key(2)).unwrap(), Some(second));
        assert!(result.write_runs <= 8);
    }

    #[test]
    fn batch_larger_than_capacity_never_reuses_an_unpublished_segment() {
        let dir = tempdir().unwrap();
        let engine = engine(dir.path(), 2 * 1024 * 1024);
        let inserts = engine.store.layout().usable_entries as usize * 2;
        let values = (0..inserts)
            .map(|index| vec![(index % 251) as u8; 16])
            .collect::<Vec<_>>();
        let keys = (0..inserts).map(|index| key(index as u64)).collect::<Vec<_>>();
        let batch = values
            .iter()
            .zip(&keys)
            .map(|(value, key)| BlobInsert::new(key, value, CachePriority::Normal))
            .collect::<Vec<_>>();

        let result = engine.insert_batch_with_stats(&batch).unwrap();
        assert_eq!(result.outcomes.len(), inserts);
        for (index, expected) in values.iter().enumerate() {
            if let Some(value) = engine.get(&key(index as u64)).unwrap() {
                assert_eq!(&value, expected);
            }
        }
    }

    #[test]
    fn hot_update_batch_larger_than_capacity_preserves_fifo_values() {
        let dir = tempdir().unwrap();
        let engine = engine(dir.path(), 512 * 1024);
        let entries = engine.store.layout().usable_entries as usize;
        for index in 0..entries {
            engine
                .insert(&key(index as u64), &[7; 16], CachePriority::Normal)
                .unwrap();
        }
        for index in 0..entries {
            for _ in 0..3 {
                assert_eq!(engine.get(&key(index as u64)).unwrap(), Some(vec![7; 16]));
            }
        }

        let values = (0..entries * 2)
            .map(|index| vec![(index / entries + 1) as u8; 16])
            .collect::<Vec<_>>();
        let keys = (0..entries).map(|index| key(index as u64)).collect::<Vec<_>>();
        let batch = values
            .iter()
            .enumerate()
            .map(|(index, value)| BlobInsert::new(&keys[index % entries], value, CachePriority::Normal))
            .collect::<Vec<_>>();
        let result = engine.insert_batch_with_stats(&batch).unwrap();
        assert_eq!(result.outcomes.len(), batch.len());

        for index in 0..entries {
            if let Some(value) = engine.get(&key(index as u64)).unwrap() {
                assert_eq!(value, vec![2; 16]);
            }
        }
    }

    #[test]
    fn low_priority_cannot_reclaim_protected_data() {
        let dir = tempdir().unwrap();
        let engine = engine(dir.path(), 2 * 1024 * 1024);
        let entries = engine.store.layout().usable_entries as usize;
        for index in 0..entries {
            engine
                .insert(&key(index as u64), &[1; 16], CachePriority::High)
                .unwrap();
        }
        assert_eq!(
            engine.insert(&key(10_000), &[2; 16], CachePriority::Low).unwrap(),
            InsertOutcome::Rejected
        );
        assert!(engine.get(&key(0)).unwrap().is_some());
        assert_ne!(
            engine.insert(&key(10_001), &[3; 16], CachePriority::High).unwrap(),
            InsertOutcome::Rejected
        );
    }

    #[test]
    fn priority_capacity_is_borrowed_and_repaid_without_starvation() {
        let dir = tempdir().unwrap();
        let engine = SegmentEngine::create(
            dir.path(),
            SegmentEngineConfig::new(2 * 1024 * 1024)
                .with_slot_size(PAGE_SIZE)
                .with_options(options().with_priority_capacity_floors(25, 50)),
        )
        .unwrap();
        let layout = engine.store.layout();
        let usable_segments = layout.segment_count - 1;
        let value = vec![1; layout.segment_size - stored_blob_len(&key(0), &[]).unwrap()];
        let high_floor = engine.priority_occupancy().capacity_floor_segments(CachePriority::High);
        let normal_floor = engine
            .priority_occupancy()
            .capacity_floor_segments(CachePriority::Normal);

        for index in 0..usable_segments as usize {
            assert_ne!(
                engine.insert(&key(index as u64), &value, CachePriority::High).unwrap(),
                InsertOutcome::Rejected
            );
        }
        assert_eq!(
            engine.priority_occupancy().occupied_segments(CachePriority::High),
            usable_segments
        );

        let normal_segments = usable_segments - high_floor;
        for index in 0..normal_segments as usize {
            assert_ne!(
                engine
                    .insert(&key(100_000 + index as u64), &value, CachePriority::Normal)
                    .unwrap(),
                InsertOutcome::Rejected
            );
        }
        let occupancy = engine.priority_occupancy();
        assert_eq!(occupancy.occupied_segments(CachePriority::High), high_floor);
        assert_eq!(
            occupancy.occupied_segments(CachePriority::Normal),
            usable_segments - high_floor
        );

        let high_segments = usable_segments - normal_floor - high_floor;
        for index in 0..high_segments as usize {
            assert_ne!(
                engine
                    .insert(&key(200_000 + index as u64), &value, CachePriority::High)
                    .unwrap(),
                InsertOutcome::Rejected
            );
        }
        let occupancy = engine.priority_occupancy();
        assert_eq!(occupancy.occupied_segments(CachePriority::Normal), normal_floor);
        assert_eq!(
            occupancy.occupied_segments(CachePriority::High),
            usable_segments - normal_floor
        );
    }

    #[test]
    fn invalid_priority_capacity_is_rejected() {
        let dir = tempdir().unwrap();
        let error = SegmentEngine::create(
            dir.path(),
            SegmentEngineConfig::new(2 * 1024 * 1024)
                .with_slot_size(PAGE_SIZE)
                .with_options(options().with_priority_capacity_floors(40, 61)),
        )
        .unwrap_err();
        assert!(matches!(error, Error::InvalidConfig(_)));
    }

    #[test]
    fn lower_priority_current_is_reclaimed_before_sealed_normal_data() {
        let dir = tempdir().unwrap();
        let engine = engine(dir.path(), 512 * 1024);
        let layout = engine.store.layout();
        engine.insert(&key(0), &[1; 16], CachePriority::Low).unwrap();

        let normal_entries = (layout.segment_count as usize - 2).saturating_mul(layout.slots_per_segment as usize);
        for index in 0..normal_entries {
            assert_ne!(
                engine
                    .insert(&key(index as u64 + 1), &[2; 16], CachePriority::Normal)
                    .unwrap(),
                InsertOutcome::Rejected
            );
        }

        let result = engine
            .insert_batch_with_stats(&[
                BlobInsert::new(&key(0), &[9; 16], CachePriority::Low),
                BlobInsert::new(&key(100_000), &[3; 16], CachePriority::Normal),
            ])
            .unwrap();
        assert!(
            result
                .outcomes
                .iter()
                .all(|outcome| *outcome != InsertOutcome::Rejected)
        );
        assert_eq!(result.reclaim.reclaimed_segments(CachePriority::Low), 1);
        assert_eq!(result.reclaim.evicted_entries(CachePriority::Low), 1);
        assert_eq!(result.reclaim.evicted_bytes(CachePriority::Low), 16);
        assert_eq!(engine.get(&key(0)).unwrap(), None);
        assert_eq!(engine.get(&key(1)).unwrap(), Some(vec![2; 16]));
    }

    #[test]
    fn same_priority_reclaim_promotes_hot_entries() {
        let dir = tempdir().unwrap();
        let engine = engine(dir.path(), 2 * 1024 * 1024);
        let layout = engine.store.layout();
        let entries = layout.usable_entries as usize;
        for index in 0..entries {
            assert_ne!(
                engine
                    .insert(&key(index as u64), &[5; 16], CachePriority::Normal)
                    .unwrap(),
                InsertOutcome::Rejected
            );
        }
        for _ in 0..3 {
            assert_eq!(engine.get(&key(0)).unwrap(), Some(vec![5; 16]));
        }

        let incoming = key(entries as u64);
        let result = engine
            .insert_batch_with_stats(&[BlobInsert::new(&incoming, &[6; 16], CachePriority::Normal)])
            .unwrap();
        assert_ne!(result.outcomes[0], InsertOutcome::Rejected);
        assert_eq!(result.reclaim.total_reclaimed_segments(), 1);
        assert_eq!(result.reclaim.total_promoted_entries(), 1);
        assert_eq!(result.reclaim.total_promoted_bytes(), 16);
        assert_eq!(
            result.reclaim.total_evicted_entries(),
            layout.slots_per_segment as usize - 1
        );
        assert_eq!(
            result.reclaim.total_evicted_bytes(),
            (layout.slots_per_segment as usize - 1) * 16
        );
        assert_eq!(engine.get(&key(0)).unwrap(), Some(vec![5; 16]));
    }

    #[test]
    fn low_priority_hot_frequency_can_be_raised() {
        let dir = tempdir().unwrap();
        let engine = SegmentEngine::create(
            dir.path(),
            SegmentEngineConfig::new(2 * 1024 * 1024)
                .with_slot_size(PAGE_SIZE)
                .with_options(options().with_low_hot_frequency(15)),
        )
        .unwrap();
        let entries = engine.store.layout().usable_entries as usize;
        for index in 0..entries {
            assert_ne!(
                engine.insert(&key(index as u64), &[5; 16], CachePriority::Low).unwrap(),
                InsertOutcome::Rejected
            );
        }
        for _ in 0..3 {
            assert_eq!(engine.get(&key(0)).unwrap(), Some(vec![5; 16]));
        }

        let incoming = key(entries as u64);
        let result = engine
            .insert_batch_with_stats(&[BlobInsert::new(&incoming, &[6; 16], CachePriority::Low)])
            .unwrap();
        assert_ne!(result.outcomes[0], InsertOutcome::Rejected);
        assert_eq!(result.reclaim.total_reclaimed_segments(), 1);
        assert_eq!(result.reclaim.total_promoted_entries(), 0);
        assert_eq!(engine.get(&key(0)).unwrap(), None);
    }

    #[test]
    fn low_priority_hot_frequency_is_independent() {
        let dir = tempdir().unwrap();
        let engine = SegmentEngine::create(
            dir.path(),
            SegmentEngineConfig::new(2 * 1024 * 1024)
                .with_slot_size(PAGE_SIZE)
                .with_options(options().with_hot_frequency(15).with_low_hot_frequency(2)),
        )
        .unwrap();
        let entries = engine.store.layout().usable_entries as usize;
        for index in 0..entries {
            assert_ne!(
                engine.insert(&key(index as u64), &[5; 16], CachePriority::Low).unwrap(),
                InsertOutcome::Rejected
            );
        }
        for _ in 0..3 {
            assert_eq!(engine.get(&key(0)).unwrap(), Some(vec![5; 16]));
        }

        let incoming = key(entries as u64);
        let result = engine
            .insert_batch_with_stats(&[BlobInsert::new(&incoming, &[6; 16], CachePriority::Low)])
            .unwrap();
        assert_ne!(result.outcomes[0], InsertOutcome::Rejected);
        assert_eq!(result.reclaim.total_promoted_entries(), 1);
        assert_eq!(engine.get(&key(0)).unwrap(), Some(vec![5; 16]));
    }

    #[test]
    fn same_priority_reclaim_caps_hot_promotion() {
        let dir = tempdir().unwrap();
        let engine = engine(dir.path(), 2 * 1024 * 1024);
        let layout = engine.store.layout();
        let entries = layout.usable_entries as usize;
        for index in 0..entries {
            assert_ne!(
                engine
                    .insert(&key(index as u64), &[5; 16], CachePriority::Normal)
                    .unwrap(),
                InsertOutcome::Rejected
            );
        }
        for index in 0..entries {
            for _ in 0..3 {
                assert_eq!(engine.get(&key(index as u64)).unwrap(), Some(vec![5; 16]));
            }
        }

        let incoming = key(entries as u64);
        let result = engine
            .insert_batch_with_stats(&[BlobInsert::new(&incoming, &[6; 16], CachePriority::Normal)])
            .unwrap();
        let promoted = promotion_limit(layout.slots_per_segment);
        assert_ne!(result.outcomes[0], InsertOutcome::Rejected);
        assert_eq!(result.reclaim.total_reclaimed_segments(), 1);
        assert_eq!(result.reclaim.total_promoted_entries(), promoted);
        assert_eq!(result.reclaim.total_promoted_bytes(), promoted * 16);
        assert_eq!(
            result.reclaim.total_evicted_entries(),
            layout.slots_per_segment as usize - promoted
        );
        assert_eq!(
            result.reclaim.total_evicted_bytes(),
            (layout.slots_per_segment as usize - promoted) * 16
        );
    }

    #[test]
    fn reopen_completes_an_interrupted_reclaim_transaction() {
        let dir = tempdir().unwrap();
        let engine = engine(dir.path(), 2 * 1024 * 1024);
        let entries = engine.store.layout().usable_entries as usize;
        for index in 0..entries {
            assert_ne!(
                engine
                    .insert(&key(index as u64), &[7; 16], CachePriority::Normal)
                    .unwrap(),
                InsertOutcome::Rejected
            );
        }
        engine.sync().unwrap();
        assert_eq!(
            engine.store.allocate(CachePriority::Normal, 1).unwrap(),
            SegmentAllocationResult::ReclaimRequired
        );
        let (victim, is_current) = engine.store.reclaim_candidates().oldest(CachePriority::Normal).unwrap();
        assert!(!is_current);
        engine.store.begin_reclaim(victim).unwrap();
        assert!(engine.store.pending_reclaim().is_some());
        drop(engine);

        let reopened = SegmentEngine::open_with_options(dir.path(), options()).unwrap();
        assert!(reopened.store.pending_reclaim().is_none());
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
            "segment_after_payload_sync",
            "segment_after_allocator_state",
            "segment_index_after_wal_sync",
            "segment_after_index_checkpoint",
        ] {
            let dir = tempdir().unwrap();
            let engine = engine(dir.path(), 32 * 1024 * 1024);
            engine.insert(&key(1), &[1; 32], CachePriority::Normal).unwrap();
            engine.sync().unwrap();
            drop(engine);

            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("segment::engine::tests::checkpoint_crash_child")
                .arg("--nocapture")
                .env("EXTENT_ENGINE_CRASH_AT", crash_at)
                .env("SEGMENT_ENGINE_CRASH_PATH", dir.path())
                .output()
                .unwrap();
            assert!(!output.status.success(), "child did not crash at {crash_at}");

            let reopened = SegmentEngine::open_with_options(
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
        let Ok(path) = std::env::var("SEGMENT_ENGINE_CRASH_PATH") else {
            return;
        };
        let engine = SegmentEngine::open_with_options(path, options()).unwrap();
        engine.insert(&key(2), &[2; 32], CachePriority::Normal).unwrap();
        engine.sync().unwrap();
        panic!("crash failpoint was not reached");
    }

    #[test]
    fn process_crash_during_compacting_reclaim_recovers_a_valid_engine() {
        for crash_at in [
            "segment_reclaim_after_begin",
            "segment_reclaim_after_payload_sync",
            "segment_reclaim_after_index_checkpoint",
        ] {
            let dir = tempdir().unwrap();
            let engine = engine(dir.path(), 2 * 1024 * 1024);
            let entries = engine.store.layout().usable_entries;
            for index in 0..entries {
                assert_ne!(
                    engine.insert(&key(index), &[7; 16], CachePriority::Normal).unwrap(),
                    InsertOutcome::Rejected
                );
            }
            engine.sync().unwrap();
            drop(engine);

            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("segment::engine::tests::reclaim_crash_child")
                .arg("--nocapture")
                .env("EXTENT_ENGINE_CRASH_AT", crash_at)
                .env("SEGMENT_ENGINE_CRASH_PATH", dir.path())
                .output()
                .unwrap();
            assert!(!output.status.success(), "child did not crash at {crash_at}");

            let reopened = SegmentEngine::open_with_options(dir.path(), options()).unwrap();
            assert!(reopened.store.pending_reclaim().is_none());
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
    }

    #[test]
    fn reclaim_crash_child() {
        let Ok(path) = std::env::var("SEGMENT_ENGINE_CRASH_PATH") else {
            return;
        };
        let engine = SegmentEngine::open_with_options(path, options()).unwrap();
        for _ in 0..3 {
            assert_eq!(engine.get(&key(0)).unwrap(), Some(vec![7; 16]));
        }
        engine.insert(&key(100_000), &[8; 16], CachePriority::Normal).unwrap();
        panic!("crash failpoint was not reached");
    }

    #[test]
    fn process_crash_during_whole_segment_eviction_recovers_a_valid_engine() {
        for crash_at in [
            "segment_evict_after_allocator_state",
            "segment_evict_after_index_checkpoint",
            "segment_evict_after_release",
        ] {
            let dir = tempdir().unwrap();
            let engine = engine(dir.path(), 2 * 1024 * 1024);
            let entries = engine.store.layout().usable_entries;
            for index in 0..entries {
                assert_ne!(
                    engine.insert(&key(index), &[5; 16], CachePriority::Low).unwrap(),
                    InsertOutcome::Rejected
                );
            }
            engine.sync().unwrap();
            drop(engine);

            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("segment::engine::tests::whole_segment_eviction_crash_child")
                .arg("--nocapture")
                .env("EXTENT_ENGINE_CRASH_AT", crash_at)
                .env("SEGMENT_ENGINE_CRASH_PATH", dir.path())
                .output()
                .unwrap();
            assert!(!output.status.success(), "child did not crash at {crash_at}");

            let reopened = SegmentEngine::open_with_options(dir.path(), options()).unwrap();
            for index in 0..entries {
                let recovered = reopened.get(&key(index)).unwrap();
                assert!(recovered.is_none() || recovered == Some(vec![5; 16]));
            }
            assert_ne!(
                reopened.insert(&key(300_000), &[6; 16], CachePriority::High).unwrap(),
                InsertOutcome::Rejected
            );
            reopened.sync().unwrap();
            assert_eq!(reopened.get(&key(300_000)).unwrap(), Some(vec![6; 16]));
        }
    }

    #[test]
    fn whole_segment_eviction_crash_child() {
        let Ok(path) = std::env::var("SEGMENT_ENGINE_CRASH_PATH") else {
            return;
        };
        let engine = SegmentEngine::open_with_options(path, options()).unwrap();
        engine.insert(&key(100_000), &[8; 16], CachePriority::High).unwrap();
        panic!("crash failpoint was not reached");
    }
}
