use std::{
    fmt,
    path::PathBuf,
    sync::{
        Arc, Mutex, MutexGuard, Weak,
        atomic::{AtomicU8, Ordering},
        mpsc::{SyncSender, TrySendError},
    },
    thread::JoinHandle,
    time::Duration,
};

use bytes::Bytes;
use foyer::{
    Age, Engine, EngineBuildContext, EngineConfig, Error as FoyerError, ErrorKind as FoyerErrorKind,
    HybridCacheProperties, IoControl, Load, Metrics, PieceRef, Populated, Spawner, StorageFilterResult, StorageUsage,
    Throttle,
};
use futures_core::future::BoxFuture;
use tokio::sync::Notify;

#[cfg(test)]
use std::sync::atomic::AtomicBool;

use crate::{
    CheckpointStats, EngineValue, Error, IndexReadStats, IndexStats, MAX_BLOB_KEY_SIZE, PhysicalWriteStats,
    ReclaimStats,
    format::BLOB_HEADER_SIZE,
    model::BlobKey,
    segment::{SegmentEngine, SegmentEngineConfig},
};

mod queue;
mod recovery;
mod stats;
mod writer;

pub use self::stats::{EngineReadStats, EngineWriteStats};
use self::{
    queue::{QueueReservation, SubmissionQueue},
    recovery::open_segment,
    stats::EngineStats,
    writer::{BackgroundError, Command, WriteWorker, sync_segment},
};

const DEFAULT_QUEUE_CAPACITY_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_QUEUE_CAPACITY_ENTRIES: usize = 65_536;
const DEFAULT_WRITE_BATCH_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_WRITE_BATCH_ENTRIES: usize = 4_096;
const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(1);
const OPEN: u8 = 0;
const CLOSING: u8 = 1;
const CLOSED: u8 = 2;
type ExtentPiece = PieceRef<Bytes, EngineValue, HybridCacheProperties>;

/// Configuration for installing Extent as a Foyer disk engine.
///
/// Capacity, slot size, and segment size define the on-disk layout and must match when reopening in
/// strict recovery mode. Queue, batching, I/O, checkpoint, frequency, and index-cache settings are
/// runtime tuning knobs and may change across reopens.
#[derive(Debug)]
pub struct ExtentEngineConfig {
    path: PathBuf,
    segment: SegmentEngineConfig,
    queue_capacity_bytes: usize,
    queue_capacity_entries: usize,
    write_batch_bytes: usize,
    write_batch_entries: usize,
    checkpoint_interval: Duration,
    throttle: Throttle,
    handle: ExtentEngineHandle,
}

impl ExtentEngineConfig {
    /// Create an Extent disk-engine configuration with balanced static defaults.
    pub fn new(path: impl Into<PathBuf>, capacity_bytes: u64) -> Self {
        Self {
            path: path.into(),
            segment: SegmentEngineConfig::new(capacity_bytes),
            queue_capacity_bytes: DEFAULT_QUEUE_CAPACITY_BYTES,
            queue_capacity_entries: DEFAULT_QUEUE_CAPACITY_ENTRIES,
            write_batch_bytes: DEFAULT_WRITE_BATCH_BYTES,
            write_batch_entries: DEFAULT_WRITE_BATCH_ENTRIES,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            throttle: Throttle::default(),
            handle: ExtentEngineHandle::default(),
        }
    }

    /// Set the physical allocation quantum. This is a static on-disk layout choice.
    pub fn with_slot_size(mut self, bytes: usize) -> Self {
        self.segment.slot_size = bytes;
        self
    }

    /// Set the physical reclaim unit. This is a static on-disk layout choice.
    pub fn with_segment_size(mut self, bytes: usize) -> Self {
        self.segment.options.segment_size = bytes;
        self
    }

    /// Set the number of concurrent data-file write runs.
    pub fn with_write_concurrency(mut self, concurrency: usize) -> Self {
        self.segment.options.write_concurrency = concurrency;
        self
    }

    /// Bound one coalesced data read.
    pub fn with_read_run_size(mut self, bytes: usize) -> Self {
        self.segment.options.read_run_size = bytes;
        self
    }

    /// Bound one coalesced data write.
    pub fn with_write_run_size(mut self, bytes: usize) -> Self {
        self.segment.options.write_run_size = bytes;
        self
    }

    /// Set the FixedRecordLSM mutable write-buffer budget.
    pub fn with_index_write_buffer_size(mut self, bytes: usize) -> Self {
        self.segment.options.index_write_buffer_size = bytes;
        self
    }

    /// Set the FixedRecordLSM metadata page-cache budget.
    pub fn with_index_cache_size(mut self, bytes: usize) -> Self {
        self.segment.options.index_cache_size = bytes;
        self
    }

    /// Set the number of index mutations between background checkpoint requests.
    pub fn with_checkpoint_changes(mut self, changes: usize) -> Self {
        self.segment.options.checkpoint_changes = changes;
        self
    }

    /// Set the maximum interval between metadata checkpoint requests.
    pub fn with_checkpoint_interval(mut self, interval: Duration) -> Self {
        self.checkpoint_interval = interval;
        self
    }

    /// Set the reuse frequency required to promote normal/high-priority entries.
    pub fn with_hot_frequency(mut self, frequency: u8) -> Self {
        self.segment.options.hot_frequency = frequency;
        self
    }

    /// Set the reuse frequency required to promote low-priority entries.
    pub fn with_low_hot_frequency(mut self, frequency: u8) -> Self {
        self.segment.options.low_hot_frequency = frequency;
        self
    }

    /// Select direct data-file I/O on supported Linux filesystems.
    pub fn with_direct_io(mut self, direct_io: bool) -> Self {
        self.segment.options.direct_io = direct_io;
        self
    }

    /// Set the hard byte limit for queued and currently flushing commands.
    pub fn with_queue_capacity_bytes(mut self, capacity: usize) -> Self {
        self.queue_capacity_bytes = capacity;
        self
    }

    /// Set the hard entry limit for queued and currently flushing commands.
    pub fn with_queue_capacity_entries(mut self, capacity: usize) -> Self {
        self.queue_capacity_entries = capacity;
        self
    }

    /// Set the target logical bytes collected into one storage batch.
    ///
    /// A single entry larger than this target is still written as one batch.
    pub fn with_write_batch_bytes(mut self, bytes: usize) -> Self {
        self.write_batch_bytes = bytes;
        self
    }

    /// Set the maximum number of commands collected into one storage batch.
    pub fn with_write_batch_entries(mut self, entries: usize) -> Self {
        self.write_batch_entries = entries;
        self
    }

    /// Set the engine-level I/O throttle.
    pub fn with_throttle(mut self, throttle: Throttle) -> Self {
        self.throttle = throttle;
        self
    }

    /// Return a read-only handle for runtime statistics after this config is built.
    pub fn handle(&self) -> ExtentEngineHandle {
        self.handle.clone()
    }

    fn validate(&self) -> foyer::Result<()> {
        if self.queue_capacity_bytes == 0 {
            return Err(config_error("extent queue byte capacity must be positive"));
        }
        if self.queue_capacity_entries == 0 {
            return Err(config_error("extent queue entry capacity must be positive"));
        }
        if self.write_batch_bytes == 0 || self.write_batch_bytes > self.queue_capacity_bytes {
            return Err(config_error(
                "extent write batch bytes must be positive and no larger than the queue",
            ));
        }
        if self.write_batch_entries == 0 || self.write_batch_entries > self.queue_capacity_entries {
            return Err(config_error(
                "extent write batch entries must be positive and no larger than the queue",
            ));
        }
        if self.checkpoint_interval.is_zero() {
            return Err(config_error("extent checkpoint interval must be positive"));
        }
        Ok(())
    }
}

/// A read-only runtime handle to an ExtentEngine built from an [`ExtentEngineConfig`].
#[derive(Clone, Default)]
pub struct ExtentEngineHandle {
    inner: Arc<Mutex<Option<Weak<Inner>>>>,
}

impl fmt::Debug for ExtentEngineHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExtentEngineHandle")
            .field("attached", &self.upgrade().is_some())
            .finish()
    }
}

impl ExtentEngineHandle {
    fn attach(&self, inner: &Arc<Inner>) {
        *mutex_lock(&self.inner) = Some(Arc::downgrade(inner));
    }

    fn upgrade(&self) -> Option<Arc<Inner>> {
        mutex_lock(&self.inner).as_ref()?.upgrade()
    }

    pub fn file_size(&self) -> Option<u64> {
        self.upgrade().map(|inner| inner.segment.file_size())
    }

    pub fn allocated_size(&self) -> Option<u64> {
        self.upgrade()?.segment.allocated_size().ok()
    }

    pub fn physical_write_stats(&self) -> Option<PhysicalWriteStats> {
        self.upgrade().map(|inner| inner.segment.physical_write_stats())
    }

    pub fn index_stats(&self) -> Option<IndexStats> {
        self.upgrade().map(|inner| inner.segment.index_stats())
    }

    pub fn index_read_stats(&self) -> Option<IndexReadStats> {
        self.upgrade().map(|inner| inner.segment.index_read_stats())
    }

    pub fn reclaim_stats(&self) -> Option<ReclaimStats> {
        self.upgrade().map(|inner| inner.stats.reclaim())
    }

    pub fn read_stats(&self) -> Option<EngineReadStats> {
        self.upgrade().map(|inner| inner.stats.reads())
    }

    pub fn write_stats(&self) -> Option<EngineWriteStats> {
        self.upgrade().map(|inner| inner.stats.writes())
    }

    pub fn checkpoint_stats(&self) -> Option<CheckpointStats> {
        self.upgrade().map(|inner| inner.segment.checkpoint_stats())
    }

    pub fn background_error(&self) -> Option<String> {
        self.upgrade()?.background_error.message()
    }

    pub fn queue_capacity_entries(&self) -> Option<usize> {
        self.upgrade().map(|inner| inner.queue.capacity_entries())
    }

    pub fn queue_pending_entries(&self) -> Option<usize> {
        self.upgrade().map(|inner| inner.queue.pending_entries())
    }

    pub fn queue_capacity_bytes(&self) -> Option<usize> {
        self.upgrade().map(|inner| inner.queue.capacity_bytes())
    }

    pub fn queue_pending_bytes(&self) -> Option<usize> {
        self.upgrade().map(|inner| inner.queue.pending_bytes())
    }

    pub fn write_latency_samples(&self) -> Option<(Vec<Duration>, Vec<Duration>)> {
        self.upgrade().map(|inner| inner.stats.write_latency_samples())
    }

    #[cfg(test)]
    fn inject_segment_fault(&self, fault: crate::segment::InjectedFault) {
        self.upgrade()
            .expect("Extent engine handle must be attached")
            .segment
            .inject_fault(fault);
    }

    #[cfg(test)]
    fn panic_next_write(&self) {
        self.upgrade()
            .expect("Extent engine handle must be attached")
            .panic_next
            .store(true, Ordering::Release);
    }
}

impl EngineConfig<Bytes, EngineValue, HybridCacheProperties> for ExtentEngineConfig {
    fn build(
        self: Box<Self>,
        ctx: EngineBuildContext,
    ) -> BoxFuture<'static, foyer::Result<Arc<dyn Engine<Bytes, EngineValue, HybridCacheProperties>>>> {
        Box::pin(async move {
            self.validate()?;
            let capacity = usize::try_from(self.segment.capacity_bytes)
                .map_err(|_| config_error("extent capacity does not fit usize"))?;
            let io_control = IoControl::new(self.throttle.clone());
            let path = self.path.clone();
            let segment_config = self.segment;
            let recover_mode = ctx.recover_mode;
            let open = ctx
                .spawner
                .spawn_blocking(move || open_segment(&path, segment_config, recover_mode))
                .await?;
            let segment = Arc::new(open.map_err(|error| extent_error("open Extent engine", error))?);
            let handle = self.handle.clone();
            let engine = ExtentEngine::start(segment, *self, capacity, io_control, ctx.spawner, ctx.metrics)?;
            handle.attach(&engine.inner);
            Ok(Arc::new(engine) as Arc<dyn Engine<Bytes, EngineValue, HybridCacheProperties>>)
        })
    }
}

/// The internal Foyer engine implementation built by [`ExtentEngineConfig`].
struct ExtentEngine {
    inner: Arc<Inner>,
}

impl fmt::Debug for ExtentEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExtentEngine")
            .field("path", &self.inner.path)
            .field("queue_capacity_bytes", &self.inner.queue.capacity_bytes())
            .field("pending", &self.inner.queue.pending_entries())
            .field("pending_bytes", &self.inner.queue.pending_bytes())
            .finish()
    }
}

struct Inner {
    path: PathBuf,
    segment: Arc<SegmentEngine>,
    capacity: usize,
    segment_size: usize,
    sender: Mutex<Option<SyncSender<Command>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    queue: Arc<SubmissionQueue>,
    spawner: Spawner,
    io_control: IoControl,
    metrics: Arc<Metrics>,
    background_error: Arc<BackgroundError>,
    stats: Arc<EngineStats>,
    close_state: AtomicU8,
    close_notify: Notify,
    #[cfg(test)]
    panic_next: Arc<AtomicBool>,
}

impl ExtentEngine {
    fn start(
        segment: Arc<SegmentEngine>,
        config: ExtentEngineConfig,
        capacity: usize,
        io_control: IoControl,
        spawner: Spawner,
        metrics: Arc<Metrics>,
    ) -> foyer::Result<Self> {
        let (sender, receiver) = std::sync::mpsc::sync_channel(config.queue_capacity_entries);
        let queue = Arc::new(SubmissionQueue::new(
            config.queue_capacity_entries,
            config.queue_capacity_bytes,
            metrics.clone(),
        ));
        let background_error = Arc::new(BackgroundError::new(metrics.clone()));
        metrics.storage_engine_healthy.absolute(1);
        let stats = Arc::new(EngineStats::new(metrics.clone()));
        stats.record_checkpoint(segment.checkpoint_stats());
        #[cfg(test)]
        let panic_next = Arc::new(AtomicBool::new(false));
        let worker = WriteWorker::new(
            segment.clone(),
            io_control.statistics().clone(),
            config.write_batch_entries,
            config.write_batch_bytes,
            config.checkpoint_interval,
            background_error.clone(),
            stats.clone(),
            #[cfg(test)]
            panic_next.clone(),
        )
        .spawn(receiver)
        .map_err(|error| FoyerError::new(FoyerErrorKind::External, "start Extent flush worker").with_source(error))?;

        Ok(Self {
            inner: Arc::new(Inner {
                path: config.path,
                capacity,
                segment_size: config.segment.options.segment_size,
                segment,
                sender: Mutex::new(Some(sender)),
                worker: Mutex::new(Some(worker)),
                queue,
                spawner,
                io_control,
                metrics,
                background_error,
                stats,
                close_state: AtomicU8::new(OPEN),
                close_notify: Notify::new(),
                #[cfg(test)]
                panic_next,
            }),
        })
    }
}

impl Engine<Bytes, EngineValue, HybridCacheProperties> for ExtentEngine {
    fn storage_usage(&self) -> StorageUsage {
        let allocated = self
            .inner
            .segment
            .allocated_size()
            .ok()
            .and_then(|bytes| usize::try_from(bytes).ok())
            .unwrap_or(self.inner.capacity);
        StorageUsage::new(self.inner.capacity, allocated)
    }

    fn io_control(&self) -> &IoControl {
        &self.inner.io_control
    }

    fn filter(&self, _hash: u64, estimated_size: usize) -> StorageFilterResult {
        if self.inner.close_state.load(Ordering::Acquire) != OPEN || self.inner.background_error.is_failed() {
            self.inner.stats.record_dropped_command();
            return StorageFilterResult::Reject;
        }
        if estimated_size > self.inner.queue.capacity_bytes()
            || self.inner.queue.pending_entries() >= self.inner.queue.capacity_entries()
            || self.inner.queue.pending_bytes() >= self.inner.queue.capacity_bytes()
        {
            self.inner.stats.record_dropped_command();
            self.inner.metrics.storage_queue_buffer_overflow.increase(1);
            return StorageFilterResult::Reject;
        }
        let delay = self.inner.io_control.statistics().write_throttle();
        if delay == Duration::ZERO {
            StorageFilterResult::Admit
        } else {
            self.inner.stats.record_dropped_command();
            self.inner.metrics.storage_throttled.increase(1);
            StorageFilterResult::Throttled(delay)
        }
    }

    fn enqueue(&self, piece: ExtentPiece, estimated_size: usize) {
        let key = piece.key();
        let value = piece.value().value();
        if key.is_empty()
            || key.len() > MAX_BLOB_KEY_SIZE
            || value.is_empty()
            || BLOB_HEADER_SIZE
                .checked_add(key.len())
                .and_then(|size| size.checked_add(value.len()))
                .is_none_or(|size| size > self.inner.segment_size || u32::try_from(size).is_err())
        {
            self.inner.stats.record_dropped_command();
            return;
        }
        self.inner
            .try_send(estimated_size, |reservation| Command::put(piece, reservation));
    }

    fn load(
        &self,
        key: Bytes,
        _hash: u64,
    ) -> BoxFuture<'static, foyer::Result<Load<Bytes, EngineValue, HybridCacheProperties>>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            if inner.close_state.load(Ordering::Acquire) != OPEN {
                return Err(FoyerError::new(
                    FoyerErrorKind::Closed,
                    "load from a closed Extent engine",
                ));
            }
            let delay = inner.io_control.statistics().read_throttle();
            if delay != Duration::ZERO {
                return Ok(Load::Throttled);
            }
            let blob_key = match BlobKey::new(&key) {
                Ok(key) => key,
                Err(_) => return Ok(Load::Miss),
            };
            let segment = inner.segment.clone();
            let loaded = inner
                .spawner
                .spawn_blocking(move || segment.get_with_stats(&blob_key))
                .await?
                .map_err(|error| extent_error("load Extent entry", error))?;
            inner
                .stats
                .record_read(loaded.data_slots, loaded.data_runs, loaded.data_bytes);
            let Some(value) = loaded.value else {
                return Ok(Load::Miss);
            };
            let priority = loaded
                .priority
                .expect("a validated Extent hit must retain its priority");
            inner
                .stats
                .record_disk_reads(inner.io_control.statistics(), loaded.data_bytes, loaded.data_runs);
            Ok(Load::Entry {
                key,
                value: EngineValue::new(Bytes::from(value), priority).expect("a stored Extent hit must be non-empty"),
                populated: Populated { age: Age::Young },
            })
        })
    }

    fn delete(&self, key: Bytes, _hash: u64) {
        if key.is_empty() || key.len() > MAX_BLOB_KEY_SIZE {
            return;
        }
        let charge = key.len();
        self.inner
            .try_send(charge, |reservation| Command::delete(key, reservation));
    }

    fn may_contains(&self, _hash: u64) -> bool {
        // Foyer currently provides only its 64-bit routing hash here, while Extent indexes a
        // digest of the complete key. Returning true preserves the no-false-negative contract;
        // load still performs exact full-key verification.
        self.inner.close_state.load(Ordering::Acquire) == OPEN
    }

    fn destroy(&self) -> BoxFuture<'static, foyer::Result<()>> {
        Box::pin(async {
            Err(FoyerError::new(
                FoyerErrorKind::External,
                "online clear is not implemented by ExtentEngine",
            ))
        })
    }

    fn wait(&self) -> BoxFuture<'static, ()> {
        let inner = self.inner.clone();
        Box::pin(async move {
            inner.queue.wait().await;
            let segment = inner.segment.clone();
            let statistics = inner.io_control.statistics().clone();
            let stats = inner.stats.clone();
            match inner
                .spawner
                .spawn_blocking(move || sync_segment(&segment, statistics.as_ref(), &stats))
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    inner.background_error.record(format!("sync Extent engine: {error}"));
                }
                Err(error) => {
                    inner.background_error.record(format!("join Extent sync task: {error}"));
                }
            }
            inner.stats.record_checkpoint(inner.segment.checkpoint_stats());
        })
    }

    fn close(&self) -> BoxFuture<'static, foyer::Result<()>> {
        let inner = self.inner.clone();
        Box::pin(async move { inner.close().await })
    }
}

impl Inner {
    fn try_send(&self, charge: usize, build: impl FnOnce(QueueReservation) -> Command) {
        if self.close_state.load(Ordering::Acquire) != OPEN || self.background_error.is_failed() {
            self.stats.record_dropped_command();
            return;
        }
        let Some(reservation) = self.queue.try_reserve(charge) else {
            self.stats.record_dropped_command();
            self.metrics.storage_queue_buffer_overflow.increase(1);
            return;
        };
        let command = build(reservation);
        let sender = mutex_lock(&self.sender);
        match sender.as_ref() {
            Some(sender) => match sender.try_send(command) {
                Ok(()) => self.stats.record_accepted_command(),
                Err(TrySendError::Full(command) | TrySendError::Disconnected(command)) => {
                    self.stats.record_dropped_command();
                    self.metrics.storage_queue_channel_overflow.increase(1);
                    drop(command);
                }
            },
            None => {
                self.stats.record_dropped_command();
                drop(command);
            }
        }
    }

    async fn close(self: Arc<Self>) -> foyer::Result<()> {
        match self
            .close_state
            .compare_exchange(OPEN, CLOSING, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                mutex_lock(&self.sender).take();
                let worker = mutex_lock(&self.worker).take();
                if let Some(worker) = worker {
                    match self.spawner.spawn_blocking(move || worker.join()).await {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => self.background_error.record("Extent flush worker panicked".to_string()),
                        Err(error) => self
                            .background_error
                            .record(format!("join Extent flush worker: {error}")),
                    }
                }
                self.close_state.store(CLOSED, Ordering::Release);
                self.stats.record_checkpoint(self.segment.checkpoint_stats());
                self.close_notify.notify_waiters();
            }
            Err(CLOSING) => {
                while self.close_state.load(Ordering::Acquire) != CLOSED {
                    let notified = self.close_notify.notified();
                    if self.close_state.load(Ordering::Acquire) == CLOSED {
                        break;
                    }
                    notified.await;
                }
            }
            Err(CLOSED) => {}
            Err(_) => unreachable!("Extent close state must be valid"),
        }
        match self.background_error.message() {
            Some(error) => Err(FoyerError::new(FoyerErrorKind::External, error)),
            None => Ok(()),
        }
    }
}

fn config_error(message: &'static str) -> FoyerError {
    FoyerError::new(FoyerErrorKind::Config, message)
}

fn extent_error(context: &'static str, error: Error) -> FoyerError {
    FoyerError::new(FoyerErrorKind::External, context).with_source(error)
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use foyer::{HybridCache, HybridCachePolicy, RecoverMode};

    use super::*;
    use crate::segment::InjectedFault;

    type TestCache = HybridCache<Bytes, EngineValue>;

    async fn cache(path: &std::path::Path) -> (TestCache, ExtentEngineHandle) {
        let config = ExtentEngineConfig::new(path, 16 * 1024 * 1024)
            .with_slot_size(4 * 1024)
            .with_segment_size(32 * 1024)
            .with_index_write_buffer_size(64 * 1024)
            .with_index_cache_size(1024 * 1024)
            .with_queue_capacity_bytes(1024 * 1024)
            .with_queue_capacity_entries(128)
            .with_write_batch_bytes(128 * 1024)
            .with_write_batch_entries(32);
        let handle = config.handle();
        let cache = HybridCache::builder()
            .with_name("extent-fault-injection")
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .with_flush_on_close(false)
            .memory(1024 * 1024)
            .storage()
            .with_engine_config(Box::new(config) as Box<dyn EngineConfig<Bytes, EngineValue, HybridCacheProperties>>)
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();
        (cache, handle)
    }

    fn insert(cache: &TestCache, suffix: u8) {
        cache.insert(
            Bytes::from(vec![b'k', suffix]),
            EngineValue::new(Bytes::from(vec![suffix; 4096]), crate::CachePriority::Normal).unwrap(),
        );
    }

    async fn assert_insert_fault(fault: InjectedFault, expected: &str) {
        let directory = tempfile::tempdir().unwrap();
        let (cache, handle) = cache(directory.path()).await;
        handle.inject_segment_fault(fault);
        insert(&cache, 1);
        cache.storage().wait().await;
        let error = cache.close().await.unwrap_err().to_string();
        assert!(error.contains(expected), "unexpected close error: {error}");
        assert_eq!(handle.queue_pending_entries(), Some(0));
        assert_eq!(handle.queue_pending_bytes(), Some(0));
        let writes = handle.write_stats().unwrap();
        assert_eq!(writes.accepted_commands, 1);
        assert_eq!(writes.completed_commands, 0);
        assert_eq!(writes.failed_batches, 1);
        assert!(handle.background_error().is_some());
    }

    #[tokio::test]
    async fn io_write_failures_trip_the_async_circuit_breaker() {
        assert_insert_fault(InjectedFault::WriteNoSpace, "no-space write").await;
        assert_insert_fault(InjectedFault::WriteZero, "short write").await;
    }

    #[tokio::test]
    async fn sync_failure_is_reported_by_wait_and_close() {
        let directory = tempfile::tempdir().unwrap();
        let (cache, handle) = cache(directory.path()).await;
        handle.inject_segment_fault(InjectedFault::Sync);
        insert(&cache, 1);
        cache.storage().wait().await;
        let error = cache.close().await.unwrap_err().to_string();
        assert!(error.contains("fdatasync failure"), "unexpected close error: {error}");
        let writes = handle.write_stats().unwrap();
        assert_eq!(writes.accepted_commands, 1);
        assert_eq!(writes.completed_commands, 1);
        assert_eq!(handle.queue_pending_entries(), Some(0));
    }

    #[tokio::test]
    async fn worker_panic_is_contained_and_releases_queue_reservations() {
        let directory = tempfile::tempdir().unwrap();
        let (cache, handle) = cache(directory.path()).await;
        handle.panic_next_write();
        insert(&cache, 1);
        cache.storage().wait().await;
        let error = cache.close().await.unwrap_err().to_string();
        assert!(error.contains("worker panicked"), "unexpected close error: {error}");
        assert_eq!(handle.queue_pending_entries(), Some(0));
        assert_eq!(handle.queue_pending_bytes(), Some(0));
        assert_eq!(handle.write_stats().unwrap().accepted_commands, 1);
    }
}
