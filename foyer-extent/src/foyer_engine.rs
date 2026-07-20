use std::{
    fmt,
    path::PathBuf,
    sync::{
        Arc, Mutex, MutexGuard, Weak,
        atomic::{AtomicBool, AtomicU8, Ordering},
        mpsc::{SyncSender, TrySendError},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use bytes::Bytes;
use foyer::{
    Age, Engine, EngineBuildContext, EngineConfig, Error as FoyerError, ErrorKind as FoyerErrorKind,
    HybridCacheProperties, IoControl, Load, Metrics, PieceRef, Populated, Spawner, StorageFilterResult, StorageUsage,
    Throttle,
};
use futures_core::future::BoxFuture;
use tokio::sync::Notify;

use crate::{
    CheckpointStats, EngineValue, EntryIndexReadStats, EntryIndexStats, Error, ExtentOccupancy, IoSchedulerStats,
    MAX_KEY_SIZE, PhysicalWriteStats, ReclaimStats,
    format::STORED_ENTRY_HEADER_SIZE,
    model::EntryKey,
    store::{ExtentStore, ExtentStoreConfig, PreparedGet},
};

mod queue;
mod read;
mod recovery;
mod stats;
mod writer;

pub use self::stats::{EngineReadStats, EngineWriteStats};
use self::{
    queue::{QueueReservation, SubmissionQueue},
    read::ReadLimiter,
    recovery::{RecoveryOutcome, open_store},
    stats::EngineStats,
    writer::{BackgroundError, Command, WriteWorker, sync_store},
};

const DEFAULT_QUEUE_CAPACITY_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_QUEUE_CAPACITY_ENTRIES: usize = 65_536;
const DEFAULT_WRITE_BATCH_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_WRITE_BATCH_ENTRIES: usize = 4_096;
const DEFAULT_READ_BUSY_WRITE_BATCH_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(1);
const OPEN: u8 = 0;
const CLOSING: u8 = 1;
const CLOSED: u8 = 2;
type ExtentPiece = PieceRef<Bytes, EngineValue, HybridCacheProperties>;

/// Configuration for installing Extent as a Foyer disk engine.
///
/// Capacity is the only production static input. The format owns its entry charge and extent size and
/// validates the effective layout when reopening. Queue, batching, I/O, checkpoint, frequency, and
/// index-cache settings are runtime tuning knobs and may change across reopens.
#[derive(Debug)]
pub struct ExtentEngineConfig {
    path: PathBuf,
    store: ExtentStoreConfig,
    queue_capacity_bytes: usize,
    queue_capacity_entries: usize,
    write_batch_bytes: usize,
    write_batch_entries: usize,
    read_busy_write_batch_bytes: usize,
    read_concurrency: usize,
    checkpoint_interval: Duration,
    throttle: Throttle,
    handle: ExtentEngineHandle,
}

impl ExtentEngineConfig {
    /// Create an ExtentEngine configuration with balanced static defaults.
    pub fn new(path: impl Into<PathBuf>, capacity_bytes: u64) -> Self {
        Self {
            path: path.into(),
            store: ExtentStoreConfig::new(capacity_bytes),
            queue_capacity_bytes: DEFAULT_QUEUE_CAPACITY_BYTES,
            queue_capacity_entries: DEFAULT_QUEUE_CAPACITY_ENTRIES,
            write_batch_bytes: DEFAULT_WRITE_BATCH_BYTES,
            write_batch_entries: DEFAULT_WRITE_BATCH_ENTRIES,
            read_busy_write_batch_bytes: DEFAULT_READ_BUSY_WRITE_BATCH_BYTES,
            read_concurrency: std::thread::available_parallelism()
                .map_or(2, |parallelism| parallelism.get().saturating_mul(2)),
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            throttle: Throttle::default(),
            handle: ExtentEngineHandle::default(),
        }
    }

    /// Override the format-owned layout for tests and benchmarks.
    ///
    /// Production integrations must use the balanced layout selected by [`Self::new`].
    #[doc(hidden)]
    pub fn with_test_layout(mut self, entry_charge: usize, extent_size: usize) -> Self {
        self.store.entry_charge = entry_charge;
        self.store.options.extent_size = extent_size;
        self
    }

    /// Set the number of concurrent data-file write runs.
    pub fn with_write_concurrency(mut self, concurrency: usize) -> Self {
        self.store.options.write_concurrency = concurrency;
        self
    }

    /// Set how long physical writes yield to continuously active payload reads.
    pub fn with_io_read_priority_duration(mut self, duration: Duration) -> Self {
        self.store.options.io_read_priority_duration = duration;
        self
    }

    /// Bound one coalesced data read.
    pub fn with_read_run_size(mut self, bytes: usize) -> Self {
        self.store.options.read_run_size = bytes;
        self
    }

    /// Bound one coalesced data write.
    pub fn with_write_run_size(mut self, bytes: usize) -> Self {
        self.store.options.write_run_size = bytes;
        self
    }

    /// Set the FixedRecordLSM mutable write-buffer budget.
    pub fn with_index_write_buffer_size(mut self, bytes: usize) -> Self {
        self.store.options.index_write_buffer_size = bytes;
        self
    }

    /// Set the FixedRecordLSM index page-cache budget.
    pub fn with_index_cache_size(mut self, bytes: usize) -> Self {
        self.store.options.index_cache_size = bytes;
        self
    }

    /// Set the published Stored Entry byte budget between background checkpoint requests.
    pub fn with_checkpoint_bytes(mut self, bytes: usize) -> Self {
        self.store.options.checkpoint_bytes = bytes;
        self
    }

    /// Set the maximum interval between metadata checkpoint requests.
    pub fn with_checkpoint_interval(mut self, interval: Duration) -> Self {
        self.checkpoint_interval = interval;
        self
    }

    /// Set the reuse frequency required to promote normal/high-priority entries.
    pub fn with_hot_frequency(mut self, frequency: u8) -> Self {
        self.store.options.hot_frequency = frequency;
        self
    }

    /// Set the reuse frequency required to promote low-priority entries.
    pub fn with_low_hot_frequency(mut self, frequency: u8) -> Self {
        self.store.options.low_hot_frequency = frequency;
        self
    }

    /// Set the minimum usable-extent percentages protected for high and normal priority data.
    ///
    /// The percentages may sum to at most 100 and are rounded up to whole extents. Unoccupied
    /// protection remains shared capacity; low-priority data has no protected floor.
    pub fn with_priority_capacity_floors(mut self, high_percent: u8, normal_percent: u8) -> Self {
        self.store.options.priority_capacity_floors =
            crate::store::PriorityCapacityFloors::new(high_percent, normal_percent);
        self
    }

    /// Select direct data-file I/O on supported Linux filesystems.
    pub fn with_direct_io(mut self, direct_io: bool) -> Self {
        self.store.options.direct_io = direct_io;
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

    /// Bound a write batch while physical reads are active.
    pub fn with_read_busy_write_batch_bytes(mut self, bytes: usize) -> Self {
        self.read_busy_write_batch_bytes = bytes;
        self
    }

    /// Set the hard number of concurrent physical cache reads.
    ///
    /// Reads above the limit return `Load::Throttled` immediately rather than queueing.
    pub fn with_read_concurrency(mut self, concurrency: usize) -> Self {
        self.read_concurrency = concurrency;
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
        if self.read_busy_write_batch_bytes == 0 {
            return Err(config_error("extent read-busy write batch bytes must be positive"));
        }
        if self.read_concurrency == 0 {
            return Err(config_error("extent read concurrency must be positive"));
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
        self.upgrade().map(|inner| inner.store.file_size())
    }

    pub fn allocated_size(&self) -> Option<u64> {
        self.upgrade()?.store.allocated_size().ok()
    }

    pub fn physical_write_stats(&self) -> Option<PhysicalWriteStats> {
        self.upgrade().map(|inner| inner.store.physical_write_stats())
    }

    pub fn io_scheduler_stats(&self) -> Option<IoSchedulerStats> {
        self.upgrade().map(|inner| inner.store.io_scheduler_stats())
    }

    pub fn entry_index_stats(&self) -> Option<EntryIndexStats> {
        self.upgrade().map(|inner| inner.store.entry_index_stats())
    }

    pub fn entry_index_read_stats(&self) -> Option<EntryIndexReadStats> {
        self.upgrade().map(|inner| inner.store.entry_index_read_stats())
    }

    pub fn reclaim_stats(&self) -> Option<ReclaimStats> {
        self.upgrade().map(|inner| inner.stats.reclaim())
    }

    pub fn extent_occupancy(&self) -> Option<ExtentOccupancy> {
        self.upgrade().map(|inner| inner.store.extent_occupancy())
    }

    pub fn read_stats(&self) -> Option<EngineReadStats> {
        self.upgrade().map(|inner| inner.stats.reads())
    }

    pub fn write_stats(&self) -> Option<EngineWriteStats> {
        self.upgrade().map(|inner| inner.stats.writes())
    }

    pub fn checkpoint_stats(&self) -> Option<CheckpointStats> {
        self.upgrade().map(|inner| inner.store.checkpoint_stats())
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

    pub fn active_reads(&self) -> Option<usize> {
        self.upgrade().map(|inner| inner.read_limiter.active())
    }

    pub fn read_concurrency(&self) -> Option<usize> {
        self.upgrade().map(|inner| inner.read_limiter.limit())
    }

    #[cfg(test)]
    fn inject_store_fault(&self, fault: crate::store::InjectedFault) {
        self.upgrade()
            .expect("ExtentEngine handle must be attached")
            .store
            .inject_fault(fault);
    }

    #[cfg(test)]
    fn panic_next_write(&self) {
        self.upgrade()
            .expect("ExtentEngine handle must be attached")
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
            let capacity = usize::try_from(self.store.capacity_bytes)
                .map_err(|_| config_error("extent capacity does not fit usize"))?;
            let io_control = IoControl::new(self.throttle.clone());
            let path = self.path.clone();
            let recovery_path = path.clone();
            let store_config = self.store;
            let recover_mode = ctx.recover_mode;
            let recovery_started = Instant::now();
            let open = ctx
                .spawner
                .spawn_blocking(move || open_store(&recovery_path, store_config, recover_mode))
                .await?;
            let open = open.map_err(|error| extent_error("open ExtentEngine", error))?;
            ctx.metrics
                .storage_engine_recovery_duration
                .record(recovery_started.elapsed().as_secs_f64());
            match &open.outcome {
                RecoveryOutcome::Created => ctx.metrics.storage_engine_recovery_created.increase(1),
                RecoveryOutcome::Recovered => ctx.metrics.storage_engine_recovery_recovered.increase(1),
                RecoveryOutcome::Recreated(reason) => {
                    ctx.metrics.storage_engine_recovery_recreated.increase(1);
                    tracing::warn!(path = %path.display(), %reason, "recreated Extent cache after recovery failure");
                }
            }
            let store = Arc::new(open.store);
            let handle = self.handle.clone();
            let engine = ExtentEngine::start(store, *self, capacity, io_control, ctx.spawner, ctx.metrics)?;
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
    store: Arc<ExtentStore>,
    capacity: usize,
    extent_size: usize,
    sender: Mutex<Option<SyncSender<Command>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    queue: Arc<SubmissionQueue>,
    spawner: Spawner,
    io_control: IoControl,
    metrics: Arc<Metrics>,
    background_error: Arc<BackgroundError>,
    stats: Arc<EngineStats>,
    read_limiter: Arc<ReadLimiter>,
    shutdown: Arc<AtomicBool>,
    close_state: AtomicU8,
    close_notify: Notify,
    #[cfg(test)]
    panic_next: Arc<AtomicBool>,
}

impl ExtentEngine {
    fn start(
        store: Arc<ExtentStore>,
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
        stats.record_remaining_index_reads(io_control.statistics(), store.entry_index_io_read_stats());
        stats.record_extent_occupancy(store.extent_occupancy());
        let read_limiter = ReadLimiter::new(config.read_concurrency, metrics.clone());
        let shutdown = Arc::new(AtomicBool::new(false));
        stats.record_checkpoint(store.checkpoint_stats());
        #[cfg(test)]
        let panic_next = Arc::new(AtomicBool::new(false));
        let worker = WriteWorker::new(
            store.clone(),
            io_control.statistics().clone(),
            config.write_batch_entries,
            config.write_batch_bytes,
            config.read_busy_write_batch_bytes,
            config.checkpoint_interval,
            background_error.clone(),
            stats.clone(),
            read_limiter.clone(),
            shutdown.clone(),
            #[cfg(test)]
            panic_next.clone(),
        )
        .spawn(receiver)
        .map_err(|error| FoyerError::new(FoyerErrorKind::External, "start Extent flush worker").with_source(error))?;

        Ok(Self {
            inner: Arc::new(Inner {
                path: config.path,
                capacity,
                extent_size: config.store.options.extent_size,
                store,
                sender: Mutex::new(Some(sender)),
                worker: Mutex::new(Some(worker)),
                queue,
                spawner,
                io_control,
                metrics,
                background_error,
                stats,
                read_limiter,
                shutdown,
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
            .store
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
        // The exact priority is available only at enqueue. Keep this prefilter to hard bounds and
        // apply priority/read-pressure shedding at the engine boundary.
        StorageFilterResult::Admit
    }

    fn enqueue(&self, piece: ExtentPiece, estimated_size: usize) {
        let key = piece.key();
        let value = piece.value().value();
        if key.is_empty()
            || key.len() > MAX_KEY_SIZE
            || value.is_empty()
            || STORED_ENTRY_HEADER_SIZE
                .checked_add(key.len())
                .and_then(|size| size.checked_add(value.len()))
                .is_none_or(|size| size > self.inner.extent_size || u32::try_from(size).is_err())
        {
            self.inner.stats.record_dropped_command();
            return;
        }
        let priority = piece.value().priority();
        self.inner
            .try_send(estimated_size, priority, |reservation| Command::put(piece, reservation));
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
                    "load from a closed ExtentEngine",
                ));
            }
            let delay = inner.io_control.statistics().read_throttle();
            if delay != Duration::ZERO {
                return Ok(Load::Throttled);
            }
            let entry_key = match EntryKey::new(&key) {
                Ok(key) => key,
                Err(_) => return Ok(Load::Miss),
            };
            let prepared = inner
                .store
                .prepare_get(&entry_key)
                .map_err(|error| extent_error("prepare Extent entry lookup", error))?;
            if prepared == PreparedGet::Miss {
                return Ok(Load::Miss);
            }
            let Some(read_permit) = inner.read_limiter.try_acquire() else {
                return Ok(Load::Throttled);
            };
            let store = inner.store.clone();
            let loaded = inner
                .spawner
                .spawn_blocking(move || {
                    let _read_permit = read_permit;
                    store.get_prepared(&entry_key, prepared)
                })
                .await;
            inner
                .stats
                .record_remaining_index_reads(inner.io_control.statistics(), inner.store.entry_index_io_read_stats());
            let loaded = loaded?.map_err(|error| extent_error("load Extent entry", error))?;
            inner
                .stats
                .record_read(loaded.data_frames, loaded.data_runs, loaded.data_bytes);
            inner
                .stats
                .record_disk_reads(inner.io_control.statistics(), loaded.data_bytes, loaded.data_runs);
            let Some(value) = loaded.value else {
                return Ok(Load::Miss);
            };
            let priority = loaded
                .priority
                .expect("a validated Extent hit must retain its priority");
            Ok(Load::Entry {
                key,
                value: EngineValue::new(Bytes::from(value), priority).expect("a stored Extent hit must be non-empty"),
                populated: Populated { age: Age::Young },
            })
        })
    }

    fn delete(&self, key: Bytes, _hash: u64) {
        if key.is_empty() || key.len() > MAX_KEY_SIZE {
            return;
        }
        let charge = key.len();
        self.inner.try_send(charge, crate::CachePriority::High, |reservation| {
            Command::delete(key, reservation)
        });
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
            let store = inner.store.clone();
            let statistics = inner.io_control.statistics().clone();
            let stats = inner.stats.clone();
            match inner
                .spawner
                .spawn_blocking(move || sync_store(&store, statistics.as_ref(), &stats))
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    inner.background_error.record(format!("sync ExtentEngine: {error}"));
                }
                Err(error) => {
                    inner.background_error.record(format!("join Extent sync task: {error}"));
                }
            }
            inner.stats.record_checkpoint(inner.store.checkpoint_stats());
        })
    }

    fn close(&self) -> BoxFuture<'static, foyer::Result<()>> {
        let inner = self.inner.clone();
        Box::pin(async move { inner.close().await })
    }
}

impl Inner {
    fn try_send(&self, charge: usize, priority: crate::CachePriority, build: impl FnOnce(QueueReservation) -> Command) {
        if self.close_state.load(Ordering::Acquire) != OPEN || self.background_error.is_failed() {
            self.stats.record_dropped_command();
            return;
        }
        if self.should_shed(priority) {
            self.stats.record_shed_command(priority);
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

    fn should_shed(&self, priority: crate::CachePriority) -> bool {
        let readers_active = self.read_limiter.active() > 0;
        let (numerator, denominator) = match (priority, readers_active) {
            (crate::CachePriority::Low, true) => (1, 4),
            (crate::CachePriority::Low, false) => (1, 2),
            (crate::CachePriority::Normal, true) => (1, 2),
            (crate::CachePriority::Normal, false) => (3, 4),
            (crate::CachePriority::High, _) => return false,
        };
        at_fraction(
            self.queue.pending_entries(),
            self.queue.capacity_entries(),
            numerator,
            denominator,
        ) || at_fraction(
            self.queue.pending_bytes(),
            self.queue.capacity_bytes(),
            numerator,
            denominator,
        )
    }

    async fn close(self: Arc<Self>) -> foyer::Result<()> {
        match self
            .close_state
            .compare_exchange(OPEN, CLOSING, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                let started = Instant::now();
                self.shutdown.store(true, Ordering::Release);
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
                self.metrics
                    .storage_engine_shutdown_duration
                    .record(started.elapsed().as_secs_f64());
                self.stats.record_checkpoint(self.store.checkpoint_stats());
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

fn at_fraction(value: usize, capacity: usize, numerator: usize, denominator: usize) -> bool {
    let threshold = capacity
        .checked_mul(numerator)
        .map_or_else(|| capacity / denominator * numerator, |scaled| scaled / denominator)
        .max(1);
    value >= threshold
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
    use crate::store::InjectedFault;

    type TestCache = HybridCache<Bytes, EngineValue>;

    async fn cache(path: &std::path::Path) -> (TestCache, ExtentEngineHandle) {
        let config = ExtentEngineConfig::new(path, 16 * 1024 * 1024)
            .with_test_layout(4 * 1024, 32 * 1024)
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
        handle.inject_store_fault(fault);
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
        handle.inject_store_fault(InjectedFault::Sync);
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

    #[tokio::test]
    async fn close_discards_only_unstarted_commands_and_leaves_a_recoverable_prefix() {
        let directory = tempfile::tempdir().unwrap();
        let config = ExtentEngineConfig::new(directory.path(), 16 * 1024 * 1024)
            .with_test_layout(4 * 1024, 32 * 1024)
            .with_index_write_buffer_size(64 * 1024)
            .with_index_cache_size(1024 * 1024)
            .with_queue_capacity_bytes(1024 * 1024)
            .with_queue_capacity_entries(128)
            .with_write_batch_bytes(8 * 1024)
            .with_write_batch_entries(1)
            .with_throttle(Throttle::new().with_write_throughput(1));
        let store_config = config.store;
        let handle = config.handle();
        let cache = HybridCache::builder()
            .with_name("extent-bounded-close")
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .with_flush_on_close(false)
            .memory(1024 * 1024)
            .storage()
            .with_engine_config(Box::new(config) as Box<dyn EngineConfig<Bytes, EngineValue, HybridCacheProperties>>)
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        for suffix in 0..128u8 {
            cache.insert(
                Bytes::from(vec![b'k', suffix]),
                EngineValue::new(Bytes::from(vec![suffix; 4096]), crate::CachePriority::High).unwrap(),
            );
        }
        cache.close().await.unwrap();

        let writes = handle.write_stats().unwrap();
        assert!(writes.shutdown_dropped_commands > 0);
        assert_eq!(
            writes.accepted_commands,
            writes.completed_commands + writes.shutdown_dropped_commands
        );
        assert_eq!(handle.queue_pending_entries(), Some(0));
        drop(cache);

        let reopened = open_store(directory.path(), store_config, RecoverMode::Strict).unwrap();
        let mut hits = 0;
        for suffix in 0..128u8 {
            let key = EntryKey::new([b'k', suffix]).unwrap();
            if let Some(value) = reopened.store.get(&key).unwrap() {
                assert_eq!(value, vec![suffix; 4096]);
                hits += 1;
            }
        }
        assert_eq!(hits, writes.completed_commands);
    }
}
