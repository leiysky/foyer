use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use foyer::{Metrics, Statistics};

use crate::{
    CachePriority, CheckpointStats, EntryIndexReadStats, ExtentOccupancy, ReclaimStats, foyer_engine::mutex_lock,
};

const LATENCY_SAMPLE_CAPACITY: usize = 16_384;

/// Cumulative physical read work performed by ExtentEngine.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EngineReadStats {
    pub calls: u64,
    /// Number of covering 4 KiB payload frames, not the number of I/O requests.
    pub data_frames: u64,
    /// Number of userspace payload read calls after contiguous bytes are coalesced.
    pub data_runs: u64,
    pub data_bytes: u64,
}

/// Cumulative outcomes at ExtentEngine's asynchronous write boundary.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EngineWriteStats {
    /// Commands accepted by Extent's ordered queue (puts are bounded; deletes may overcommit).
    pub accepted_commands: u64,
    /// Commands shed by admission, validation, close, or an enqueue race.
    pub dropped_commands: u64,
    /// Accepted commands discarded during a bounded graceful shutdown.
    pub shutdown_dropped_commands: u64,
    /// Low-priority commands shed before reaching the hard queue bound.
    pub shed_low_commands: u64,
    /// Normal-priority commands shed before reaching the hard queue bound.
    pub shed_normal_commands: u64,
    /// High-priority commands shed at the hard queue bound.
    pub shed_high_commands: u64,
    /// Accepted commands fully processed by ExtentStore.
    pub completed_commands: u64,
    /// Put commands processed but rejected by ExtentStore under allocation pressure.
    pub storage_rejected_puts: u64,
    /// Batches fully processed by ExtentStore.
    pub completed_batches: u64,
    /// Batches that encountered a storage failure before publication completed.
    pub failed_batches: u64,
}

/// Engine-level observations that bridge ExtentStore work to Foyer statistics.
pub struct EngineStats {
    metrics: Arc<Metrics>,
    reclaim: Mutex<ReclaimStats>,
    reads: EngineReadCounters,
    writes: EngineWriteCounters,
    recorded_writes: Mutex<RecordedIo>,
    recorded_index_reads: Mutex<RecordedIo>,
    write_batches: Mutex<LatencySamples>,
    write_publications: Mutex<LatencySamples>,
}

impl EngineStats {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            metrics,
            reclaim: Mutex::default(),
            reads: EngineReadCounters::default(),
            writes: EngineWriteCounters::default(),
            recorded_writes: Mutex::default(),
            recorded_index_reads: Mutex::default(),
            write_batches: Mutex::default(),
            write_publications: Mutex::default(),
        }
    }

    pub fn reclaim(&self) -> ReclaimStats {
        *mutex_lock(&self.reclaim)
    }

    pub fn merge_reclaim(&self, reclaim: ReclaimStats) {
        mutex_lock(&self.reclaim).merge(reclaim);
    }

    pub fn record_read(&self, data_frames: usize, data_runs: usize, data_bytes: usize) {
        self.reads.record(data_frames, data_runs, data_bytes);
    }

    pub fn reads(&self) -> EngineReadStats {
        self.reads.snapshot()
    }

    pub fn writes(&self) -> EngineWriteStats {
        self.writes.snapshot()
    }

    pub fn record_accepted_command(&self) {
        self.writes.accepted_commands.fetch_add(1, Ordering::Relaxed);
        self.metrics.storage_engine_command_accepted.increase(1);
    }

    pub fn record_dropped_command(&self) {
        self.writes.dropped_commands.fetch_add(1, Ordering::Relaxed);
        self.metrics.storage_engine_command_dropped.increase(1);
    }

    pub fn record_shed_command(&self, priority: CachePriority) {
        self.record_dropped_command();
        match priority {
            CachePriority::Low => {
                self.writes.shed_low_commands.fetch_add(1, Ordering::Relaxed);
                self.metrics.storage_engine_command_shed_low.increase(1);
            }
            CachePriority::Normal => {
                self.writes.shed_normal_commands.fetch_add(1, Ordering::Relaxed);
                self.metrics.storage_engine_command_shed_normal.increase(1);
            }
            CachePriority::High => {
                self.writes.shed_high_commands.fetch_add(1, Ordering::Relaxed);
                self.metrics.storage_engine_command_shed_high.increase(1);
            }
        }
    }

    pub fn record_shutdown_dropped(&self, commands: usize) {
        self.writes
            .dropped_commands
            .fetch_add(commands as u64, Ordering::Relaxed);
        self.writes
            .shutdown_dropped_commands
            .fetch_add(commands as u64, Ordering::Relaxed);
        self.metrics.storage_engine_command_dropped.increase(commands as u64);
        self.metrics
            .storage_engine_command_shutdown_dropped
            .increase(commands as u64);
    }

    pub fn record_abandoned(&self, commands: usize) {
        self.writes
            .dropped_commands
            .fetch_add(commands as u64, Ordering::Relaxed);
        self.metrics.storage_engine_command_dropped.increase(commands as u64);
    }

    pub fn record_completed_batch(&self, commands: usize, storage_rejected_puts: usize) {
        self.writes
            .completed_commands
            .fetch_add(commands as u64, Ordering::Relaxed);
        self.writes
            .storage_rejected_puts
            .fetch_add(storage_rejected_puts as u64, Ordering::Relaxed);
        self.writes.completed_batches.fetch_add(1, Ordering::Relaxed);
        self.metrics.storage_engine_command_completed.increase(commands as u64);
        self.metrics
            .storage_engine_command_rejected
            .increase(storage_rejected_puts as u64);
        self.metrics.storage_engine_batch_completed.increase(1);
    }

    pub fn record_failed_batch(&self) {
        self.writes.failed_batches.fetch_add(1, Ordering::Relaxed);
        self.metrics.storage_engine_batch_failed.increase(1);
    }

    pub fn record_checkpoint(&self, checkpoint: CheckpointStats) {
        self.metrics
            .storage_engine_checkpoint_published
            .absolute(checkpoint.published_epoch);
        self.metrics
            .storage_engine_checkpoint_requested
            .absolute(checkpoint.requested_epoch);
        self.metrics
            .storage_engine_checkpoint_durable
            .absolute(checkpoint.durable_epoch);
        self.metrics
            .storage_engine_checkpoint_in_flight
            .absolute(checkpoint.in_flight_epoch.unwrap_or(0));
        self.metrics
            .storage_engine_checkpoint_dirty
            .absolute(checkpoint.dirty_bytes as u64);
    }

    pub fn record_extent_occupancy(&self, occupancy: ExtentOccupancy) {
        for priority in [CachePriority::Low, CachePriority::Normal, CachePriority::High] {
            let index = priority as usize;
            self.metrics.storage_engine_priority_extents[index]
                .absolute(u64::from(occupancy.occupied_extents(priority)));
            self.metrics.storage_engine_priority_floor_extents[index]
                .absolute(u64::from(occupancy.capacity_floor_extents(priority)));
            self.metrics.storage_engine_priority_allocated_bytes[index].absolute(occupancy.used_bytes(priority));
        }
    }

    pub fn record_write_batch(&self, latency: Duration) {
        mutex_lock(&self.write_batches).record(latency);
        self.metrics.storage_engine_batch_duration.record(latency.as_secs_f64());
    }

    pub fn record_write_publications(&self, latencies: impl IntoIterator<Item = Duration>) {
        let mut samples = mutex_lock(&self.write_publications);
        for latency in latencies {
            samples.record(latency);
            self.metrics
                .storage_engine_publication_duration
                .record(latency.as_secs_f64());
        }
    }

    pub fn write_latency_samples(&self) -> (Vec<Duration>, Vec<Duration>) {
        (
            mutex_lock(&self.write_batches).snapshot(),
            mutex_lock(&self.write_publications).snapshot(),
        )
    }

    pub fn record_disk_reads(&self, statistics: &Statistics, bytes: usize, runs: usize) {
        record_io(bytes, runs, |bytes| {
            statistics.record_disk_read(bytes);
            self.metrics.storage_disk_read.increase(1);
            self.metrics.storage_disk_read_bytes.increase(bytes as u64);
        });
    }

    pub fn record_remaining_index_reads(&self, statistics: &Statistics, total: EntryIndexReadStats) {
        let mut recorded = mutex_lock(&self.recorded_index_reads);
        let runs = usize::try_from(total.read_operations.saturating_sub(recorded.runs)).unwrap_or(usize::MAX);
        let bytes = usize::try_from(total.read_bytes.saturating_sub(recorded.bytes)).unwrap_or(usize::MAX);
        recorded.runs = total.read_operations;
        recorded.bytes = total.read_bytes;
        drop(recorded);
        self.record_disk_reads(statistics, bytes, runs);
    }

    pub fn record_synchronous_writes(&self, statistics: &Statistics, runs: usize, bytes: usize) {
        let mut recorded = mutex_lock(&self.recorded_writes);
        recorded.runs = recorded.runs.saturating_add(runs as u64);
        recorded.bytes = recorded.bytes.saturating_add(bytes as u64);
        drop(recorded);
        record_io(bytes, runs, |bytes| {
            statistics.record_disk_write(bytes);
            self.metrics.storage_disk_write.increase(1);
            self.metrics.storage_disk_write_bytes.increase(bytes as u64);
        });
    }

    pub fn record_remaining_writes(&self, statistics: &Statistics, total_runs: u64, total_bytes: u64) {
        let mut recorded = mutex_lock(&self.recorded_writes);
        let runs = usize::try_from(total_runs.saturating_sub(recorded.runs)).unwrap_or(usize::MAX);
        let bytes = usize::try_from(total_bytes.saturating_sub(recorded.bytes)).unwrap_or(usize::MAX);
        recorded.runs = total_runs;
        recorded.bytes = total_bytes;
        drop(recorded);
        record_io(bytes, runs, |bytes| {
            statistics.record_disk_write(bytes);
            self.metrics.storage_disk_write.increase(1);
            self.metrics.storage_disk_write_bytes.increase(bytes as u64);
        });
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct RecordedIo {
    runs: u64,
    bytes: u64,
}

#[derive(Default)]
struct EngineReadCounters {
    calls: AtomicU64,
    data_frames: AtomicU64,
    data_runs: AtomicU64,
    data_bytes: AtomicU64,
}

#[derive(Default)]
struct EngineWriteCounters {
    accepted_commands: AtomicU64,
    dropped_commands: AtomicU64,
    shutdown_dropped_commands: AtomicU64,
    shed_low_commands: AtomicU64,
    shed_normal_commands: AtomicU64,
    shed_high_commands: AtomicU64,
    completed_commands: AtomicU64,
    storage_rejected_puts: AtomicU64,
    completed_batches: AtomicU64,
    failed_batches: AtomicU64,
}

impl EngineWriteCounters {
    fn snapshot(&self) -> EngineWriteStats {
        EngineWriteStats {
            accepted_commands: self.accepted_commands.load(Ordering::Relaxed),
            dropped_commands: self.dropped_commands.load(Ordering::Relaxed),
            shutdown_dropped_commands: self.shutdown_dropped_commands.load(Ordering::Relaxed),
            shed_low_commands: self.shed_low_commands.load(Ordering::Relaxed),
            shed_normal_commands: self.shed_normal_commands.load(Ordering::Relaxed),
            shed_high_commands: self.shed_high_commands.load(Ordering::Relaxed),
            completed_commands: self.completed_commands.load(Ordering::Relaxed),
            storage_rejected_puts: self.storage_rejected_puts.load(Ordering::Relaxed),
            completed_batches: self.completed_batches.load(Ordering::Relaxed),
            failed_batches: self.failed_batches.load(Ordering::Relaxed),
        }
    }
}

impl EngineReadCounters {
    fn record(&self, data_frames: usize, data_runs: usize, data_bytes: usize) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.data_frames.fetch_add(data_frames as u64, Ordering::Relaxed);
        self.data_runs.fetch_add(data_runs as u64, Ordering::Relaxed);
        self.data_bytes.fetch_add(data_bytes as u64, Ordering::Relaxed);
    }

    fn snapshot(&self) -> EngineReadStats {
        EngineReadStats {
            calls: self.calls.load(Ordering::Relaxed),
            data_frames: self.data_frames.load(Ordering::Relaxed),
            data_runs: self.data_runs.load(Ordering::Relaxed),
            data_bytes: self.data_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
struct LatencySamples {
    values: Vec<Duration>,
    next: usize,
}

impl LatencySamples {
    fn record(&mut self, value: Duration) {
        if self.values.len() < LATENCY_SAMPLE_CAPACITY {
            self.values.push(value);
        } else {
            self.values[self.next] = value;
            self.next = (self.next + 1) % LATENCY_SAMPLE_CAPACITY;
        }
    }

    fn snapshot(&self) -> Vec<Duration> {
        self.values.clone()
    }
}

fn record_io(bytes: usize, runs: usize, mut record: impl FnMut(usize)) {
    if runs == 0 {
        debug_assert_eq!(bytes, 0);
        return;
    }
    let bytes_per_run = bytes / runs;
    let larger_runs = bytes % runs;
    for run in 0..runs {
        record(bytes_per_run + usize::from(run < larger_runs));
    }
}

#[cfg(test)]
mod tests {
    use foyer::Throttle;

    use super::*;

    #[test]
    fn io_statistics_preserve_bytes_and_runs() {
        let mut recorded = Vec::new();
        record_io(10, 3, |bytes| recorded.push(bytes));
        assert_eq!(recorded, vec![4, 3, 3]);

        record_io(0, 0, |bytes| recorded.push(bytes));
        assert_eq!(recorded, vec![4, 3, 3]);
    }

    #[test]
    fn engine_stats_start_empty() {
        let stats = EngineStats::new(Arc::new(Metrics::noop()));
        assert_eq!(stats.writes(), EngineWriteStats::default());
        assert_eq!(stats.reads(), EngineReadStats::default());
    }

    #[test]
    fn cumulative_index_reads_are_recorded_exactly_once() {
        let stats = EngineStats::new(Arc::new(Metrics::noop()));
        let statistics = Statistics::new(Throttle::default());
        stats.record_remaining_index_reads(
            &statistics,
            EntryIndexReadStats {
                read_operations: 3,
                read_bytes: 10,
                ..Default::default()
            },
        );
        assert_eq!(statistics.disk_read_ios(), 3);
        assert_eq!(statistics.disk_read_bytes(), 10);

        stats.record_remaining_index_reads(
            &statistics,
            EntryIndexReadStats {
                read_operations: 3,
                read_bytes: 10,
                ..Default::default()
            },
        );
        assert_eq!(statistics.disk_read_ios(), 3);
        assert_eq!(statistics.disk_read_bytes(), 10);

        stats.record_remaining_index_reads(
            &statistics,
            EntryIndexReadStats {
                read_operations: 5,
                read_bytes: 16,
                ..Default::default()
            },
        );
        assert_eq!(statistics.disk_read_ios(), 5);
        assert_eq!(statistics.disk_read_bytes(), 16);
    }
}
