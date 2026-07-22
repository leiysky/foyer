use std::{
    collections::HashMap,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, RecvTimeoutError, TryRecvError},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use bytes::Bytes;
use foyer::{Metrics, Statistics};

use crate::{
    ReclaimStats,
    foyer_engine::{ExtentPiece, mutex_lock, queue::QueueReservation, stats::EngineStats},
    model::EntryKey,
    store::{BatchInsertResult, EntryInsert, ExtentStore, InsertOutcome},
};

/// Preserves the first asynchronous failure so close reports the causal error.
pub struct BackgroundError {
    failed: AtomicBool,
    message: Mutex<Option<String>>,
    metrics: Arc<Metrics>,
}

impl BackgroundError {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            failed: AtomicBool::new(false),
            message: Mutex::new(None),
            metrics,
        }
    }

    pub fn record(&self, message: String) {
        let mut current = mutex_lock(&self.message);
        if current.is_none() {
            *current = Some(message);
            self.metrics.storage_error.increase(1);
            self.metrics.storage_engine_healthy.absolute(0);
            self.failed.store(true, Ordering::Release);
        }
    }

    pub fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    pub fn message(&self) -> Option<String> {
        mutex_lock(&self.message).clone()
    }
}

/// One accepted write whose reservation remains charged until the command is dropped.
pub enum Command {
    Put {
        piece: ExtentPiece,
        reservation: QueueReservation,
        queued_at: Instant,
    },
    Delete {
        key: Bytes,
        reservation: QueueReservation,
        queued_at: Instant,
    },
}

impl Command {
    pub fn put(piece: ExtentPiece, reservation: QueueReservation) -> Self {
        Self::Put {
            piece,
            reservation,
            queued_at: Instant::now(),
        }
    }

    pub fn delete(key: Bytes, reservation: QueueReservation) -> Self {
        Self::Delete {
            key,
            reservation,
            queued_at: Instant::now(),
        }
    }

    const fn charge(&self) -> usize {
        match self {
            Self::Put { reservation, .. } | Self::Delete { reservation, .. } => reservation.bytes(),
        }
    }

    const fn queued_at(&self) -> Instant {
        match self {
            Self::Put { queued_at, .. } | Self::Delete { queued_at, .. } => *queued_at,
        }
    }
}

/// Owns the single ordered publication stream from Foyer into ExtentStore.
pub struct WriteWorker {
    store: Arc<ExtentStore>,
    statistics: Arc<Statistics>,
    batch_entries: usize,
    batch_bytes: usize,
    batch_delay: Duration,
    checkpoint_interval: Duration,
    background_error: Arc<BackgroundError>,
    stats: Arc<EngineStats>,
    shutdown: Arc<AtomicBool>,
    #[cfg(test)]
    panic_next: Arc<AtomicBool>,
}

impl WriteWorker {
    pub fn new(
        store: Arc<ExtentStore>,
        statistics: Arc<Statistics>,
        batch_entries: usize,
        batch_bytes: usize,
        batch_delay: Duration,
        checkpoint_interval: Duration,
        background_error: Arc<BackgroundError>,
        stats: Arc<EngineStats>,
        shutdown: Arc<AtomicBool>,
        #[cfg(test)] panic_next: Arc<AtomicBool>,
    ) -> Self {
        Self {
            store,
            statistics,
            batch_entries,
            batch_bytes,
            batch_delay,
            checkpoint_interval,
            background_error,
            stats,
            shutdown,
            #[cfg(test)]
            panic_next,
        }
    }

    pub fn spawn(self, receiver: Receiver<Command>) -> io::Result<JoinHandle<()>> {
        std::thread::Builder::new()
            .name("extent-flush".to_string())
            .spawn(move || self.run(&receiver))
    }

    fn run(self, receiver: &Receiver<Command>) {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.write_loop(receiver);
        }));
        if outcome.is_err() {
            self.background_error.record("Extent flush worker panicked".to_string());
            self.stats.record_abandoned(drain_abandoned(receiver));
        }
        if let Err(error) = sync_store(&self.store, self.statistics.as_ref(), &self.stats) {
            self.background_error.record(format!("sync ExtentEngine: {error}"));
        }
        self.stats.record_checkpoint(self.store.checkpoint_stats());
    }

    fn write_loop(&self, receiver: &Receiver<Command>) {
        let mut checkpoint_requested_at = Instant::now();
        loop {
            let timeout = self
                .checkpoint_interval
                .saturating_sub(checkpoint_requested_at.elapsed());
            let first = match receiver.recv_timeout(timeout) {
                Ok(first) => first,
                Err(RecvTimeoutError::Timeout) => {
                    if let Err(error) = self.store.request_checkpoint() {
                        self.background_error
                            .record(format!("request periodic Extent checkpoint: {error}"));
                        return;
                    }
                    self.stats.record_checkpoint(self.store.checkpoint_stats());
                    checkpoint_requested_at = Instant::now();
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => return,
            };
            if self.shutdown.load(Ordering::Acquire) {
                let abandoned = 1usize.saturating_add(drain_abandoned(receiver));
                self.stats.record_shutdown_dropped(abandoned);
                return;
            }
            #[cfg(test)]
            if self.panic_next.swap(false, Ordering::AcqRel) {
                panic!("injected Extent flush worker panic");
            }
            let mut bytes = first.charge();
            let mut commands = Vec::with_capacity(self.batch_entries.min(1_024));
            commands.push(first);
            while commands.len() < self.batch_entries && bytes < self.batch_bytes {
                match receiver.try_recv() {
                    Ok(command) => {
                        bytes = bytes.saturating_add(command.charge());
                        commands.push(command);
                    }
                    Err(TryRecvError::Disconnected) => break,
                    Err(TryRecvError::Empty) => {
                        // Bound the coalescing delay from enqueue, not from the moment the worker
                        // finally dequeues the first command. Backlogged commands therefore never
                        // pay an additional batching delay after already waiting in the queue.
                        let remaining = self.batch_delay.saturating_sub(commands[0].queued_at().elapsed());
                        if remaining.is_zero() {
                            break;
                        }
                        match receiver.recv_timeout(remaining) {
                            Ok(command) => {
                                bytes = bytes.saturating_add(command.charge());
                                commands.push(command);
                            }
                            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => break,
                        }
                    }
                }
            }

            if !self.wait_for_write_budget() {
                let abandoned = commands.len().saturating_add(drain_abandoned(receiver));
                self.stats.record_shutdown_dropped(abandoned);
                return;
            }

            let started = Instant::now();
            match process_commands(&self.store, &commands) {
                Ok(result) => {
                    self.stats.merge_reclaim(result.reclaim);
                    self.stats
                        .record_completed_batch(commands.len(), result.storage_rejected_puts);
                    self.stats.record_synchronous_writes(
                        self.statistics.as_ref(),
                        result.write_runs,
                        result.written_bytes,
                    );
                    self.stats.record_extent_occupancy(self.store.extent_occupancy());
                }
                Err(error) => {
                    self.stats.record_failed_batch();
                    self.stats.record_write_batch(started.elapsed());
                    self.background_error.record(format!("flush Extent entries: {error}"));
                    return;
                }
            }
            self.stats
                .record_remaining_index_reads(self.statistics.as_ref(), self.store.entry_index_io_read_stats());
            let completed = Instant::now();
            self.stats.record_write_batch(completed.duration_since(started));
            self.stats.record_write_publications(
                commands
                    .iter()
                    .map(|command| completed.duration_since(command.queued_at())),
            );
            self.stats.record_checkpoint(self.store.checkpoint_stats());

            // Dropping pieces removes them from Foyer's pending-write keeper before their queue
            // reservations are released, so `wait` cannot observe a stale pending piece.
            drop(commands);

            if checkpoint_requested_at.elapsed() >= self.checkpoint_interval {
                if let Err(error) = self.store.request_checkpoint() {
                    self.background_error
                        .record(format!("request periodic Extent checkpoint: {error}"));
                    return;
                }
                self.stats.record_checkpoint(self.store.checkpoint_stats());
                checkpoint_requested_at = Instant::now();
            }
        }
    }

    fn wait_for_write_budget(&self) -> bool {
        loop {
            if self.shutdown.load(Ordering::Acquire) {
                return false;
            }
            let delay = self.statistics.write_throttle();
            if delay.is_zero() {
                return true;
            }
            // Keep shutdown responsive even when the configured token-bucket debt is large.
            std::thread::sleep(delay.min(Duration::from_millis(10)));
        }
    }
}

pub fn sync_store(store: &ExtentStore, statistics: &Statistics, stats: &EngineStats) -> crate::Result<()> {
    let result = store.sync();
    stats.record_remaining_index_reads(statistics, store.entry_index_io_read_stats());
    let physical = store.physical_write_stats();
    stats.record_remaining_writes(statistics, physical.total_runs(), physical.total_bytes());
    result
}

fn process_commands(store: &ExtentStore, commands: &[Command]) -> crate::Result<ProcessResult> {
    let mut result = ProcessResult::default();
    let mut last_commands = HashMap::<&[u8], usize>::with_capacity(commands.len());
    for (position, command) in commands.iter().enumerate() {
        last_commands.insert(command_key(command), position);
    }
    let mut positions = last_commands.into_values().collect::<Vec<_>>();
    positions.sort_unstable();

    let mut put_keys = Vec::new();
    let mut put_pieces = Vec::new();
    let mut deletes = Vec::new();
    for position in positions {
        match &commands[position] {
            Command::Put { piece, .. } => {
                put_keys.push(EntryKey::new(piece.key())?);
                put_pieces.push(piece);
            }
            Command::Delete { key, .. } => deletes.push(EntryKey::new(key)?),
        }
    }
    if !put_keys.is_empty() {
        let inserts = put_pieces
            .iter()
            .zip(&put_keys)
            .map(|(piece, key)| EntryInsert::new(key, piece.value().value(), piece.value().priority()))
            .collect::<Vec<_>>();
        let inserted = store.insert_batch_with_stats(&inserts)?;
        result.merge(inserted);
    }
    store.remove_batch(&deletes)?;
    Ok(result)
}

fn command_key(command: &Command) -> &[u8] {
    match command {
        Command::Put { piece, .. } => piece.key().as_ref(),
        Command::Delete { key, .. } => key.as_ref(),
    }
}

#[derive(Debug, Default)]
struct ProcessResult {
    reclaim: ReclaimStats,
    write_runs: usize,
    written_bytes: usize,
    storage_rejected_puts: usize,
}

impl ProcessResult {
    fn merge(&mut self, result: BatchInsertResult) {
        self.reclaim.merge(result.reclaim);
        self.write_runs = self.write_runs.saturating_add(result.write_runs);
        self.written_bytes = self.written_bytes.saturating_add(result.written_bytes);
        self.storage_rejected_puts = self.storage_rejected_puts.saturating_add(
            result
                .outcomes
                .iter()
                .filter(|outcome| **outcome == InsertOutcome::Rejected)
                .count(),
        );
    }
}

fn drain_abandoned(receiver: &Receiver<Command>) -> usize {
    let mut commands = 0usize;
    while receiver.try_recv().is_ok() {
        commands = commands.saturating_add(1);
    }
    commands
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn background_error_preserves_the_first_failure() {
        let error = BackgroundError::new(Arc::new(Metrics::noop()));
        error.record("first".to_string());
        error.record("second".to_string());
        assert!(error.is_failed());
        assert_eq!(error.message().as_deref(), Some("first"));
    }
}
