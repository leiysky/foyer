use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use foyer::Metrics;
use tokio::sync::Notify;

/// Hard write-memory bounds shared by queued and currently executing engine commands.
///
/// Admission into this queue depends only on write-pipeline occupancy. Read activity and cache
/// priority are deliberately not inputs: priority governs persistent placement and reclaim, while
/// physical read/write arbitration belongs to the I/O scheduler below the queue.
pub struct SubmissionQueue {
    capacity_entries: usize,
    capacity_bytes: usize,
    pending_entries: AtomicUsize,
    pending_bytes: AtomicUsize,
    notify: Notify,
    metrics: Arc<Metrics>,
}

impl SubmissionQueue {
    pub fn new(capacity_entries: usize, capacity_bytes: usize, metrics: Arc<Metrics>) -> Self {
        metrics
            .storage_engine_queue_capacity_entries
            .absolute(capacity_entries as u64);
        metrics
            .storage_engine_queue_capacity_bytes
            .absolute(capacity_bytes as u64);
        metrics.storage_engine_queue_pending_entries.absolute(0);
        metrics.storage_engine_queue_pending_bytes.absolute(0);
        Self {
            capacity_entries,
            capacity_bytes,
            pending_entries: AtomicUsize::new(0),
            pending_bytes: AtomicUsize::new(0),
            notify: Notify::new(),
            metrics,
        }
    }

    pub const fn capacity_entries(&self) -> usize {
        self.capacity_entries
    }

    pub const fn capacity_bytes(&self) -> usize {
        self.capacity_bytes
    }

    pub fn pending_entries(&self) -> usize {
        self.pending_entries.load(Ordering::Acquire)
    }

    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes.load(Ordering::Acquire)
    }

    pub fn try_reserve(self: &Arc<Self>, bytes: usize) -> Option<QueueReservation> {
        if bytes > self.capacity_bytes {
            return None;
        }

        let mut entries = self.pending_entries.load(Ordering::Acquire);
        loop {
            if entries >= self.capacity_entries {
                return None;
            }
            match self
                .pending_entries
                .compare_exchange_weak(entries, entries + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(observed) => entries = observed,
            }
        }

        let mut current = self.pending_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                self.rollback_entry();
                return None;
            };
            if next > self.capacity_bytes {
                self.rollback_entry();
                return None;
            }
            match self
                .pending_bytes
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }

        self.metrics.storage_engine_queue_pending_entries.increase(1);
        self.metrics.storage_engine_queue_pending_bytes.increase(bytes as u64);
        Some(QueueReservation {
            queue: self.clone(),
            bytes,
        })
    }

    fn release(&self, bytes: usize) {
        let previous_bytes = self.pending_bytes.fetch_sub(bytes, Ordering::AcqRel);
        assert!(previous_bytes >= bytes, "Extent pending queue byte count underflow");
        let previous_entries = self.pending_entries.fetch_sub(1, Ordering::AcqRel);
        assert!(previous_entries > 0, "Extent pending queue entry count underflow");
        self.metrics.storage_engine_queue_pending_entries.decrease(1);
        self.metrics.storage_engine_queue_pending_bytes.decrease(bytes as u64);
        self.notify.notify_waiters();
    }

    fn rollback_entry(&self) {
        let previous_entries = self.pending_entries.fetch_sub(1, Ordering::AcqRel);
        assert!(previous_entries > 0, "Extent pending queue entry count underflow");
        self.notify.notify_waiters();
    }

    pub async fn wait(&self) {
        loop {
            if self.pending_entries.load(Ordering::Acquire) == 0 {
                return;
            }
            let notified = self.notify.notified();
            if self.pending_entries.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// Releases one command's entry and byte budgets whenever the command leaves the pipeline.
pub struct QueueReservation {
    queue: Arc<SubmissionQueue>,
    bytes: usize,
}

impl QueueReservation {
    pub const fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for QueueReservation {
    fn drop(&mut self) {
        self.queue.release(self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservation_enforces_entry_and_byte_limits() {
        let queue = Arc::new(SubmissionQueue::new(2, 10, Arc::new(Metrics::noop())));

        let first = queue.try_reserve(6).unwrap();
        assert!(queue.try_reserve(5).is_none());
        assert_eq!(queue.pending_entries(), 1);
        assert_eq!(queue.pending_bytes(), 6);

        let second = queue.try_reserve(4).unwrap();
        assert!(queue.try_reserve(0).is_none());
        assert_eq!(queue.pending_entries(), 2);
        assert_eq!(queue.pending_bytes(), 10);

        drop((first, second));
        assert_eq!(queue.pending_entries(), 0);
        assert_eq!(queue.pending_bytes(), 0);
    }

    #[test]
    fn failed_byte_reservation_releases_entry_budget() {
        let queue = Arc::new(SubmissionQueue::new(1, 10, Arc::new(Metrics::noop())));
        assert!(queue.try_reserve(11).is_none());
        assert_eq!(queue.pending_entries(), 0);
        assert_eq!(queue.pending_bytes(), 0);
    }

    #[test]
    fn randomized_concurrent_reservations_stay_within_hard_bounds() {
        let queue = Arc::new(SubmissionQueue::new(128, 1024 * 1024, Arc::new(Metrics::noop())));
        let mut workers = Vec::new();
        for worker in 0..8u64 {
            let queue = queue.clone();
            workers.push(std::thread::spawn(move || {
                let mut state = 0x9e37_79b9_7f4a_7c15u64 ^ worker;
                let mut reservations = Vec::new();
                for _ in 0..10_000 {
                    state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                    if !reservations.is_empty() && (state & 3) == 0 {
                        let index = state as usize % reservations.len();
                        reservations.swap_remove(index);
                    } else {
                        let bytes = ((state >> 16) as usize % (32 * 1024)) + 1;
                        if let Some(reservation) = queue.try_reserve(bytes) {
                            reservations.push(reservation);
                        }
                    }
                    assert!(queue.pending_entries() <= queue.capacity_entries());
                    assert!(queue.pending_bytes() <= queue.capacity_bytes());
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(queue.pending_entries(), 0);
        assert_eq!(queue.pending_bytes(), 0);
    }
}
