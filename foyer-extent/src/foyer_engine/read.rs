use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use foyer::Metrics;

/// A non-blocking admission gate for physical reads.
///
/// The permit is owned by the blocking read itself, so cancelling its async caller does not allow
/// unbounded orphaned reads to accumulate in the blocking pool.
pub struct ReadLimiter {
    limit: usize,
    active: AtomicUsize,
    metrics: Arc<Metrics>,
    report_active: bool,
}

impl ReadLimiter {
    pub fn new(limit: usize, metrics: Arc<Metrics>) -> Arc<Self> {
        metrics.storage_engine_read_limit.absolute(limit as u64);
        metrics.storage_engine_read_active.absolute(0);
        Arc::new(Self {
            limit,
            active: AtomicUsize::new(0),
            metrics,
            report_active: true,
        })
    }

    /// Create a separate admission domain without overwriting Foyer's payload-read gauges. A
    /// rejection still contributes to the shared rejected-read counter.
    pub fn new_secondary(limit: usize, metrics: Arc<Metrics>) -> Arc<Self> {
        Arc::new(Self {
            limit,
            active: AtomicUsize::new(0),
            metrics,
            report_active: false,
        })
    }

    pub const fn limit(&self) -> usize {
        self.limit
    }

    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    pub fn try_acquire(self: &Arc<Self>) -> Option<ReadPermit> {
        let acquired = self
            .active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < self.limit).then_some(active + 1)
            })
            .is_ok();
        if !acquired {
            self.metrics.storage_engine_read_rejected.increase(1);
            return None;
        }
        if self.report_active {
            self.metrics.storage_engine_read_active.increase(1);
        }
        Some(ReadPermit { limiter: self.clone() })
    }
}

pub struct ReadPermit {
    limiter: Arc<ReadLimiter>,
}

impl Drop for ReadPermit {
    fn drop(&mut self) {
        let previous = self.limiter.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "Extent active reader count underflow");
        if self.limiter.report_active {
            self.limiter.metrics.storage_engine_read_active.decrease(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_without_waiting_at_the_limit() {
        let limiter = ReadLimiter::new(2, Arc::new(Metrics::noop()));
        let first = limiter.try_acquire().unwrap();
        let second = limiter.try_acquire().unwrap();
        assert!(limiter.try_acquire().is_none());
        assert_eq!(limiter.active(), 2);
        drop(first);
        assert!(limiter.try_acquire().is_some());
        drop(second);
    }

    #[test]
    fn metadata_miss_pressure_does_not_consume_payload_permits() {
        let metadata = ReadLimiter::new_secondary(2, Arc::new(Metrics::noop()));
        let payload = ReadLimiter::new(1, Arc::new(Metrics::noop()));
        let _first_miss = metadata.try_acquire().unwrap();
        let _second_miss = metadata.try_acquire().unwrap();
        assert!(metadata.try_acquire().is_none());

        let hit = payload
            .try_acquire()
            .expect("metadata misses must not occupy payload admission");
        assert_eq!(payload.active(), 1);
        drop(hit);
    }
}
