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
}

impl ReadLimiter {
    pub fn new(limit: usize, metrics: Arc<Metrics>) -> Arc<Self> {
        metrics.storage_engine_read_limit.absolute(limit as u64);
        metrics.storage_engine_read_active.absolute(0);
        Arc::new(Self {
            limit,
            active: AtomicUsize::new(0),
            metrics,
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
        self.metrics.storage_engine_read_active.absolute(self.active() as u64);
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
        self.limiter
            .metrics
            .storage_engine_read_active
            .absolute(previous.saturating_sub(1) as u64);
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
}
