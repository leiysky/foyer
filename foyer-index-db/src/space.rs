use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Error, Result};

/// Tracks on-disk usage against a soft capacity target across WAL, flush, and compaction output.
///
/// Reservations include transient files. Bytes are released only after obsolete files have been
/// unlinked and the directory entry update has been synchronized. Exceeding `capacity` is allowed;
/// callers can expose it as pressure but must not reject a write solely because of this target.
#[derive(Debug)]
pub struct DiskBudget {
    capacity: u64,
    used: AtomicU64,
}

impl DiskBudget {
    pub fn new(capacity: u64, used: u64) -> Self {
        Self {
            capacity,
            used: AtomicU64::new(used),
        }
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    pub fn reserve(&self, bytes: u64) -> Result<DiskReservation<'_>> {
        let result = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| used.checked_add(bytes));
        match result {
            Ok(_) => Ok(DiskReservation {
                budget: self,
                bytes,
                committed: false,
            }),
            Err(used) => Err(Error::InvalidOptions(format!(
                "disk usage accounting overflows u64: used={used}, requested={bytes}"
            ))),
        }
    }

    pub fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let result = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| used.checked_sub(bytes));
        assert!(result.is_ok(), "IndexDB disk budget release underflow");
    }
}

#[derive(Debug)]
pub struct DiskReservation<'a> {
    budget: &'a DiskBudget,
    bytes: u64,
    committed: bool,
}

impl DiskReservation<'_> {
    /// Converts the transient reservation into accounted live storage.
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for DiskReservation<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.budget.release(self.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservations_include_transient_bytes_allow_overcommit_and_release_on_drop() {
        let budget = DiskBudget::new(100, 40);
        let reservation = budget.reserve(50).unwrap();
        assert_eq!(budget.used(), 90);
        let overcommitted = budget.reserve(11).unwrap();
        assert_eq!(budget.used(), 101);
        drop(overcommitted);
        drop(reservation);
        assert_eq!(budget.used(), 40);

        budget.reserve(61).unwrap().commit();
        assert_eq!(budget.used(), 101);
        budget.release(61);
        assert_eq!(budget.used(), 40);
    }

    #[test]
    fn opening_usage_above_the_soft_capacity_is_allowed() {
        let budget = DiskBudget::new(100, 125);
        assert_eq!(budget.capacity(), 100);
        assert_eq!(budget.used(), 125);
    }
}
