use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Error, Result};

/// Enforces the fixed on-disk budget across WAL, flush, and compaction output.
///
/// Reservations include transient files. Bytes are released only after obsolete files have been
/// unlinked and the directory entry update has been synchronized.
#[derive(Debug)]
pub struct DiskBudget {
    capacity: u64,
    used: AtomicU64,
}

impl DiskBudget {
    pub fn new(capacity: u64, used: u64) -> Result<Self> {
        if used > capacity {
            return Err(Error::CapacityExceeded {
                capacity,
                used,
                requested: 0,
            });
        }
        Ok(Self {
            capacity,
            used: AtomicU64::new(used),
        })
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    pub fn reserve(&self, bytes: u64) -> Result<DiskReservation<'_>> {
        let result = self.used.fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
            used.checked_add(bytes).filter(|next| *next <= self.capacity)
        });
        match result {
            Ok(_) => Ok(DiskReservation {
                budget: self,
                bytes,
                committed: false,
            }),
            Err(used) => Err(Error::CapacityExceeded {
                capacity: self.capacity,
                used,
                requested: bytes,
            }),
        }
    }

    pub fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let result = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| used.checked_sub(bytes));
        assert!(result.is_ok(), "fixed-lsm disk budget release underflow");
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
    fn reservations_include_transient_bytes_and_release_on_drop() {
        let budget = DiskBudget::new(100, 40).unwrap();
        let reservation = budget.reserve(50).unwrap();
        assert_eq!(budget.used(), 90);
        assert!(matches!(budget.reserve(11), Err(Error::CapacityExceeded { .. })));
        drop(reservation);
        assert_eq!(budget.used(), 40);

        budget.reserve(60).unwrap().commit();
        assert_eq!(budget.used(), 100);
        budget.release(60);
        assert_eq!(budget.used(), 40);
    }
}
