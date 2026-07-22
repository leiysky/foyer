use std::{collections::HashSet, time::Instant};

use crate::{
    error::Result,
    model::CachePriority,
    store::{
        checkpoint::CheckpointCoordinator,
        pool::{AllocationResult, EntryAllocation, ExtentPool, ExtentVictim, ReclaimCandidates},
        stats::ReclaimStats,
    },
};

/// Coordinates allocation pressure and whole-extent generation invalidation.
///
/// The caller owns the store mutation lock. Reclaim never scans Entry owners, probes the key
/// index, or copies payloads. Cache priority controls physical placement and victim selection;
/// generation validation makes every old location miss before payload I/O after an extent is
/// released.
pub struct Reclaimer<'a> {
    pool: &'a ExtentPool,
    checkpoints: &'a CheckpointCoordinator,
    priority_capacity_floors: [u32; 3],
}

impl<'a> Reclaimer<'a> {
    pub const fn new(
        pool: &'a ExtentPool,
        checkpoints: &'a CheckpointCoordinator,
        priority_capacity_floors: [u32; 3],
    ) -> Self {
        Self {
            pool,
            checkpoints,
            priority_capacity_floors,
        }
    }

    pub fn allocate(
        &self,
        priority: CachePriority,
        stored_len: usize,
        protected_extents: &HashSet<u32>,
    ) -> Result<AllocationDecision> {
        let mut reclaimed = ReclaimResult::default();
        loop {
            match self.pool.allocate(priority, stored_len)? {
                AllocationResult::Allocated(allocation) => {
                    return Ok(AllocationDecision::Allocated(allocation, reclaimed));
                }
                AllocationResult::ReclaimRequired if !protected_extents.is_empty() => {
                    // Reclaim persists allocator state. Publish pending byte ranges first so every
                    // durable cursor names only complete directory records and a sealed I/O frame.
                    return Ok(AllocationDecision::FlushRequired(reclaimed));
                }
                AllocationResult::ReclaimRequired => {}
            }
            let candidate = select_victim(self.pool.reclaim_candidates(), priority, self.priority_capacity_floors);
            let Some((victim, is_current)) = candidate else {
                return Ok(AllocationDecision::Rejected(reclaimed));
            };
            debug_assert!(!protected_extents.contains(&victim.extent));
            let victim = if is_current {
                self.pool.seal_current(victim)?
            } else {
                victim
            };
            reclaimed.merge(self.reclaim(victim)?);
        }
    }

    /// Completes a legacy Format 1 reclaim transaction left by an older process without scanning
    /// its source. The persisted target cursor remains authoritative, and source locations become
    /// generation-invalid cache misses.
    pub fn recover_pending(&self) -> Result<()> {
        let Some(transaction) = self.pool.pending_reclaim() else {
            return Ok(());
        };
        self.checkpoints.wait_for_idle_locked()?;
        self.pool.finish_reclaim(transaction)
    }

    fn reclaim(&self, victim: ExtentVictim) -> Result<ReclaimResult> {
        let started = Instant::now();
        self.pool.ensure_payload_fenced()?;
        let checkpoint_wait_started = Instant::now();
        self.checkpoints.wait_for_idle_locked()?;
        let checkpoint_wait = checkpoint_wait_started.elapsed();
        let invalidation_started = Instant::now();
        // release() increments and durably checkpoints the generation before this extent can be
        // allocated again. Existing index locations therefore fail the pre-I/O generation check.
        self.pool.release(victim)?;
        #[cfg(test)]
        crate::store::crash_if_requested("extent_reclaim_after_generation_invalidation");

        let mut stats = ReclaimStats::default();
        stats.record(victim.priority, victim.entries as usize, victim.used_bytes as usize);
        stats.record_work(
            duration_nanos(checkpoint_wait),
            duration_nanos(invalidation_started.elapsed()),
            duration_nanos(started.elapsed()),
        );
        Ok(ReclaimResult {
            stats,
            ..ReclaimResult::default()
        })
    }
}

#[derive(Debug)]
pub enum AllocationDecision {
    Allocated(EntryAllocation, ReclaimResult),
    FlushRequired(ReclaimResult),
    Rejected(ReclaimResult),
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ReclaimResult {
    pub stats: ReclaimStats,
    pub write_runs: usize,
    pub written_bytes: usize,
}

impl ReclaimResult {
    pub fn merge(&mut self, other: Self) {
        self.stats.merge(other.stats);
        self.write_runs = self.write_runs.saturating_add(other.write_runs);
        self.written_bytes = self.written_bytes.saturating_add(other.written_bytes);
    }
}

fn duration_nanos(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn select_victim(
    candidates: ReclaimCandidates,
    incoming: CachePriority,
    capacity_floors: [u32; 3],
) -> Option<(ExtentVictim, bool)> {
    let borrowed = |priority| {
        if candidates.occupied_extents(priority) > capacity_floors[priority as usize] {
            candidates.oldest(priority)
        } else {
            None
        }
    };
    match incoming {
        CachePriority::Low => candidates.oldest(CachePriority::Low),
        CachePriority::Normal => candidates
            .oldest(CachePriority::Low)
            // Normal demand repays High borrowing only while Normal is below its own protected
            // floor. Once that floor is satisfied, reclaim Normal's oldest extent instead of
            // evicting explicitly hotter data merely to grow Normal into shared capacity.
            .or_else(|| {
                (candidates.occupied_extents(CachePriority::Normal) < capacity_floors[CachePriority::Normal as usize])
                    .then(|| borrowed(CachePriority::High))
                    .flatten()
            })
            .or_else(|| candidates.oldest(CachePriority::Normal)),
        CachePriority::High => candidates
            .oldest(CachePriority::Low)
            .or_else(|| borrowed(CachePriority::Normal))
            .or_else(|| candidates.oldest(CachePriority::High)),
    }
}
