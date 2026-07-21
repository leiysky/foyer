use std::{
    cmp::Reverse,
    collections::HashSet,
    time::{Duration, Instant},
};

use crate::{
    error::Result,
    format::ContentDigest,
    model::{CachePriority, EntryKey, KeyDigest},
    store::{
        checkpoint::CheckpointCoordinator,
        format::{EntryLocation, EntryOwner},
        index::EntryIndex,
        operation::InsertOutcome,
        pool::{
            AllocationResult, EntryAllocation, EntryWrite, ExtentPool, ExtentVictim, ReclaimCandidates,
            ReclaimTransaction,
        },
        stats::ReclaimStats,
    },
};

const PROMOTION_PORTION_DENOMINATOR: usize = 8;

/// Coordinates allocation pressure, eviction, and bounded hot-entry promotion.
///
/// The caller owns the store mutation lock. This component owns the generation-reuse fence and
/// therefore cannot be invoked independently of the ordered publication path.
pub struct Reclaimer<'a> {
    index: &'a EntryIndex,
    pool: &'a ExtentPool,
    checkpoints: &'a CheckpointCoordinator,
    priority_capacity_floors: [u32; 3],
    hot_frequency: u8,
    low_hot_frequency: u8,
}

impl<'a> Reclaimer<'a> {
    pub fn new(
        index: &'a EntryIndex,
        pool: &'a ExtentPool,
        checkpoints: &'a CheckpointCoordinator,
        priority_capacity_floors: [u32; 3],
        hot_frequency: u8,
        low_hot_frequency: u8,
    ) -> Self {
        Self {
            index,
            pool,
            checkpoints,
            priority_capacity_floors,
            hot_frequency,
            low_hot_frequency,
        }
    }

    pub fn allocate(
        &self,
        priority: CachePriority,
        stored_len: usize,
        protected_extents: &HashSet<u32>,
    ) -> Result<AllocationDecision> {
        self.recover_pending()?;
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
            reclaimed.merge(self.reclaim(victim, priority)?);
        }
    }

    pub fn recover_pending(&self) -> Result<()> {
        let Some(transaction) = self.pool.pending_reclaim() else {
            return Ok(());
        };
        self.pool.sync_payload()?;
        let mut removed = Vec::new();
        for (data_offset, owner) in self.pool.entry_owners(transaction.source)? {
            let Some(location) = self.index.peek(owner.key_digest)? else {
                continue;
            };
            if location.data_offset == data_offset && location.extent_generation == transaction.source.generation {
                removed.push(owner.key_digest);
            }
        }
        self.index.remove_batch(&removed)?;
        self.index.checkpoint()?;
        self.pool.finish_reclaim(transaction)
    }

    fn reclaim(&self, victim: ExtentVictim, incoming: CachePriority) -> Result<ReclaimResult> {
        let started = Instant::now();
        // Generation reuse cannot pass an immutable checkpoint that may still reference this
        // victim. The caller has already fenced every completed physical segment; enforce that
        // boundary before closing the metadata frontier inline under the mutation lock.
        let preparation_started = Instant::now();
        self.pool.ensure_payload_fenced()?;
        self.checkpoints.checkpoint_inline_locked()?;
        let preparation_duration = preparation_started.elapsed();
        let (live, mut work) = self.live_victims(victim)?;
        work.preparation_duration = preparation_duration;
        let hot_frequency = self.frequency_threshold(victim.priority);
        if victim.priority == incoming
            && promotion_limit(self.pool.layout().extent_size) > 0
            && live.iter().any(|entry| entry.frequency >= hot_frequency)
        {
            let transaction_started = Instant::now();
            let transaction = self.pool.begin_reclaim(victim)?;
            work.transaction_duration = transaction_started.elapsed();
            #[cfg(test)]
            crate::store::crash_if_requested("extent_reclaim_after_begin");
            return self.compact(transaction, live, hot_frequency, work, started);
        }

        let evicted_entries = live.len();
        let evicted_bytes = live.iter().fold(0usize, |bytes, entry| {
            bytes.saturating_add(entry.owner.value_len as usize)
        });
        let publication_started = Instant::now();
        let keys = live.into_iter().map(|entry| entry.owner.key_digest).collect::<Vec<_>>();
        self.index.remove_batch(&keys)?;
        // The current caller-visible store batch has not advanced its publication epoch yet.
        // Persist its allocator positions before an Index checkpoint can expose those locations.
        self.pool.checkpoint_state()?;
        #[cfg(test)]
        crate::store::crash_if_requested("extent_evict_after_allocator_state");
        self.index.checkpoint()?;
        #[cfg(test)]
        crate::store::crash_if_requested("extent_evict_after_index_checkpoint");
        self.pool.release(victim)?;
        #[cfg(test)]
        crate::store::crash_if_requested("extent_evict_after_release");
        let mut stats = ReclaimStats::default();
        stats.record(victim.priority, evicted_entries, 0, evicted_bytes, 0);
        work.record(
            &mut stats,
            Duration::ZERO,
            publication_started.elapsed(),
            started.elapsed(),
        );
        Ok(ReclaimResult {
            stats,
            ..Default::default()
        })
    }

    fn live_victims(&self, victim: ExtentVictim) -> Result<(Vec<LiveVictim>, ReclaimWork)> {
        let directory_before = self.pool.directory_read_stats();
        let directory_started = Instant::now();
        let owners = self.pool.entry_owners(victim)?;
        let directory_duration = directory_started.elapsed();
        let directory_after = self.pool.directory_read_stats();
        let scanned_entries = owners.len() as u64;
        let index_started = Instant::now();
        let mut live = Vec::new();
        for (data_offset, owner) in owners {
            let Some(location) = self.index.peek(owner.key_digest)? else {
                continue;
            };
            if location.data_offset == data_offset && location.extent_generation == victim.generation {
                live.push(LiveVictim {
                    owner,
                    location,
                    frequency: self.index.estimated_frequency(owner.key_digest),
                });
            }
        }
        Ok((
            live,
            ReclaimWork {
                scanned_entries,
                directory_read_runs: directory_after.runs.saturating_sub(directory_before.runs),
                directory_read_bytes: directory_after.bytes.saturating_sub(directory_before.bytes),
                index_lookups: scanned_entries,
                preparation_duration: Duration::ZERO,
                directory_duration,
                index_duration: index_started.elapsed(),
                transaction_duration: Duration::ZERO,
            },
        ))
    }

    fn compact(
        &self,
        transaction: ReclaimTransaction,
        mut live: Vec<LiveVictim>,
        hot_frequency: u8,
        work: ReclaimWork,
        started: Instant,
    ) -> Result<ReclaimResult> {
        let promotion_started = Instant::now();
        live.sort_unstable_by_key(|entry| (Reverse(entry.frequency), Reverse(entry.owner.sequence)));
        // Retain at most the hottest eighth. That guarantees each compaction frees seven eighths
        // of its source and caps promotion write amplification at one seventh.
        let promotion_limit = promotion_limit(self.pool.layout().extent_size);
        let mut promoted_capacity = 0usize;
        let mut promotions = Vec::new();
        for entry in live.iter().filter(|entry| entry.frequency >= hot_frequency) {
            let stored_len = entry.location.stored_len as usize;
            let charge = stored_len.max(self.pool.layout().entry_charge);
            if promoted_capacity.saturating_add(charge) > promotion_limit {
                continue;
            }
            let Some((key, value)) = self.pool.read_stored_entry(entry.location)? else {
                continue;
            };
            let key_digest = KeyDigest::for_key(&key);
            if key_digest != entry.owner.key_digest {
                continue;
            }
            let Some(allocation) = self.pool.allocate_reclaim_target(transaction, stored_len)? else {
                break;
            };
            promoted_capacity += charge;
            promotions.push(Promotion {
                key,
                key_digest,
                allocation,
                value,
                content_digest: entry.location.content_digest,
            });
        }
        let writes = promotions
            .iter()
            .map(|promotion| EntryWrite {
                allocation: promotion.allocation,
                key: &promotion.key,
                key_digest: promotion.key_digest,
                value: &promotion.value,
                content_digest: promotion.content_digest,
            })
            .collect::<Vec<_>>();
        let written = self.pool.write_batch(&writes)?;
        self.pool.sync_payload()?;
        let promotion_duration = promotion_started.elapsed();
        #[cfg(test)]
        crate::store::crash_if_requested("extent_reclaim_after_payload_sync");

        let publication_started = Instant::now();
        let removed = live.iter().map(|entry| entry.owner.key_digest).collect::<Vec<_>>();
        self.index.remove_batch(&removed)?;
        let promoted = promotions
            .iter()
            .zip(written.locations.iter().copied())
            .map(|(promotion, location)| (promotion.key_digest, location))
            .collect::<Vec<_>>();
        let outcomes = self.index.insert_batch(&promoted)?;
        assert!(
            outcomes
                .outcomes
                .iter()
                .all(|outcome| *outcome != InsertOutcome::Rejected),
            "a promoted existing index entry must be admitted"
        );
        self.index.checkpoint()?;
        #[cfg(test)]
        crate::store::crash_if_requested("extent_reclaim_after_index_checkpoint");
        self.pool.finish_reclaim(transaction)?;
        let promoted_entries = promotions.len();
        let promoted_bytes = promotions
            .iter()
            .fold(0usize, |bytes, promotion| bytes.saturating_add(promotion.value.len()));
        let live_bytes = live.iter().fold(0usize, |bytes, entry| {
            bytes.saturating_add(entry.owner.value_len as usize)
        });
        let mut stats = ReclaimStats::default();
        stats.record(
            transaction.priority,
            live.len().saturating_sub(promoted_entries),
            promoted_entries,
            live_bytes.saturating_sub(promoted_bytes),
            promoted_bytes,
        );
        work.record(
            &mut stats,
            promotion_duration,
            publication_started.elapsed(),
            started.elapsed(),
        );
        Ok(ReclaimResult {
            stats,
            write_runs: written.data_runs.saturating_add(written.entry_directory_runs),
            written_bytes: written.data_bytes.saturating_add(written.entry_directory_bytes),
        })
    }

    fn frequency_threshold(&self, priority: CachePriority) -> u8 {
        match priority {
            CachePriority::Low => self.low_hot_frequency,
            CachePriority::Normal | CachePriority::High => self.hot_frequency,
        }
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

#[derive(Debug, Clone, Copy)]
struct LiveVictim {
    owner: EntryOwner,
    location: EntryLocation,
    frequency: u8,
}

#[derive(Debug, Clone, Copy)]
struct ReclaimWork {
    scanned_entries: u64,
    directory_read_runs: u64,
    directory_read_bytes: u64,
    index_lookups: u64,
    preparation_duration: Duration,
    directory_duration: Duration,
    index_duration: Duration,
    transaction_duration: Duration,
}

impl ReclaimWork {
    fn record(self, stats: &mut ReclaimStats, promotion: Duration, publication: Duration, total: Duration) {
        stats.record_work(
            self.scanned_entries,
            self.directory_read_runs,
            self.directory_read_bytes,
            self.index_lookups,
            duration_nanos(self.preparation_duration),
            duration_nanos(self.directory_duration),
            duration_nanos(self.index_duration),
            duration_nanos(self.transaction_duration),
            duration_nanos(promotion),
            duration_nanos(publication),
            duration_nanos(total),
        );
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[derive(Debug)]
struct Promotion {
    key: EntryKey,
    key_digest: KeyDigest,
    allocation: EntryAllocation,
    value: Vec<u8>,
    content_digest: ContentDigest,
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
            .or_else(|| borrowed(CachePriority::High))
            .or_else(|| candidates.oldest(CachePriority::Normal)),
        CachePriority::High => candidates
            .oldest(CachePriority::Low)
            .or_else(|| borrowed(CachePriority::Normal))
            .or_else(|| candidates.oldest(CachePriority::High)),
    }
}

pub fn promotion_limit(extent_size: usize) -> usize {
    (extent_size / PROMOTION_PORTION_DENOMINATOR).min(extent_size.saturating_sub(1))
}
