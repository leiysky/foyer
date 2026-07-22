#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::{
    fmt,
    sync::{
        Arc, Condvar, Mutex, MutexGuard,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    thread::JoinHandle,
};

use crate::{
    error::{Error, Result},
    store::{
        index::{EntryIndex, IndexCheckpoint},
        pool::{ExtentPool, ExtentPoolCheckpoint},
    },
};

/// Coordinates one total publication order with an independently advancing durability frontier.
///
/// The store mutation mutex is held while dirty payload is synchronized and an immutable
/// allocator/index epoch is detached. Metadata persistence then happens after that mutex is
/// released. Reclaim waits for an already captured epoch to finish before generation reuse, so
/// two allocator images can never race to publish the same state generation.
pub struct CheckpointCoordinator {
    shared: Arc<CheckpointShared>,
    worker: Option<JoinHandle<()>>,
}

struct CheckpointShared {
    index: Arc<EntryIndex>,
    pool: Arc<ExtentPool>,
    mutations: Arc<Mutex<()>>,
    dirty_bytes: Arc<AtomicUsize>,
    published_epoch: AtomicU64,
    state: Mutex<CheckpointState>,
    changed: Condvar,
    #[cfg(test)]
    pause_after_capture: AtomicBool,
    #[cfg(test)]
    fail_after_capture: AtomicBool,
    #[cfg(test)]
    test_state: Mutex<CheckpointTestState>,
    #[cfg(test)]
    test_changed: Condvar,
}

/// Snapshot of the publication and recovery frontiers.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointStats {
    pub published_epoch: u64,
    pub requested_epoch: u64,
    pub durable_epoch: u64,
    pub in_flight_epoch: Option<u64>,
    pub dirty_bytes: usize,
    pub failed: bool,
}

#[derive(Debug, Default)]
struct CheckpointState {
    requested_epoch: u64,
    durable_epoch: u64,
    in_flight_epoch: Option<u64>,
    error: Option<String>,
    shutdown: bool,
}

struct CheckpointEpoch {
    epoch: u64,
    allocator: ExtentPoolCheckpoint,
    index: Option<IndexCheckpoint>,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct CheckpointTestState {
    captured: bool,
    resume: bool,
}

impl CheckpointCoordinator {
    pub fn new(
        index: Arc<EntryIndex>,
        pool: Arc<ExtentPool>,
        mutations: Arc<Mutex<()>>,
        dirty_bytes: Arc<AtomicUsize>,
    ) -> Result<Self> {
        let shared = Arc::new(CheckpointShared {
            index,
            pool,
            mutations,
            dirty_bytes,
            published_epoch: AtomicU64::new(0),
            state: Mutex::new(CheckpointState::default()),
            changed: Condvar::new(),
            #[cfg(test)]
            pause_after_capture: AtomicBool::new(false),
            #[cfg(test)]
            fail_after_capture: AtomicBool::new(false),
            #[cfg(test)]
            test_state: Mutex::new(CheckpointTestState::default()),
            #[cfg(test)]
            test_changed: Condvar::new(),
        });
        let worker_shared = shared.clone();
        let worker = thread::Builder::new()
            .name("extent-durable".to_string())
            .spawn(move || checkpoint_worker(worker_shared))
            .map_err(|error| Error::io("spawn extent checkpoint coordinator", error))?;
        Ok(Self {
            shared,
            worker: Some(worker),
        })
    }

    /// Records one externally visible publication and its Stored Entry byte charge.
    ///
    /// The caller holds the store mutation lock.
    pub fn record_publication(&self, bytes: usize) -> Result<u64> {
        self.ensure_healthy()?;
        if bytes == 0 {
            return Ok(self.shared.published_epoch.load(Ordering::Acquire));
        }
        let current = self.shared.published_epoch.load(Ordering::Relaxed);
        let next = current
            .checked_add(1)
            .ok_or_else(|| Error::CheckpointFailed("publication epoch is exhausted".to_string()))?;
        self.shared.published_epoch.store(next, Ordering::Release);
        self.shared.dirty_bytes.fetch_add(bytes, Ordering::Relaxed);
        Ok(next)
    }

    pub fn dirty_bytes(&self) -> usize {
        self.shared.dirty_bytes.load(Ordering::Relaxed)
    }

    pub fn published_epoch(&self) -> u64 {
        self.shared.published_epoch.load(Ordering::Acquire)
    }

    pub fn stats(&self) -> CheckpointStats {
        let state = mutex_lock(&self.shared.state);
        CheckpointStats {
            published_epoch: self.shared.published_epoch.load(Ordering::Acquire),
            requested_epoch: state.requested_epoch,
            durable_epoch: state.durable_epoch,
            in_flight_epoch: state.in_flight_epoch,
            dirty_bytes: self.shared.dirty_bytes.load(Ordering::Relaxed),
            failed: state.error.is_some(),
        }
    }

    pub fn request_background(&self, epoch: u64) -> Result<()> {
        let mut state = mutex_lock(&self.shared.state);
        check_state(&state)?;
        state.requested_epoch = state.requested_epoch.max(epoch);
        self.shared.changed.notify_one();
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<()> {
        let target = self.published_epoch();
        self.request_background(target)?;
        self.wait_for(target)
    }

    /// Waits until metadata I/O for an already captured epoch has completed.
    ///
    /// The caller keeps the store mutation lock, which prevents the background worker from
    /// capturing another epoch before generation invalidation is durably published. This wait
    /// does not create a checkpoint or add I/O to reclaim.
    pub fn wait_for_idle_locked(&self) -> Result<()> {
        let mut state = mutex_lock(&self.shared.state);
        loop {
            check_state(&state)?;
            if state.in_flight_epoch.is_none() {
                return Ok(());
            }
            state = condvar_wait(&self.shared.changed, state);
        }
    }

    pub fn ensure_healthy(&self) -> Result<()> {
        check_state(&mutex_lock(&self.shared.state))
    }

    #[cfg(test)]
    pub fn pause_after_capture(&self) {
        let mut state = mutex_lock(&self.shared.test_state);
        state.captured = false;
        state.resume = false;
        self.shared.pause_after_capture.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub fn wait_until_captured(&self) {
        let mut state = mutex_lock(&self.shared.test_state);
        while !state.captured {
            state = condvar_wait(&self.shared.test_changed, state);
        }
    }

    #[cfg(test)]
    pub fn resume_checkpoint(&self) {
        let mut state = mutex_lock(&self.shared.test_state);
        state.resume = true;
        self.shared.test_changed.notify_all();
    }

    #[cfg(test)]
    pub fn fail_after_capture(&self) {
        self.shared.fail_after_capture.store(true, Ordering::Release);
    }

    fn wait_for(&self, target: u64) -> Result<()> {
        let mut state = mutex_lock(&self.shared.state);
        loop {
            check_state(&state)?;
            if state.durable_epoch >= target {
                return Ok(());
            }
            state = condvar_wait(&self.shared.changed, state);
        }
    }
}

impl fmt::Debug for CheckpointCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CheckpointCoordinator")
            .field("state", &*mutex_lock(&self.shared.state))
            .finish_non_exhaustive()
    }
}

impl Drop for CheckpointCoordinator {
    fn drop(&mut self) {
        {
            let mut state = mutex_lock(&self.shared.state);
            state.shutdown = true;
            self.shared.changed.notify_all();
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl CheckpointShared {
    fn prepare_epoch(&self, epoch: u64) -> Result<CheckpointEpoch> {
        // One checkpoint fence covers every data write in the captured publication range.
        // Recovery deliberately discards an uncheckpointed tail instead of paying per-put
        // ownership-metadata I/O.
        self.pool.sync_payload()?;
        #[cfg(test)]
        crate::store::crash_if_requested("extent_after_payload_sync");
        let allocator = self.pool.prepare_checkpoint_state()?;
        let index = self.index.prepare_checkpoint()?;
        Ok(CheckpointEpoch {
            epoch,
            allocator,
            index,
        })
    }

    fn persist_epoch(&self, mut checkpoint: CheckpointEpoch) -> Result<()> {
        let allocator_result = self.pool.persist_checkpoint_state(&checkpoint.allocator);
        if let Err(error) = allocator_result {
            if let Some(index) = checkpoint.index.take() {
                self.index.abort_checkpoint(index);
            }
            return Err(error);
        }
        #[cfg(test)]
        crate::store::crash_if_requested("extent_after_allocator_state");
        if let Some(index) = checkpoint.index.take() {
            self.index.persist_checkpoint(index)?;
        }
        #[cfg(test)]
        crate::store::crash_if_requested("extent_after_index_checkpoint");
        debug_assert!(self.published_epoch.load(Ordering::Acquire) >= checkpoint.epoch);
        Ok(())
    }

    fn complete(&self, epoch: u64, result: Result<()>) -> Result<()> {
        let mut state = mutex_lock(&self.state);
        state.in_flight_epoch = None;
        match result {
            Ok(()) => state.durable_epoch = state.durable_epoch.max(epoch),
            Err(error) => state.error = Some(error.to_string()),
        }
        self.changed.notify_all();
        check_state(&state)
    }

    #[cfg(test)]
    fn fail_after_capture_if_requested(&self, checkpoint: CheckpointEpoch) -> Result<()> {
        if !self.fail_after_capture.swap(false, Ordering::AcqRel) {
            return self.persist_epoch(checkpoint);
        }
        if let Some(index) = checkpoint.index {
            self.index.abort_checkpoint(index);
        }
        Err(Error::CheckpointFailed(
            "injected failure after immutable epoch capture".to_string(),
        ))
    }

    #[cfg(test)]
    fn pause_after_capture_if_requested(&self) {
        if !self.pause_after_capture.swap(false, Ordering::AcqRel) {
            return;
        }
        let mut state = mutex_lock(&self.test_state);
        state.captured = true;
        self.test_changed.notify_all();
        while !state.resume {
            state = condvar_wait(&self.test_changed, state);
        }
    }
}

fn checkpoint_worker(shared: Arc<CheckpointShared>) {
    loop {
        let mut state = mutex_lock(&shared.state);
        while state.error.is_none() && state.requested_epoch <= state.durable_epoch && !state.shutdown {
            state = condvar_wait(&shared.changed, state);
        }
        if state.error.is_some() || (state.shutdown && state.requested_epoch <= state.durable_epoch) {
            return;
        }
        drop(state);

        let mutation = mutex_lock(&shared.mutations);
        let target = shared.published_epoch.load(Ordering::Acquire);
        let mut state = mutex_lock(&shared.state);
        if state.error.is_some() || state.in_flight_epoch.is_some() {
            drop(state);
            drop(mutation);
            continue;
        }
        if state.durable_epoch >= target {
            drop(state);
            drop(mutation);
            continue;
        }
        state.requested_epoch = state.requested_epoch.max(target);
        state.in_flight_epoch = Some(target);
        drop(state);

        let checkpoint = shared.prepare_epoch(target);
        if checkpoint.is_ok() {
            shared.dirty_bytes.store(0, Ordering::Relaxed);
        }
        drop(mutation);
        #[cfg(test)]
        shared.pause_after_capture_if_requested();
        #[cfg(test)]
        let result = checkpoint.and_then(|checkpoint| shared.fail_after_capture_if_requested(checkpoint));
        #[cfg(not(test))]
        let result = checkpoint.and_then(|checkpoint| shared.persist_epoch(checkpoint));
        if shared.complete(target, result).is_err() {
            return;
        }
    }
}

fn check_state(state: &CheckpointState) -> Result<()> {
    match &state.error {
        Some(error) => Err(Error::CheckpointFailed(error.clone())),
        None => Ok(()),
    }
}

fn mutex_lock<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn condvar_wait<'a, T>(condvar: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    condvar.wait(guard).unwrap_or_else(|poisoned| poisoned.into_inner())
}
