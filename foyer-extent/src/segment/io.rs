use std::{
    fmt, io,
    sync::{
        Condvar, Mutex, MutexGuard,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IoClass {
    Read,
    Write,
}

#[derive(Debug, Default)]
struct State {
    active_writes: usize,
}

/// Cumulative admission statistics for Extent payload I/O.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IoSchedulerStats {
    pub read_priority_duration: Duration,
    pub active_reads: usize,
    pub active_writes: usize,
    pub waiting_writes: usize,
    pub write_operations: u64,
    pub read_priority_waits: u64,
    pub write_limit_waits: u64,
    pub total_write_wait: Duration,
    pub maximum_write_wait: Duration,
}

impl IoSchedulerStats {
    pub const fn enabled(self) -> bool {
        !self.read_priority_duration.is_zero()
    }
}

/// A cooperative scheduler for Extent's synchronous payload I/O.
///
/// Callers retain buffer ownership and execute the positional syscall themselves after admission.
/// Reads use a lock-free accounting path and never wait behind writes. A write waits for a
/// read-quiescent point, but may proceed after the bounded read-priority interval so sustained read
/// traffic cannot starve cache publication or reclaim forever.
pub struct SegmentIoScheduler {
    state: Mutex<State>,
    changed: Condvar,
    write_concurrency: usize,
    read_priority_duration: Duration,
    waiting_writes: AtomicUsize,
    active_reads: AtomicUsize,
    write_operations: AtomicU64,
    read_priority_waits: AtomicU64,
    write_limit_waits: AtomicU64,
    total_write_wait_ns: AtomicU64,
    maximum_write_wait_ns: AtomicU64,
}

impl fmt::Debug for SegmentIoScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = mutex_lock(&self.state);
        f.debug_struct("SegmentIoScheduler")
            .field("write_concurrency", &self.write_concurrency)
            .field("read_priority_duration", &self.read_priority_duration)
            .field("waiting_writes", &self.waiting_writes.load(Ordering::Relaxed))
            .field("active_reads", &self.active_reads.load(Ordering::Relaxed))
            .field("active_writes", &state.active_writes)
            .finish()
    }
}

impl SegmentIoScheduler {
    pub fn new(write_concurrency: usize, read_priority_duration: Duration) -> io::Result<Self> {
        if write_concurrency == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Extent write I/O concurrency must be positive",
            ));
        }
        Ok(Self {
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
            write_concurrency,
            read_priority_duration,
            waiting_writes: AtomicUsize::new(0),
            active_reads: AtomicUsize::new(0),
            write_operations: AtomicU64::new(0),
            read_priority_waits: AtomicU64::new(0),
            write_limit_waits: AtomicU64::new(0),
            total_write_wait_ns: AtomicU64::new(0),
            maximum_write_wait_ns: AtomicU64::new(0),
        })
    }

    pub fn read<T, E>(&self, operation: impl FnOnce() -> Result<T, E>) -> Result<T, E> {
        if self.read_priority_duration.is_zero() {
            return operation();
        }
        let _permit = self.acquire_read();
        operation()
    }

    pub fn write<T, E>(&self, operation: impl FnOnce() -> Result<T, E>) -> Result<T, E> {
        if self.read_priority_duration.is_zero() {
            return operation();
        }
        let _permit = self.acquire_write();
        operation()
    }

    pub fn stats(&self) -> IoSchedulerStats {
        let state = mutex_lock(&self.state);
        IoSchedulerStats {
            read_priority_duration: self.read_priority_duration,
            active_reads: self.active_reads.load(Ordering::Relaxed),
            active_writes: state.active_writes,
            waiting_writes: self.waiting_writes.load(Ordering::Relaxed),
            write_operations: self.write_operations.load(Ordering::Relaxed),
            read_priority_waits: self.read_priority_waits.load(Ordering::Relaxed),
            write_limit_waits: self.write_limit_waits.load(Ordering::Relaxed),
            total_write_wait: Duration::from_nanos(self.total_write_wait_ns.load(Ordering::Relaxed)),
            maximum_write_wait: Duration::from_nanos(self.maximum_write_wait_ns.load(Ordering::Relaxed)),
        }
    }

    fn acquire_read(&self) -> IoPermit<'_> {
        self.active_reads.fetch_add(1, Ordering::AcqRel);
        IoPermit {
            scheduler: self,
            class: IoClass::Read,
        }
    }

    fn acquire_write(&self) -> IoPermit<'_> {
        let started = Instant::now();
        let mut waited_for_reads = false;
        let mut waited_for_limit = false;
        self.write_operations.fetch_add(1, Ordering::Relaxed);
        self.waiting_writes.fetch_add(1, Ordering::AcqRel);
        let mut state = mutex_lock(&self.state);
        loop {
            let active_reads = self.active_reads.load(Ordering::Acquire);
            let elapsed = started.elapsed();
            let admitted = state.active_writes < self.write_concurrency
                && (active_reads == 0 || elapsed >= self.read_priority_duration);
            if admitted {
                let waiting = self.waiting_writes.fetch_sub(1, Ordering::AcqRel);
                debug_assert!(waiting > 0);
                state.active_writes = state.active_writes.saturating_add(1);
                self.record_wait(started.elapsed(), waited_for_reads, waited_for_limit);
                return IoPermit {
                    scheduler: self,
                    class: IoClass::Write,
                };
            }

            waited_for_limit |= state.active_writes >= self.write_concurrency;
            waited_for_reads |= active_reads > 0 && elapsed < self.read_priority_duration;
            let remaining = self.read_priority_duration.saturating_sub(elapsed);
            if !remaining.is_zero() && state.active_writes < self.write_concurrency {
                let (next, _) = condvar_wait_timeout(&self.changed, state, remaining);
                state = next;
            } else {
                state = condvar_wait(&self.changed, state);
            }
        }
    }

    fn record_wait(&self, wait: Duration, waited_for_reads: bool, waited_for_limit: bool) {
        if waited_for_reads {
            self.read_priority_waits.fetch_add(1, Ordering::Relaxed);
        }
        if waited_for_limit {
            self.write_limit_waits.fetch_add(1, Ordering::Relaxed);
        }
        if !waited_for_reads && !waited_for_limit {
            return;
        }
        let nanos = wait.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.total_write_wait_ns.fetch_add(nanos, Ordering::Relaxed);
        self.maximum_write_wait_ns.fetch_max(nanos, Ordering::Relaxed);
    }
}

struct IoPermit<'a> {
    scheduler: &'a SegmentIoScheduler,
    class: IoClass,
}

impl Drop for IoPermit<'_> {
    fn drop(&mut self) {
        match self.class {
            IoClass::Read => {
                let active = self.scheduler.active_reads.fetch_sub(1, Ordering::AcqRel);
                debug_assert!(active > 0);
                if self.scheduler.waiting_writes.load(Ordering::Acquire) > 0 {
                    let _state = mutex_lock(&self.scheduler.state);
                    self.scheduler.changed.notify_all();
                }
            }
            IoClass::Write => {
                let mut state = mutex_lock(&self.scheduler.state);
                debug_assert!(state.active_writes > 0);
                state.active_writes -= 1;
                self.scheduler.changed.notify_all();
            }
        }
    }
}

fn mutex_lock<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn condvar_wait<'a, T>(condvar: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    condvar.wait(guard).unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn condvar_wait_timeout<'a, T>(
    condvar: &Condvar,
    guard: MutexGuard<'a, T>,
    timeout: Duration,
) -> (MutexGuard<'a, T>, bool) {
    match condvar.wait_timeout(guard, timeout) {
        Ok((guard, result)) => (guard, result.timed_out()),
        Err(poisoned) => {
            let (guard, result) = poisoned.into_inner();
            (guard, result.timed_out())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, mpsc},
        thread,
        time::Duration,
    };

    use super::*;

    #[test]
    fn read_does_not_wait_for_an_active_write() {
        let scheduler = Arc::new(SegmentIoScheduler::new(1, Duration::from_secs(1)).unwrap());
        let (write_started, write_started_rx) = mpsc::channel();
        let (release_write, release_write_rx) = mpsc::channel();
        let writer = {
            let scheduler = scheduler.clone();
            thread::spawn(move || {
                scheduler
                    .write(|| {
                        write_started.send(()).unwrap();
                        release_write_rx.recv().unwrap();
                        Ok::<_, io::Error>(())
                    })
                    .unwrap();
            })
        };
        write_started_rx.recv().unwrap();

        let (read_completed, read_completed_rx) = mpsc::channel();
        let reader = {
            let scheduler = scheduler.clone();
            thread::spawn(move || {
                scheduler.read(|| Ok::<_, io::Error>(())).unwrap();
                read_completed.send(()).unwrap();
            })
        };
        read_completed_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        release_write.send(()).unwrap();
        reader.join().unwrap();
        writer.join().unwrap();
    }

    #[test]
    fn write_waits_for_read_quiescence_but_cannot_starve() {
        let priority = Duration::from_millis(20);
        let scheduler = Arc::new(SegmentIoScheduler::new(1, priority).unwrap());
        let read = scheduler.acquire_read();
        let (elapsed, elapsed_rx) = mpsc::channel();
        let writer = {
            let scheduler = scheduler.clone();
            thread::spawn(move || {
                let started = Instant::now();
                scheduler.write(|| Ok::<_, io::Error>(())).unwrap();
                elapsed.send(started.elapsed()).unwrap();
            })
        };

        let waited = elapsed_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(waited >= priority);
        drop(read);
        writer.join().unwrap();

        let stats = scheduler.stats();
        assert_eq!(stats.write_operations, 1);
        assert_eq!(stats.read_priority_waits, 1);
        assert_eq!(stats.write_limit_waits, 0);
        assert!(stats.total_write_wait >= priority);
        assert!(stats.maximum_write_wait >= priority);
    }

    #[test]
    fn write_proceeds_when_reads_become_quiescent() {
        let priority = Duration::from_secs(5);
        let scheduler = Arc::new(SegmentIoScheduler::new(1, priority).unwrap());
        let read = scheduler.acquire_read();
        let (started, started_rx) = mpsc::channel();
        let (completed, completed_rx) = mpsc::channel();
        let writer = {
            let scheduler = scheduler.clone();
            thread::spawn(move || {
                started.send(()).unwrap();
                scheduler.write(|| Ok::<_, io::Error>(())).unwrap();
                completed.send(()).unwrap();
            })
        };

        started_rx.recv().unwrap();
        assert!(completed_rx.recv_timeout(Duration::from_millis(20)).is_err());
        drop(read);
        completed_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        writer.join().unwrap();

        let stats = scheduler.stats();
        assert_eq!(stats.read_priority_waits, 1);
        assert!(stats.maximum_write_wait < priority);
    }

    #[test]
    fn zero_read_priority_bypasses_admission() {
        let scheduler = SegmentIoScheduler::new(1, Duration::ZERO).unwrap();
        scheduler
            .read(|| {
                assert_eq!(scheduler.stats().active_reads, 0);
                Ok::<_, io::Error>(())
            })
            .unwrap();
        scheduler
            .write(|| {
                assert_eq!(scheduler.stats().active_writes, 0);
                Ok::<_, io::Error>(())
            })
            .unwrap();
        assert!(!scheduler.stats().enabled());
        assert_eq!(scheduler.stats().write_operations, 0);
    }

    #[test]
    fn write_concurrency_is_bounded() {
        let scheduler = Arc::new(SegmentIoScheduler::new(1, Duration::from_secs(1)).unwrap());
        let (first_started, first_started_rx) = mpsc::channel();
        let (release_first, release_first_rx) = mpsc::channel();
        let first = {
            let scheduler = scheduler.clone();
            thread::spawn(move || {
                scheduler
                    .write(|| {
                        first_started.send(()).unwrap();
                        release_first_rx.recv().unwrap();
                        Ok::<_, io::Error>(())
                    })
                    .unwrap();
            })
        };
        first_started_rx.recv().unwrap();

        let (second_completed, second_completed_rx) = mpsc::channel();
        let second = {
            let scheduler = scheduler.clone();
            thread::spawn(move || {
                scheduler.write(|| Ok::<_, io::Error>(())).unwrap();
                second_completed.send(()).unwrap();
            })
        };
        let deadline = Instant::now() + Duration::from_secs(1);
        while scheduler.stats().waiting_writes == 0 && Instant::now() < deadline {
            thread::yield_now();
        }
        assert_eq!(scheduler.stats().waiting_writes, 1);
        assert!(second_completed_rx.recv_timeout(Duration::from_millis(20)).is_err());
        release_first.send(()).unwrap();
        second_completed_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        first.join().unwrap();
        second.join().unwrap();

        let stats = scheduler.stats();
        assert_eq!(stats.write_operations, 2);
        assert_eq!(stats.read_priority_waits, 0);
        assert_eq!(stats.write_limit_waits, 1);
    }
}
