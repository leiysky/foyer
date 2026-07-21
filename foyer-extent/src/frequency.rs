use std::sync::atomic::{AtomicU64, Ordering};

const COUNTERS_PER_ENTRY: u64 = 4;
const MAX_COUNT: u8 = 15;
const SAMPLES_PER_COUNTER: u64 = 10;

#[derive(Debug)]
pub struct FrequencySketch {
    counters: Box<[AtomicU64]>,
    samples: AtomicU64,
    epoch: AtomicU64,
    sample_window: u64,
}

impl FrequencySketch {
    pub fn new(counters: usize) -> Self {
        debug_assert!(counters.is_power_of_two());
        Self {
            counters: (0..counters)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            samples: AtomicU64::new(0),
            epoch: AtomicU64::new(0),
            sample_window: counters as u64 * SAMPLES_PER_COUNTER,
        }
    }

    pub fn record(&self, hash: u64) {
        let samples = self.samples.fetch_add(1, Ordering::Relaxed) + 1;
        if samples.is_multiple_of(self.sample_window) {
            self.epoch.fetch_add(1, Ordering::Relaxed);
        }
        let epoch = self.epoch.load(Ordering::Relaxed);
        for index in indexes(hash, self.counters.len()) {
            update(&self.counters[index], epoch);
        }
    }

    pub fn estimate(&self, hash: u64) -> u8 {
        let epoch = self.epoch.load(Ordering::Relaxed);
        indexes(hash, self.counters.len())
            .into_iter()
            .map(|index| decode(self.counters[index].load(Ordering::Relaxed), epoch))
            .min()
            .unwrap_or(0)
    }

    pub fn counters(&self) -> usize {
        self.counters.len()
    }

    pub const fn sample_window(&self) -> u64 {
        self.sample_window
    }
}

fn indexes(hash: u64, counters: usize) -> [usize; COUNTERS_PER_ENTRY as usize] {
    let mask = counters - 1;
    let mut state = hash;
    std::array::from_fn(|_| {
        state = splitmix64(state);
        state as usize & mask
    })
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn update(counter: &AtomicU64, epoch: u64) {
    let mut stored = counter.load(Ordering::Relaxed);
    loop {
        let count = decode(stored, epoch).saturating_add(1).min(MAX_COUNT);
        let next = encode(epoch, count);
        match counter.compare_exchange_weak(stored, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(actual) => stored = actual,
        }
    }
}

fn decode(stored: u64, current_epoch: u64) -> u8 {
    let stored_epoch = stored >> 8;
    let count = stored as u8;
    let age = current_epoch.saturating_sub(stored_epoch);
    if age >= u8::BITS as u64 { 0 } else { count >> age }
}

const fn encode(epoch: u64, count: u8) -> u64 {
    (epoch << 8) | count as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_frequency_and_ages_old_samples() {
        let sketch = FrequencySketch::new(16);
        let hash = 42;
        for _ in 0..8 {
            sketch.record(hash);
        }
        assert_eq!(sketch.estimate(hash), 8);

        sketch.epoch.fetch_add(1, Ordering::Relaxed);
        assert_eq!(sketch.estimate(hash), 4);
    }

    #[test]
    fn frequency_remains_valid_after_a_32_bit_epoch() {
        let sketch = FrequencySketch::new(16);
        let hash = 42;
        sketch.epoch.store(u32::MAX as u64 + 1, Ordering::Relaxed);

        sketch.record(hash);

        assert_eq!(sketch.estimate(hash), 1);
    }

    #[test]
    fn stale_frequency_expires_without_an_overshift() {
        assert_eq!(decode(encode(1, MAX_COUNT), 9), 0);
    }
}
