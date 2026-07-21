use std::time::Duration;

const GOLDEN_RATIO: u64 = 0x9e37_79b9_7f4a_7c15;

/// A reproducible, allocation-free pseudorandom permutation of `0..len`.
///
/// A keyed bijection over the next power-of-two domain is cycle-walked back into the requested
/// range. Benchmark setup can therefore randomize hundred-million-entry workloads without
/// allocating and shuffling a hundred-million-element side table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Permutation {
    len: u64,
    mask: u64,
    add1: u64,
    add2: u64,
    multiplier1: u64,
    multiplier2: u64,
    shifts: [u32; 3],
}

impl Permutation {
    pub fn new(len: u64, seed: u64, stream: u64) -> Self {
        assert!(len > 0, "permutation length must be positive");
        if len == 1 {
            return Self {
                len,
                mask: 0,
                add1: 0,
                add2: 0,
                multiplier1: 1,
                multiplier2: 1,
                shifts: [1; 3],
            };
        }
        let bits = (u64::BITS - (len - 1).leading_zeros()).max(1);
        let mask = if bits == u64::BITS {
            u64::MAX
        } else {
            (1_u64 << bits) - 1
        };
        Self {
            len,
            mask,
            add1: random_word(seed, stream ^ 0x243f_6a88_85a3_08d3, 0) & mask,
            add2: random_word(seed, stream ^ 0x1319_8a2e_0370_7344, 1) & mask,
            multiplier1: random_word(seed, stream ^ 0xa409_3822_299f_31d0, 2) | 1,
            multiplier2: random_word(seed, stream ^ 0x082e_fa98_ec4e_6c89, 3) | 1,
            shifts: [(bits / 2).max(1), (bits / 3).max(1), (bits * 2 / 3).max(1)],
        }
    }

    pub fn get(self, position: u64) -> u64 {
        if self.len == 1 {
            return 0;
        }
        let mut value = position % self.len;
        loop {
            value = self.permute_domain(value);
            if value < self.len {
                return value;
            }
        }
    }

    fn permute_domain(self, mut value: u64) -> u64 {
        value = value.wrapping_add(self.add1) & self.mask;
        value ^= value >> self.shifts[0];
        value = value.wrapping_mul(self.multiplier1) & self.mask;
        value ^= (value << self.shifts[1]) & self.mask;
        value &= self.mask;
        value = value.wrapping_add(self.add2) & self.mask;
        value ^= value >> self.shifts[2];
        value = value.wrapping_mul(self.multiplier2) & self.mask;
        value ^= value >> self.shifts[1];
        value & self.mask
    }
}

/// Return one deterministic random word from an independent counter-based stream.
pub const fn random_word(seed: u64, stream: u64, counter: u64) -> u64 {
    mix64(seed ^ mix64(stream) ^ mix64(counter))
}

/// Map one deterministic random word into `0..upper` without modulo's short-period low bits.
pub fn random_below(seed: u64, stream: u64, counter: u64, upper: u64) -> u64 {
    assert!(upper > 0, "random upper bound must be positive");
    ((u128::from(random_word(seed, stream, counter)) * u128::from(upper)) >> 64) as u64
}

/// Sample one deterministic exponential inter-arrival and cap it at eight times the mean.
pub fn randomized_put_interval(mean: Duration, seed: u64, stream: u64, operation: u64) -> Duration {
    assert!(!mean.is_zero(), "put arrival mean must be positive");
    // Use the high 53 random bits to construct an open-interval uniform variate. Exponential
    // inter-arrivals form a Poisson process; the 8x cap bounds a single test pause while retaining
    // more than 99.9% of the unbounded distribution.
    let mantissa = random_word(seed, stream, operation) >> 11;
    let unit = (mantissa as f64 + 0.5) / (1_u64 << 53) as f64;
    let multiple = (-(1.0 - unit).ln()).min(8.0);
    Duration::from_secs_f64(mean.as_secs_f64() * multiple)
}

/// Build a fixed quantile table for a positive, bounded log-normal size distribution.
///
/// `median` is p50. `p999_maximum` is both the unbounded distribution's p99.9 and the hard cap.
/// Values are rounded to `quantum`, which keeps the hot-path lookup deterministic and avoids
/// architecture-specific floating-point work while the benchmark is running.
pub fn bounded_log_normal_table(
    minimum: usize,
    median: usize,
    p999_maximum: usize,
    quantum: usize,
    buckets: usize,
) -> Vec<usize> {
    assert!(minimum > 0, "minimum must be positive");
    assert!(minimum <= median, "minimum must not exceed median");
    assert!(median < p999_maximum, "median must be below p99.9 maximum");
    assert!(quantum > 0, "quantum must be positive");
    assert!(buckets > 0, "quantile table must not be empty");

    let p999_z = inverse_standard_normal(0.999);
    let sigma = ((p999_maximum as f64) / (median as f64)).ln() / p999_z;
    (0..buckets)
        .map(|bucket| {
            let probability = (bucket as f64 + 0.5) / buckets as f64;
            let value = median as f64 * (sigma * inverse_standard_normal(probability)).exp();
            let rounded = ((value / quantum as f64).round() as usize).saturating_mul(quantum);
            rounded.clamp(minimum, p999_maximum)
        })
        .collect()
}

// Peter J. Acklam's rational approximation. The benchmark quantizes its result before use, so the
// approximation is only setup work and never appears in the measured storage hot path.
fn inverse_standard_normal(probability: f64) -> f64 {
    assert!((0.0..1.0).contains(&probability));

    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];
    const LOWER: f64 = 0.024_25;
    const UPPER: f64 = 1.0 - LOWER;

    if probability < LOWER {
        let q = (-2.0 * probability.ln()).sqrt();
        return (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0);
    }
    if probability > UPPER {
        let q = (-2.0 * (1.0 - probability).ln()).sqrt();
        return -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0);
    }

    let q = probability - 0.5;
    let r = q * q;
    (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
        / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
}

/// Select an unbiased-in-practice latency sample without coupling sampling to periodic key patterns.
pub fn should_sample(operation: u64, operations: u64, target: u64, seed: u64, stream: u64) -> bool {
    operations <= target || random_below(seed, stream, operation, operations) < target
}

pub const fn mix64(mut value: u64) -> u64 {
    value = value.wrapping_add(GOLDEN_RATIO);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
