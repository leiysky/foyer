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
