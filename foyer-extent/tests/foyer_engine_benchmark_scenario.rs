#[path = "../benches/support/scenario.rs"]
mod scenario;

use std::time::Duration;

use scenario::{
    Permutation, bounded_log_normal_table, random_below, random_word, randomized_put_interval, should_sample,
};

#[test]
fn seeded_permutations_cover_every_position_once() {
    for len in [1, 2, 3, 10, 255, 4_096, 65_535, 65_536, 65_537] {
        for seed in [0, 1, 0x0123_4567_89ab_cdef, u64::MAX] {
            let permutation = Permutation::new(len, seed, 17);
            let mut values = (0..len).map(|position| permutation.get(position)).collect::<Vec<_>>();
            values.sort_unstable();
            assert_eq!(values, (0..len).collect::<Vec<_>>(), "len={len} seed={seed}");
        }
    }
}

#[test]
fn permutation_order_is_not_a_fixed_stride() {
    let permutation = Permutation::new(65_536, 42, 17);
    let sequence = (0..1_024).map(|position| permutation.get(position)).collect::<Vec<_>>();
    let mut deltas = sequence
        .windows(2)
        .map(|pair| pair[1].wrapping_sub(pair[0]) % 65_536)
        .collect::<Vec<_>>();
    deltas.sort_unstable();
    deltas.dedup();
    assert!(
        deltas.len() > 256,
        "permutation has only {} distinct strides",
        deltas.len()
    );
    assert_ne!(
        sequence,
        (0..1_024)
            .map(|position| Permutation::new(65_536, 43, 17).get(position))
            .collect::<Vec<_>>()
    );
}

#[test]
fn counter_streams_are_reproducible_and_independent() {
    let first = (0..1_000)
        .map(|counter| random_word(42, 1, counter))
        .collect::<Vec<_>>();
    let replay = (0..1_000)
        .map(|counter| random_word(42, 1, counter))
        .collect::<Vec<_>>();
    let other = (0..1_000)
        .map(|counter| random_word(42, 2, counter))
        .collect::<Vec<_>>();
    assert_eq!(first, replay);
    assert_ne!(first, other);
}

#[test]
fn seeded_poisson_arrivals_are_reproducible_bounded_and_centered() {
    let mean = Duration::from_micros(1_000);
    let samples = (0..100_000)
        .map(|operation| randomized_put_interval(mean, 42, 19, operation))
        .collect::<Vec<_>>();
    let replay = (0..100_000)
        .map(|operation| randomized_put_interval(mean, 42, 19, operation))
        .collect::<Vec<_>>();

    assert_eq!(samples, replay);
    assert_ne!(samples[0], randomized_put_interval(mean, 43, 19, 0));
    assert!(samples.iter().all(|sample| *sample <= mean * 8));
    let observed = samples.iter().map(Duration::as_nanos).sum::<u128>() as f64 / samples.len() as f64;
    let ratio = observed / mean.as_nanos() as f64;
    assert!((0.99..=1.01).contains(&ratio), "observed mean ratio {ratio}");
}

#[test]
fn bounded_random_stream_has_no_large_bucket_skew() {
    let mut buckets = [0_u64; 10];
    for operation in 0..100_000 {
        buckets[random_below(7, 11, operation, buckets.len() as u64) as usize] += 1;
    }
    for count in buckets {
        assert!(
            (9_500..=10_500).contains(&count),
            "bucket count {count} is unexpectedly skewed"
        );
    }
}

#[test]
fn latency_sampling_is_deterministic_and_near_its_target() {
    let selected = (0..1_000_000)
        .filter(|operation| should_sample(*operation, 1_000_000, 200_000, 9, 13))
        .collect::<Vec<_>>();
    let replay = (0..1_000_000)
        .filter(|operation| should_sample(*operation, 1_000_000, 200_000, 9, 13))
        .collect::<Vec<_>>();
    assert_eq!(selected, replay);
    assert!((198_000..=202_000).contains(&selected.len()));
}

#[test]
fn bounded_log_normal_table_has_requested_quantiles_and_cap() {
    const KIB: usize = 1024;
    let table = bounded_log_normal_table(KIB, 64 * KIB, 1024 * KIB, KIB, 65_536);

    assert!(table.windows(2).all(|pair| pair[0] <= pair[1]));
    assert_eq!(table[table.len() / 2] / KIB, 64);
    assert!((276..=284).contains(&(table[table.len() * 95 / 100] / KIB)));
    assert!((505..=525).contains(&(table[table.len() * 99 / 100] / KIB)));
    assert_eq!(table[table.len() * 999 / 1000] / KIB, 1024);
    assert_eq!(table.last().copied(), Some(1024 * KIB));
}

#[test]
fn bounded_log_normal_sampling_is_reproducible_and_seeded() {
    const KIB: usize = 1024;
    let table = bounded_log_normal_table(KIB, 64 * KIB, 1024 * KIB, KIB, 65_536);
    let sample = |seed| {
        (0..100_000)
            .map(|index| table[random_below(seed, 37, index, table.len() as u64) as usize])
            .collect::<Vec<_>>()
    };

    let first = sample(11);
    assert_eq!(first, sample(11));
    assert_ne!(first, sample(12));
    assert!(first.iter().all(|size| (KIB..=1024 * KIB).contains(size)));
}
