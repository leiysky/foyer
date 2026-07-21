#[cfg(target_os = "linux")]
use std::fs::OpenOptions;
use std::{
    env,
    io::Write,
    path::{Path, PathBuf},
    sync::Barrier,
    time::{Duration, Instant},
};

use fixed_lsm::{FixedLsm, FixedLsmOptions, FixedLsmStats, WriteBatch, WriteOptions};

const DEFAULT_CACHE_MIB: usize = 512;
const DEFAULT_WRITE_BUFFER_MIB: usize = 64;
const DEFAULT_BATCH_ENTRIES: usize = 16_128;
const DEFAULT_SAMPLE_INTERVAL: usize = 64;
const PERMUTATION_MULTIPLIER: u64 = 6_364_136_223_846_793_013;
const PERMUTATION_OFFSET: u64 = 1_442_695_040_888_963_407;

#[derive(Debug, Clone, Copy)]
struct BenchmarkConfig {
    cache_mib: usize,
    write_buffer_mib: usize,
}

impl BenchmarkConfig {
    fn from_env() -> Self {
        Self {
            cache_mib: env_usize("FIXED_LSM_BENCH_CACHE_MIB", DEFAULT_CACHE_MIB),
            write_buffer_mib: env_usize("FIXED_LSM_BENCH_WRITE_BUFFER_MIB", DEFAULT_WRITE_BUFFER_MIB),
        }
    }

    fn options(self) -> FixedLsmOptions {
        FixedLsmOptions {
            cache_capacity: self.cache_mib * 1024 * 1024,
            write_buffer_capacity: self.write_buffer_mib * 1024 * 1024,
            ..FixedLsmOptions::default()
        }
    }
}

fn scale_key(index: u64) -> [u8; 24] {
    let mut key = [0; 24];
    key[..8].copy_from_slice(&index.to_be_bytes());
    key[8..16].copy_from_slice(&(!index).to_be_bytes());
    key
}

fn scale_value(index: u64, generation: u32) -> [u8; 32] {
    let mut value = [0; 32];
    value[..4].copy_from_slice(b"SCLO");
    value[4] = 1;
    value[5] = (index % 3) as u8;
    value[8..16].copy_from_slice(&index.to_le_bytes());
    value[16..20].copy_from_slice(&generation.to_le_bytes());
    value[20..24].copy_from_slice(&(16_u32 * 1024).to_le_bytes());
    value[24..28].copy_from_slice(&(index as u32).rotate_left(generation % 31).to_le_bytes());
    let checksum = crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32Iscsi, &value[..28]) as u32;
    value[28..].copy_from_slice(&checksum.to_le_bytes());
    value
}

fn validate_value(value: &[u8; 32], index: u64, generation: u32) {
    assert_eq!(value, &scale_value(index, generation));
}

fn lookup(db: &FixedLsm, index: u64) -> [u8; 32] {
    db.get(&scale_key(index)).unwrap().expect("benchmark key is missing")
}

fn permuted_index(operation: u64, blobs: u64) -> u64 {
    u64::try_from(
        (u128::from(operation) * u128::from(PERMUTATION_MULTIPLIER) + u128::from(PERMUTATION_OFFSET))
            % u128::from(blobs),
    )
    .unwrap()
}

fn mixed_key_id(operation: u64, salt: u64, blobs: u64, hot_access_percent: usize, hotset_percent: usize) -> u64 {
    if hot_access_percent == 0 {
        return permuted_index(operation.wrapping_add(salt), blobs);
    }
    let hot_blobs = (blobs * hotset_percent as u64 / 100).max(1);
    let draw = permuted_index(operation.wrapping_add(salt.rotate_left(17)), 100);
    let domain = if draw < hot_access_percent as u64 {
        hot_blobs
    } else {
        blobs
    };
    permuted_index(operation.wrapping_add(salt), domain)
}

fn generate(path: &Path, blobs: u64, config: BenchmarkConfig) {
    if path.exists() {
        assert!(env_bool("FIXED_LSM_BENCH_REGENERATE", false));
        std::fs::remove_dir_all(path).unwrap();
    }
    let batch_entries = env_usize("FIXED_LSM_BENCH_BATCH_ENTRIES", DEFAULT_BATCH_ENTRIES);
    assert!(batch_entries > 0);
    let read_before = process_field("/proc/self/io", "read_bytes:");
    let write_before = process_field("/proc/self/io", "write_bytes:");
    let started = Instant::now();
    let db = FixedLsm::create(path, config.options()).unwrap();
    let mut first = 0_u64;
    let mut next_progress = 5_000_000_u64.min(blobs);
    while first < blobs {
        let end = (first + batch_entries as u64).min(blobs);
        let mut batch = WriteBatch::with_capacity((end - first) as usize);
        for index in first..end {
            batch.put(scale_key(index), scale_value(index, 1));
        }
        db.write(&batch, WriteOptions::bulk_load()).unwrap();
        first = end;
        if first >= next_progress {
            println!(
                "{{\"phase\":\"fixed_lsm_generate_progress\",\"blobs\":{first},\"total_blobs\":{blobs},\"elapsed_seconds\":{:.3}}}",
                started.elapsed().as_secs_f64(),
            );
            std::io::stdout().flush().unwrap();
            next_progress = next_progress.saturating_add(5_000_000).min(blobs);
        }
    }
    let publication = started.elapsed();
    let drain_started = Instant::now();
    db.flush().unwrap();
    let drain = drain_started.elapsed();
    for index in [0, 1.min(blobs - 1), blobs / 2, blobs - 1] {
        validate_value(&lookup(&db, index), index, 1);
    }
    let stats = db.stats();
    println!(
        "{{\"phase\":\"fixed_lsm_generate_result\",\"blobs\":{blobs},\"batch_entries\":{batch_entries},\"publication_ms\":{:.3},\"publication_mops\":{:.3},\"drain_ms\":{:.3},\"total_ms\":{:.3},\"disk_bytes\":{},\"level_files\":{},\"level_bytes\":{},\"level_tombstones\":{},\"base_level\":{},\"level_targets\":{},\"maintenance\":{},\"cache_usage_bytes\":{},\"table_read_bytes\":{},\"table_write_bytes\":{},\"read_bytes\":{},\"write_bytes\":{}}}",
        publication.as_secs_f64() * 1_000.0,
        blobs as f64 / publication.as_secs_f64() / 1_000_000.0,
        drain.as_secs_f64() * 1_000.0,
        started.elapsed().as_secs_f64() * 1_000.0,
        directory_bytes(path),
        array_json(&stats.level_files),
        array_json(&stats.level_bytes),
        array_json(&stats.level_tombstones),
        stats.base_level,
        array_json(&stats.level_targets),
        maintenance_delta_json(&FixedLsmStats::default(), &stats),
        stats.cache_resident_bytes,
        stats.table_read_bytes,
        stats.table_write_bytes,
        optional_number(io_difference(read_before, "/proc/self/io", "read_bytes:")),
        optional_number(io_difference(write_before, "/proc/self/io", "write_bytes:")),
    );
}

fn recover(path: &Path, blobs: u64, config: BenchmarkConfig) {
    let cold = env_bool("FIXED_LSM_BENCH_COLD", true);
    let validate_base_keys = env_bool("FIXED_LSM_BENCH_VALIDATE_BASE_KEYS", true);
    let lookups = env_usize("FIXED_LSM_BENCH_LOOKUPS", 0);
    let minimum_concurrency = minimum_concurrency();
    let concurrency = env_usize("FIXED_LSM_BENCH_CONCURRENCY", minimum_concurrency);
    let hot_keys = env_usize("FIXED_LSM_BENCH_HOT_KEYS", 258_048).min(blobs as usize);
    assert!(lookups == 0 || concurrency >= minimum_concurrency);
    assert!(!cold || evict_directory(path));
    let read_before = process_field("/proc/self/io", "read_bytes:");
    let rss_before = process_field("/proc/self/status", "VmRSS:");
    let started = Instant::now();
    let db = FixedLsm::open(path, config.options()).unwrap();
    let recovery = started.elapsed();
    let recovery_read_bytes = io_difference(read_before, "/proc/self/io", "read_bytes:");
    if validate_base_keys {
        for index in [0, 1.min(blobs - 1), blobs / 2, blobs - 1] {
            assert_eq!(u64::from_le_bytes(lookup(&db, index)[8..16].try_into().unwrap()), index);
        }
    }
    let stats = db.stats();
    println!(
        "{{\"phase\":\"fixed_lsm_recovery_result\",\"blobs\":{blobs},\"cold\":{cold},\"validate_base_keys\":{validate_base_keys},\"recovery_ms\":{:.3},\"read_bytes\":{},\"rss_before_kib\":{},\"rss_after_kib\":{},\"peak_rss_kib\":{},\"recovered_records\":{},\"discarded_wal_tail_bytes\":{},\"level_files\":{},\"level_bytes\":{},\"level_tombstones\":{},\"base_level\":{},\"level_targets\":{},\"cache_usage_bytes\":{},\"table_read_bytes\":{},\"disk_bytes\":{}}}",
        recovery.as_secs_f64() * 1_000.0,
        optional_number(recovery_read_bytes),
        optional_number(rss_before),
        optional_number(process_field("/proc/self/status", "VmRSS:")),
        optional_number(process_field("/proc/self/status", "VmHWM:")),
        stats.recovered_records,
        stats.discarded_wal_tail_bytes,
        array_json(&stats.level_files),
        array_json(&stats.level_bytes),
        array_json(&stats.level_tombstones),
        stats.base_level,
        array_json(&stats.level_targets),
        stats.cache_resident_bytes,
        stats.table_read_bytes,
        directory_bytes(path),
    );
    if lookups > 0 {
        assert!(hot_keys > 0);
        for index in 0..hot_keys as u64 {
            std::hint::black_box(lookup(&db, index));
        }
        let cache_before = db.stats();
        let read_before = process_field("/proc/self/io", "read_bytes:");
        let barrier = Barrier::new(concurrency + 1);
        let (elapsed, mut samples) = std::thread::scope(|scope| {
            let mut workers = Vec::with_capacity(concurrency);
            for worker in 0..concurrency {
                let barrier = &barrier;
                let db = &db;
                workers.push(scope.spawn(move || {
                    let first = lookups * worker / concurrency;
                    let end = lookups * (worker + 1) / concurrency;
                    let mut state = 0x243f_6a88_85a3_08d3_u64 ^ (worker as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
                    let mut samples = Vec::with_capacity((end - first).div_ceil(DEFAULT_SAMPLE_INTERVAL));
                    barrier.wait();
                    for operation in first..end {
                        state = state
                            .wrapping_mul(PERMUTATION_MULTIPLIER)
                            .wrapping_add(PERMUTATION_OFFSET);
                        let index = state % hot_keys as u64;
                        let sampled = operation.is_multiple_of(DEFAULT_SAMPLE_INTERVAL).then(Instant::now);
                        let value = lookup(db, index);
                        assert_eq!(u64::from_le_bytes(value[8..16].try_into().unwrap()), index);
                        std::hint::black_box(value);
                        if let Some(started) = sampled {
                            samples.push(started.elapsed());
                        }
                    }
                    samples
                }));
            }
            let started = Instant::now();
            barrier.wait();
            let mut samples = Vec::with_capacity(lookups.div_ceil(DEFAULT_SAMPLE_INTERVAL));
            for worker in workers {
                samples.extend(worker.join().unwrap());
            }
            (started.elapsed(), samples)
        });
        samples.sort_unstable();
        let cache_after = db.stats();
        println!(
            "{{\"phase\":\"fixed_lsm_concurrent_lookup_result\",\"lookups\":{lookups},\"concurrency\":{concurrency},\"hot_keys\":{hot_keys},\"elapsed_ms\":{:.3},\"throughput_mops\":{:.3},\"latency\":{},\"read_bytes\":{},\"cache_hits\":{},\"cache_misses\":{},\"cache_usage_bytes\":{},\"cache_data_bytes\":{},\"cache_metadata_bytes\":{}}}",
            elapsed.as_secs_f64() * 1_000.0,
            lookups as f64 / elapsed.as_secs_f64() / 1_000_000.0,
            latency_json(samples),
            optional_number(io_difference(read_before, "/proc/self/io", "read_bytes:")),
            cache_after.cache_hits.saturating_sub(cache_before.cache_hits),
            cache_after.cache_misses.saturating_sub(cache_before.cache_misses),
            cache_after.cache_resident_bytes,
            cache_after.cache_data_resident_bytes,
            cache_after.cache_metadata_resident_bytes,
        );
    }
}

struct MixedResult {
    elapsed: Duration,
    reads: Vec<Duration>,
    writes: Vec<Duration>,
    workload_read_bytes: Option<u64>,
    workload_write_bytes: Option<u64>,
}

fn mixed(path: &Path, blobs: u64, config: BenchmarkConfig) {
    let operations = env_usize("FIXED_LSM_BENCH_MIXED_OPERATIONS", 8_000_000);
    let write_percent = env_usize("FIXED_LSM_BENCH_MIXED_WRITE_PERCENT", 50);
    let hot_access_percent = env_usize("FIXED_LSM_BENCH_HOT_ACCESS_PERCENT", 0);
    let hotset_percent = env_usize("FIXED_LSM_BENCH_HOTSET_PERCENT", 1);
    let minimum_concurrency = minimum_concurrency();
    let concurrency = env_usize("FIXED_LSM_BENCH_CONCURRENCY", minimum_concurrency);
    let cold = env_bool("FIXED_LSM_BENCH_COLD", true);
    let sync_after = env_bool("FIXED_LSM_BENCH_SYNC_AFTER", true);
    let drain_after = env_bool("FIXED_LSM_BENCH_DRAIN_AFTER", true);
    assert!(concurrency >= minimum_concurrency);
    assert!(write_percent > 0 && write_percent < 100);
    assert!(hot_access_percent <= 100 && (1..=100).contains(&hotset_percent));
    assert!(!cold || evict_directory(path));
    let rss_before = process_field("/proc/self/status", "VmRSS:");
    let db = FixedLsm::open(path, config.options()).unwrap();
    let rss_after_open = process_field("/proc/self/status", "VmRSS:");
    let before = db.stats();
    let total_read_before = process_field("/proc/self/io", "read_bytes:");
    let total_write_before = process_field("/proc/self/io", "write_bytes:");
    let result = concurrent_mixed_access(
        &db,
        blobs,
        concurrency,
        operations,
        write_percent,
        hot_access_percent,
        hotset_percent,
    );
    let sync_started = Instant::now();
    if sync_after {
        db.sync_wal().unwrap();
    }
    let sync = sync_started.elapsed();
    let drain_started = Instant::now();
    if drain_after {
        db.flush().unwrap();
    }
    let drain = drain_started.elapsed();
    let after = db.stats();
    println!(
        "{{\"phase\":\"fixed_lsm_mixed_result\",\"blobs\":{blobs},\"operations\":{operations},\"concurrency\":{concurrency},\"writer_threads\":1,\"reader_threads\":{},\"write_percent\":{write_percent},\"hot_access_percent\":{hot_access_percent},\"hotset_percent\":{hotset_percent},\"elapsed_ms\":{:.3},\"throughput_mops\":{:.3},\"read_latency\":{},\"write_latency\":{},\"sync_after\":{sync_after},\"sync_ms\":{:.3},\"drain_after\":{drain_after},\"drain_ms\":{:.3},\"rss_before_kib\":{},\"rss_after_open_kib\":{},\"rss_after_kib\":{},\"peak_rss_kib\":{},\"workload_read_bytes\":{},\"workload_write_bytes\":{},\"total_read_bytes\":{},\"total_write_bytes\":{},\"cache_hits\":{},\"cache_misses\":{},\"internal_read_bytes\":{},\"point_reads\":{},\"internal_write_bytes\":{},\"maintenance\":{},\"wal_bytes\":{},\"disk_bytes\":{},\"level_files\":{},\"level_bytes\":{},\"level_tombstones\":{},\"base_level\":{},\"level_targets\":{},\"cache_usage_bytes\":{},\"cache_data_bytes\":{},\"cache_metadata_bytes\":{}}}",
        concurrency - 1,
        result.elapsed.as_secs_f64() * 1_000.0,
        operations as f64 / result.elapsed.as_secs_f64() / 1_000_000.0,
        latency_json(result.reads),
        latency_json(result.writes),
        sync.as_secs_f64() * 1_000.0,
        drain.as_secs_f64() * 1_000.0,
        optional_number(rss_before),
        optional_number(rss_after_open),
        optional_number(process_field("/proc/self/status", "VmRSS:")),
        optional_number(process_field("/proc/self/status", "VmHWM:")),
        optional_number(result.workload_read_bytes),
        optional_number(result.workload_write_bytes),
        optional_number(io_difference(total_read_before, "/proc/self/io", "read_bytes:")),
        optional_number(io_difference(total_write_before, "/proc/self/io", "write_bytes:")),
        after.cache_hits.saturating_sub(before.cache_hits),
        after.cache_misses.saturating_sub(before.cache_misses),
        after.table_read_bytes.saturating_sub(before.table_read_bytes),
        point_read_delta_json(&before, &after),
        after.table_write_bytes.saturating_sub(before.table_write_bytes),
        maintenance_delta_json(&before, &after),
        after.wal_bytes,
        directory_bytes(path),
        array_json(&after.level_files),
        array_json(&after.level_bytes),
        array_json(&after.level_tombstones),
        after.base_level,
        array_json(&after.level_targets),
        after.cache_resident_bytes,
        after.cache_data_resident_bytes,
        after.cache_metadata_resident_bytes,
    );
}

fn concurrent_mixed_access(
    db: &FixedLsm,
    blobs: u64,
    concurrency: usize,
    operations: usize,
    write_percent: usize,
    hot_access_percent: usize,
    hotset_percent: usize,
) -> MixedResult {
    assert!(operations >= concurrency * DEFAULT_SAMPLE_INTERVAL);
    let write_operations = operations * write_percent / 100;
    let read_operations = operations - write_operations;
    let reader_threads = concurrency - 1;
    let barrier = Barrier::new(concurrency + 1);
    let read_before = process_field("/proc/self/io", "read_bytes:");
    let write_before = process_field("/proc/self/io", "write_bytes:");
    let (elapsed, mut reads, mut writes) = std::thread::scope(|scope| {
        let worker_barrier = &barrier;
        let writer = scope.spawn(move || {
            let mut samples = Vec::with_capacity(write_operations.div_ceil(DEFAULT_SAMPLE_INTERVAL));
            worker_barrier.wait();
            for operation in 0..write_operations {
                let index = mixed_key_id(operation as u64, 0, blobs, hot_access_percent, hotset_percent);
                let sampled = operation.is_multiple_of(DEFAULT_SAMPLE_INTERVAL).then(Instant::now);
                db.put(scale_key(index), scale_value(index, 2), WriteOptions::buffered())
                    .unwrap();
                if let Some(started) = sampled {
                    samples.push(started.elapsed());
                }
            }
            samples
        });
        let mut readers = Vec::with_capacity(reader_threads);
        for reader in 0..reader_threads {
            let worker_barrier = &barrier;
            readers.push(scope.spawn(move || {
                let first = read_operations * reader / reader_threads;
                let end = read_operations * (reader + 1) / reader_threads;
                let mut samples = Vec::with_capacity((end - first).div_ceil(DEFAULT_SAMPLE_INTERVAL));
                worker_barrier.wait();
                for operation in first..end {
                    let index = mixed_key_id(
                        operation as u64,
                        PERMUTATION_OFFSET.wrapping_add(reader as u64),
                        blobs,
                        hot_access_percent,
                        hotset_percent,
                    );
                    let sampled = operation.is_multiple_of(DEFAULT_SAMPLE_INTERVAL).then(Instant::now);
                    let value = lookup(db, index);
                    assert_eq!(u64::from_le_bytes(value[8..16].try_into().unwrap()), index);
                    std::hint::black_box(value);
                    if let Some(started) = sampled {
                        samples.push(started.elapsed());
                    }
                }
                samples
            }));
        }
        let started = Instant::now();
        barrier.wait();
        let writes = writer.join().unwrap();
        let mut reads = Vec::with_capacity(read_operations.div_ceil(DEFAULT_SAMPLE_INTERVAL));
        for reader in readers {
            reads.extend(reader.join().unwrap());
        }
        (started.elapsed(), reads, writes)
    });
    reads.sort_unstable();
    writes.sort_unstable();
    MixedResult {
        elapsed,
        reads,
        writes,
        workload_read_bytes: io_difference(read_before, "/proc/self/io", "read_bytes:"),
        workload_write_bytes: io_difference(write_before, "/proc/self/io", "write_bytes:"),
    }
}

fn churn(path: &Path, blobs: u64, config: BenchmarkConfig) {
    let updates = env_u64("FIXED_LSM_BENCH_UPDATES");
    let generation = env_optional_u64("FIXED_LSM_BENCH_GENERATION", 2) as u32;
    let batch_entries = env_usize("FIXED_LSM_BENCH_BATCH_ENTRIES", DEFAULT_BATCH_ENTRIES);
    let drain = env_bool("FIXED_LSM_BENCH_DRAIN", true);
    let force_compact = env_bool("FIXED_LSM_BENCH_FORCE_COMPACT", false);
    let replace = env_bool("FIXED_LSM_BENCH_REPLACE", false);
    assert!(updates > 0 && updates <= blobs && batch_entries > 0);
    let cohort_start = if replace {
        u64::from(generation.checked_sub(2).expect("replacement generation starts at 2"))
            .checked_mul(updates)
            .expect("replacement cohort offset overflows")
    } else {
        0
    };
    assert!(!replace || cohort_start <= blobs - updates);
    let mutations = updates * if replace { 2 } else { 1 };
    let db = FixedLsm::open(path, config.options()).unwrap();
    let stats_before = db.stats();
    let rss_before = process_field("/proc/self/status", "VmRSS:");
    let disk_before = directory_bytes(path);
    let read_before = process_field("/proc/self/io", "read_bytes:");
    let write_before = process_field("/proc/self/io", "write_bytes:");
    let publication_started = Instant::now();
    let mut commits = Vec::new();
    let mut first = 0_u64;
    while first < updates {
        let end = (first + batch_entries as u64).min(updates);
        let started = Instant::now();
        let mut batch = WriteBatch::with_capacity((end - first) as usize * if replace { 2 } else { 1 });
        for operation in first..end {
            let index = permuted_index(cohort_start + operation, blobs);
            if replace {
                batch.delete(scale_key(index));
                let replacement = blobs + index;
                batch.put(scale_key(replacement), scale_value(replacement, generation));
            } else {
                batch.put(scale_key(index), scale_value(index, generation));
            }
        }
        db.write(&batch, WriteOptions::buffered()).unwrap();
        commits.push(started.elapsed());
        first = end;
    }
    let publication = publication_started.elapsed();
    let persist_started = Instant::now();
    db.sync_wal().unwrap();
    let persist = persist_started.elapsed();
    let drain_started = Instant::now();
    if drain {
        db.flush().unwrap();
    }
    if force_compact {
        db.compact().unwrap();
    }
    let drain_elapsed = drain_started.elapsed();
    for operation in [0, updates / 2, updates - 1] {
        let index = permuted_index(cohort_start + operation, blobs);
        if replace {
            assert_eq!(db.get(&scale_key(index)).unwrap(), None);
            let replacement = blobs + index;
            validate_value(&lookup(&db, replacement), replacement, generation);
        } else {
            validate_value(&lookup(&db, index), index, generation);
        }
    }
    let stats_after = db.stats();
    let write_bytes = io_difference(write_before, "/proc/self/io", "write_bytes:");
    println!(
        "{{\"phase\":\"fixed_lsm_churn_result\",\"blobs\":{blobs},\"updates\":{updates},\"mutations\":{mutations},\"generation\":{generation},\"replace\":{replace},\"batch_entries\":{batch_entries},\"drain\":{drain},\"force_compact\":{force_compact},\"publication_ms\":{:.3},\"publication_mops\":{:.3},\"publication_mutation_mops\":{:.3},\"commit_latency\":{},\"persist_ms\":{:.3},\"drain_ms\":{:.3},\"rss_before_kib\":{},\"rss_after_kib\":{},\"peak_rss_kib\":{},\"disk_bytes_before\":{disk_before},\"disk_bytes_after\":{},\"read_bytes\":{},\"write_bytes\":{},\"bytes_per_update\":{},\"bytes_per_mutation\":{},\"internal_read_bytes\":{},\"internal_write_bytes\":{},\"maintenance\":{},\"level_files\":{},\"level_bytes\":{},\"level_tombstones\":{},\"base_level\":{},\"level_targets\":{},\"cache_usage_bytes\":{}}}",
        publication.as_secs_f64() * 1_000.0,
        updates as f64 / publication.as_secs_f64() / 1_000_000.0,
        mutations as f64 / publication.as_secs_f64() / 1_000_000.0,
        latency_json(commits),
        persist.as_secs_f64() * 1_000.0,
        drain_elapsed.as_secs_f64() * 1_000.0,
        optional_number(rss_before),
        optional_number(process_field("/proc/self/status", "VmRSS:")),
        optional_number(process_field("/proc/self/status", "VmHWM:")),
        directory_bytes(path),
        optional_number(io_difference(read_before, "/proc/self/io", "read_bytes:")),
        optional_number(write_bytes),
        write_bytes.map_or_else(
            || "null".to_string(),
            |bytes| format!("{:.3}", bytes as f64 / updates as f64),
        ),
        write_bytes.map_or_else(
            || "null".to_string(),
            |bytes| format!("{:.3}", bytes as f64 / mutations as f64),
        ),
        stats_after
            .table_read_bytes
            .saturating_sub(stats_before.table_read_bytes),
        stats_after
            .table_write_bytes
            .saturating_sub(stats_before.table_write_bytes),
        maintenance_delta_json(&stats_before, &stats_after),
        array_json(&stats_after.level_files),
        array_json(&stats_after.level_bytes),
        array_json(&stats_after.level_tombstones),
        stats_after.base_level,
        array_json(&stats_after.level_targets),
        stats_after.cache_resident_bytes,
    );
}

fn dirty_tail(path: &Path, blobs: u64, config: BenchmarkConfig) -> ! {
    let updates = env_u64("FIXED_LSM_BENCH_UPDATES");
    let generation = env_optional_u64("FIXED_LSM_BENCH_GENERATION", 2) as u32;
    let batch_entries = env_usize("FIXED_LSM_BENCH_BATCH_ENTRIES", DEFAULT_BATCH_ENTRIES);
    assert!(updates > 0 && updates <= blobs && batch_entries > 0);

    let db = FixedLsm::open(path, config.options()).unwrap();
    let started = Instant::now();
    let mut first = 0_u64;
    while first < updates {
        let end = (first + batch_entries as u64).min(updates);
        let mut batch = WriteBatch::with_capacity((end - first) as usize);
        for operation in first..end {
            let index = permuted_index(operation, blobs);
            batch.put(scale_key(index), scale_value(index, generation));
        }
        db.write(&batch, WriteOptions::buffered()).unwrap();
        first = end;
    }
    let publication = started.elapsed();
    let persist_started = Instant::now();
    db.sync_wal().unwrap();
    let persist = persist_started.elapsed();
    for operation in [0, updates / 2, updates - 1] {
        let index = permuted_index(operation, blobs);
        validate_value(&lookup(&db, index), index, generation);
    }
    let stats = db.stats();
    println!(
        "{{\"phase\":\"fixed_lsm_dirty_tail_result\",\"blobs\":{blobs},\"updates\":{updates},\"generation\":{generation},\"batch_entries\":{batch_entries},\"publication_ms\":{:.3},\"publication_mops\":{:.3},\"persist_ms\":{:.3},\"mutable_entries\":{},\"immutable_memtables\":{},\"background_running\":{},\"wal_bytes\":{},\"rss_kib\":{},\"peak_rss_kib\":{}}}",
        publication.as_secs_f64() * 1_000.0,
        updates as f64 / publication.as_secs_f64() / 1_000_000.0,
        persist.as_secs_f64() * 1_000.0,
        stats.mutable_entries,
        stats.immutable_memtables,
        stats.background_running,
        stats.wal_bytes,
        optional_number(process_field("/proc/self/status", "VmRSS:")),
        optional_number(process_field("/proc/self/status", "VmHWM:")),
    );
    std::io::stdout().flush().unwrap();

    // Model a process crash immediately after the durability acknowledgement. In particular, do
    // not run `FixedLsm::drop`, because it waits for an in-flight background flush.
    std::process::exit(0)
}

fn checkpoint(path: &Path, config: BenchmarkConfig) {
    let force_compact = env_bool("FIXED_LSM_BENCH_FORCE_COMPACT", false);
    let read_before = process_field("/proc/self/io", "read_bytes:");
    let write_before = process_field("/proc/self/io", "write_bytes:");
    let db = FixedLsm::open(path, config.options()).unwrap();
    let started = Instant::now();
    db.flush().unwrap();
    if force_compact {
        db.compact().unwrap();
    }
    let elapsed = started.elapsed();
    let stats = db.stats();
    println!(
        "{{\"phase\":\"fixed_lsm_checkpoint_result\",\"force_compact\":{force_compact},\"elapsed_ms\":{:.3},\"read_bytes\":{},\"write_bytes\":{},\"disk_bytes\":{},\"level_files\":{},\"level_bytes\":{},\"level_tombstones\":{},\"base_level\":{},\"level_targets\":{},\"maintenance\":{}}}",
        elapsed.as_secs_f64() * 1_000.0,
        optional_number(io_difference(read_before, "/proc/self/io", "read_bytes:")),
        optional_number(io_difference(write_before, "/proc/self/io", "write_bytes:")),
        directory_bytes(path),
        array_json(&stats.level_files),
        array_json(&stats.level_bytes),
        array_json(&stats.level_tombstones),
        stats.base_level,
        array_json(&stats.level_targets),
        maintenance_delta_json(&FixedLsmStats::default(), &stats),
    );
}

fn minimum_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1)
        .saturating_mul(2)
}

fn env_u64(name: &str) -> u64 {
    env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be set"))
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be an integer"))
}

fn env_optional_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .map(|value| value.parse().unwrap_or_else(|_| panic!("{name} must be an integer")))
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    usize::try_from(env_optional_u64(name, default as u64)).unwrap()
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|value| match value.as_str() {
            "1" | "true" => true,
            "0" | "false" => false,
            _ => panic!("{name} must be 0, 1, false, or true"),
        })
        .unwrap_or(default)
}

fn percentile(samples: &[Duration], per_mille: usize) -> Duration {
    assert!(!samples.is_empty());
    samples[(samples.len() - 1) * per_mille / 1_000]
}

fn latency_json(mut samples: Vec<Duration>) -> String {
    samples.sort_unstable();
    format!(
        "{{\"samples\":{},\"p50_ns\":{},\"p95_ns\":{},\"p99_ns\":{},\"p99_9_ns\":{},\"max_ns\":{}}}",
        samples.len(),
        percentile(&samples, 500).as_nanos(),
        percentile(&samples, 950).as_nanos(),
        percentile(&samples, 990).as_nanos(),
        percentile(&samples, 999).as_nanos(),
        samples.last().unwrap().as_nanos(),
    )
}

fn array_json<const N: usize>(values: &[u64; N]) -> String {
    format!("[{}]", values.iter().map(u64::to_string).collect::<Vec<_>>().join(","))
}

fn maintenance_delta_json(before: &FixedLsmStats, after: &FixedLsmStats) -> String {
    format!(
        "{{\"flush_operations\":{},\"flush_output_bytes\":{},\"compaction_operations\":{},\"compaction_input_bytes\":{},\"compaction_output_bytes\":{},\"trivial_move_operations\":{}}}",
        after.flush_operations.saturating_sub(before.flush_operations),
        after.flush_output_bytes.saturating_sub(before.flush_output_bytes),
        after.compaction_operations.saturating_sub(before.compaction_operations),
        after
            .compaction_input_bytes
            .saturating_sub(before.compaction_input_bytes),
        after
            .compaction_output_bytes
            .saturating_sub(before.compaction_output_bytes),
        after
            .trivial_move_operations
            .saturating_sub(before.trivial_move_operations),
    )
}

fn point_read_delta_json(before: &FixedLsmStats, after: &FixedLsmStats) -> String {
    format!(
        "{{\"filter_checks\":{},\"filter_positives\":{},\"data_cache_hits\":{},\"data_reads\":{},\"false_positives\":{}}}",
        after.point_filter_checks.saturating_sub(before.point_filter_checks),
        after
            .point_filter_positives
            .saturating_sub(before.point_filter_positives),
        after.point_data_cache_hits.saturating_sub(before.point_data_cache_hits),
        after.point_data_reads.saturating_sub(before.point_data_reads),
        after.point_false_positives.saturating_sub(before.point_false_positives),
    )
}

fn optional_number(value: Option<u64>) -> String {
    value.map_or_else(|| "null".to_string(), |value| value.to_string())
}

#[cfg(target_os = "linux")]
fn process_field(path: &str, field: &str) -> Option<u64> {
    std::fs::read_to_string(path)
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix(field)?.split_whitespace().next()?.parse().ok())
}

#[cfg(not(target_os = "linux"))]
fn process_field(_path: &str, _field: &str) -> Option<u64> {
    None
}

fn io_difference(before: Option<u64>, path: &str, field: &str) -> Option<u64> {
    before
        .zip(process_field(path, field))
        .map(|(before, after)| after.saturating_sub(before))
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(metadata) = std::fs::metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| directory_bytes(&entry.unwrap().path()))
        .sum()
}

#[cfg(target_os = "linux")]
fn evict_directory(path: &Path) -> bool {
    if !path.exists() {
        return true;
    }
    let metadata = std::fs::metadata(path).unwrap();
    if metadata.is_file() {
        let file = OpenOptions::new().read(true).open(path).unwrap();
        return rustix::fs::fadvise(&file, 0, None, rustix::fs::Advice::DontNeed).is_ok();
    }
    std::fs::read_dir(path)
        .unwrap()
        .all(|entry| evict_directory(&entry.unwrap().path()))
}

#[cfg(not(target_os = "linux"))]
fn evict_directory(_path: &Path) -> bool {
    false
}

fn main() {
    let path = PathBuf::from(env::var("FIXED_LSM_BENCH_PATH").expect("FIXED_LSM_BENCH_PATH must be set"));
    assert!(path.is_absolute());
    let blobs = env_u64("FIXED_LSM_BENCH_BLOBS");
    assert!(blobs > 0);
    let config = BenchmarkConfig::from_env();
    println!(
        "{{\"phase\":\"fixed_lsm_config\",\"cache_mib\":{},\"write_buffer_mib\":{},\"block_kib\":8,\"bloom_bits_per_key\":14,\"buffered_io\":true,\"compression\":\"none\",\"compaction\":\"partitioned_leveled\",\"format_version\":7}}",
        config.cache_mib, config.write_buffer_mib,
    );
    match env::var("FIXED_LSM_BENCH_MODE")
        .unwrap_or_else(|_| "recover".to_string())
        .as_str()
    {
        "generate" => generate(&path, blobs, config),
        "recover" => recover(&path, blobs, config),
        "mixed" => mixed(&path, blobs, config),
        "churn" => churn(&path, blobs, config),
        "dirty_tail" => dirty_tail(&path, blobs, config),
        "checkpoint" => checkpoint(&path, config),
        _ => panic!("FIXED_LSM_BENCH_MODE must be generate, recover, mixed, churn, dirty_tail, or checkpoint"),
    }
}
