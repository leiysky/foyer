#![cfg(feature = "rocksdb-benchmark")]

#[cfg(target_os = "linux")]
use std::fs::OpenOptions;
use std::{
    env,
    io::Write,
    path::{Path, PathBuf},
    sync::Barrier,
    time::{Duration, Instant},
};

use rocksdb::{
    BlockBasedOptions, Cache, CompactionPri, DB, DBCompactionStyle, DBCompressionType, DataBlockIndexType,
    FlushOptions, Options, WaitForCompactOptions, WriteBatch, WriteOptions,
};

const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 32;
const DEFAULT_CACHE_MIB: usize = 512;
const DEFAULT_BLOCK_KIB: usize = 16;
const DEFAULT_BLOOM_BITS_PER_KEY: usize = 10;
const DEFAULT_WRITE_BUFFER_MIB: usize = 64;
const DEFAULT_MAX_WRITE_BUFFERS: i32 = 4;
const DEFAULT_BATCH_ENTRIES: usize = 16_128;
const DEFAULT_SAMPLE_INTERVAL: usize = 64;
const PERMUTATION_MULTIPLIER: u64 = 6_364_136_223_846_793_013;
const PERMUTATION_OFFSET: u64 = 1_442_695_040_888_963_407;

fn scale_key(index: u64) -> [u8; KEY_SIZE] {
    let mut bytes = [0; 24];
    bytes[..8].copy_from_slice(&index.to_be_bytes());
    bytes[8..16].copy_from_slice(&(!index).to_be_bytes());
    bytes
}

const fn encode_key(key: [u8; KEY_SIZE]) -> [u8; KEY_SIZE] {
    key
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BenchmarkLocation {
    physical_slot: u64,
    extent_generation: u32,
    stored_len: u32,
    checksum: u32,
    priority: u8,
}

impl BenchmarkLocation {
    fn encode(self) -> [u8; VALUE_SIZE] {
        let mut output = [0; VALUE_SIZE];
        output[..8].copy_from_slice(&self.physical_slot.to_le_bytes());
        output[8..12].copy_from_slice(&self.extent_generation.to_le_bytes());
        output[12..16].copy_from_slice(&self.stored_len.to_le_bytes());
        output[16..20].copy_from_slice(&self.checksum.to_le_bytes());
        output[20] = self.priority;
        output
    }

    fn decode(input: &[u8]) -> Option<Self> {
        if input.len() != VALUE_SIZE || input[20] > 2 {
            return None;
        }
        Some(Self {
            physical_slot: u64::from_le_bytes(input[..8].try_into().ok()?),
            extent_generation: u32::from_le_bytes(input[8..12].try_into().ok()?),
            stored_len: u32::from_le_bytes(input[12..16].try_into().ok()?),
            checksum: u32::from_le_bytes(input[16..20].try_into().ok()?),
            priority: input[20],
        })
    }
}

fn scale_location(index: u64, generation: u32) -> BenchmarkLocation {
    BenchmarkLocation {
        physical_slot: index,
        extent_generation: generation,
        stored_len: 16 * 1024,
        checksum: (index as u32).rotate_left(generation % 31),
        priority: (index % 3) as u8,
    }
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

struct BenchmarkConfig {
    cache_mib: usize,
    block_kib: usize,
    bloom_bits_per_key: usize,
    workers: usize,
    write_buffer_mib: usize,
    max_write_buffers: i32,
    collect_statistics: bool,
}

impl BenchmarkConfig {
    fn from_env() -> Self {
        Self {
            cache_mib: env_usize("ROCKSDB_INDEX_BENCH_CACHE_MIB", DEFAULT_CACHE_MIB),
            block_kib: env_usize("ROCKSDB_INDEX_BENCH_BLOCK_KIB", DEFAULT_BLOCK_KIB),
            bloom_bits_per_key: env_usize("ROCKSDB_INDEX_BENCH_BLOOM_BITS_PER_KEY", DEFAULT_BLOOM_BITS_PER_KEY),
            workers: env_usize(
                "ROCKSDB_INDEX_BENCH_WORKERS",
                std::thread::available_parallelism()
                    .map(std::num::NonZeroUsize::get)
                    .unwrap_or(1),
            ),
            write_buffer_mib: env_usize("ROCKSDB_INDEX_BENCH_WRITE_BUFFER_MIB", DEFAULT_WRITE_BUFFER_MIB),
            max_write_buffers: i32::try_from(env_usize(
                "ROCKSDB_INDEX_BENCH_MAX_WRITE_BUFFERS",
                DEFAULT_MAX_WRITE_BUFFERS as usize,
            ))
            .unwrap(),
            collect_statistics: env_bool("ROCKSDB_INDEX_BENCH_STATISTICS", false),
        }
    }
}

struct BenchmarkDatabase {
    db: DB,
    options: Options,
    cache: Cache,
}

fn open_database(path: &Path, config: &BenchmarkConfig, create: bool) -> BenchmarkDatabase {
    assert!(config.cache_mib > 0);
    assert!(config.block_kib > 0);
    assert!(config.workers > 0);
    assert!(config.write_buffer_mib > 0);
    assert!(config.max_write_buffers >= 2);

    let cache = Cache::new_lru_cache(config.cache_mib * 1024 * 1024);
    let mut table = BlockBasedOptions::default();
    table.set_block_size(config.block_kib * 1024);
    table.set_format_version(7);
    table.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);
    table.set_data_block_hash_ratio(0.75);
    table.set_block_cache(&cache);
    table.set_cache_index_and_filter_blocks(true);
    table.set_pin_l0_filter_and_index_blocks_in_cache(true);
    if config.bloom_bits_per_key > 0 {
        table.set_bloom_filter(config.bloom_bits_per_key as f64, false);
        table.set_optimize_filters_for_memory(true);
    }

    let mut options = Options::default();
    options.create_if_missing(create);
    options.set_block_based_table_factory(&table);
    options.set_compaction_style(DBCompactionStyle::Level);
    options.set_compaction_pri(CompactionPri::MinOverlappingRatio);
    options.set_compression_type(DBCompressionType::None);
    options.set_bottommost_compression_type(DBCompressionType::None);
    options.set_write_buffer_size(config.write_buffer_mib * 1024 * 1024);
    options.set_max_write_buffer_number(config.max_write_buffers);
    options.set_memtable_prefix_bloom_ratio(0.02);
    options.set_max_background_jobs(i32::try_from(config.workers).unwrap());
    options.set_bytes_per_sync(1024 * 1024);
    options.set_wal_bytes_per_sync(1024 * 1024);
    options.set_use_direct_reads(false);
    options.set_use_direct_io_for_flush_and_compaction(false);
    options.set_allow_mmap_reads(false);
    options.set_allow_mmap_writes(false);
    options.set_manual_wal_flush(false);
    options.set_use_fsync(false);
    options.set_keep_log_file_num(4);
    options.set_stats_dump_period_sec(0);
    if config.collect_statistics {
        options.enable_statistics();
    }
    let db = DB::open(&options, path).unwrap();
    BenchmarkDatabase { db, options, cache }
}

fn lookup(db: &DB, index: u64) -> BenchmarkLocation {
    let value = db
        .get_pinned(encode_key(scale_key(index)))
        .unwrap()
        .expect("benchmark key is missing");
    BenchmarkLocation::decode(&value).expect("benchmark location is corrupt")
}

fn validate_location(db: &DB, index: u64, generation: u32) {
    assert_eq!(lookup(db, index), scale_location(index, generation));
}

fn env_u64(name: &str) -> u64 {
    env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be set for the ignored benchmark"))
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

fn property(db: &DB, name: &str) -> u64 {
    db.property_int_value(name).unwrap().unwrap_or(0)
}

fn level_files_json(db: &DB) -> String {
    let files = (0..7)
        .map(|level| property(db, &format!("rocksdb.num-files-at-level{level}")))
        .map(|files| files.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("[{files}]")
}

#[derive(Clone, Copy, Default)]
struct Statistics {
    block_cache_hits: Option<u64>,
    block_cache_misses: Option<u64>,
    bytes_read: Option<u64>,
    bytes_written: Option<u64>,
    wal_bytes: Option<u64>,
    flush_write_bytes: Option<u64>,
    compaction_read_bytes: Option<u64>,
    compaction_write_bytes: Option<u64>,
    stall_micros: Option<u64>,
}

fn statistic(text: &str, name: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let suffix = line.strip_prefix(name)?;
        let (_, value) = suffix.split_once(':')?;
        value.split_whitespace().next()?.parse().ok()
    })
}

fn statistics(options: &Options) -> Statistics {
    let Some(text) = options.get_statistics() else {
        return Statistics::default();
    };
    Statistics {
        block_cache_hits: statistic(&text, "rocksdb.block.cache.hit"),
        block_cache_misses: statistic(&text, "rocksdb.block.cache.miss"),
        bytes_read: statistic(&text, "rocksdb.bytes.read"),
        bytes_written: statistic(&text, "rocksdb.bytes.written"),
        wal_bytes: statistic(&text, "rocksdb.wal.bytes"),
        flush_write_bytes: statistic(&text, "rocksdb.flush.write.bytes"),
        compaction_read_bytes: statistic(&text, "rocksdb.compact.read.bytes"),
        compaction_write_bytes: statistic(&text, "rocksdb.compact.write.bytes"),
        stall_micros: statistic(&text, "rocksdb.stall.micros"),
    }
}

fn difference(after: Option<u64>, before: Option<u64>) -> Option<u64> {
    after.zip(before).map(|(after, before)| after.saturating_sub(before))
}

fn wait_for_background(db: &DB) {
    db.wait_for_compact(&WaitForCompactOptions::default()).unwrap();
}

fn flush_memtable(db: &DB) {
    let mut options = FlushOptions::default();
    options.set_wait(true);
    db.flush_opt(&options).unwrap();
}

fn generate(path: &Path, blobs: u64, config: &BenchmarkConfig) {
    if path.exists() {
        assert!(env_bool("ROCKSDB_INDEX_BENCH_REGENERATE", false));
        std::fs::remove_dir_all(path).unwrap();
    }
    std::fs::create_dir_all(path).unwrap();
    let batch_entries = env_usize("ROCKSDB_INDEX_BENCH_BATCH_ENTRIES", DEFAULT_BATCH_ENTRIES);
    assert!(batch_entries > 0);
    let read_before = process_field("/proc/self/io", "read_bytes:");
    let write_before = process_field("/proc/self/io", "write_bytes:");
    let started = Instant::now();
    let database = open_database(path, config, true);
    let mut write_options = WriteOptions::default();
    write_options.disable_wal(true);
    let mut first = 0_u64;
    let mut next_progress = 5_000_000_u64.min(blobs);
    while first < blobs {
        let end = (first + batch_entries as u64).min(blobs);
        let mut batch = WriteBatch::with_capacity_bytes((end - first) as usize * 72);
        for index in first..end {
            batch.put(encode_key(scale_key(index)), scale_location(index, 1).encode());
        }
        database.db.write_opt(batch, &write_options).unwrap();
        first = end;
        if first >= next_progress {
            println!(
                "{{\"phase\":\"rocksdb_generate_progress\",\"blobs\":{first},\"total_blobs\":{blobs},\"elapsed_seconds\":{:.3}}}",
                started.elapsed().as_secs_f64(),
            );
            std::io::stdout().flush().unwrap();
            next_progress = next_progress.saturating_add(5_000_000).min(blobs);
        }
    }
    let publication = started.elapsed();
    let drain_started = Instant::now();
    flush_memtable(&database.db);
    wait_for_background(&database.db);
    let drain = drain_started.elapsed();
    assert_eq!(property(&database.db, "rocksdb.estimate-num-keys"), blobs);
    let read_bytes = read_before
        .zip(process_field("/proc/self/io", "read_bytes:"))
        .map(|(before, after)| after.saturating_sub(before));
    let write_bytes = write_before
        .zip(process_field("/proc/self/io", "write_bytes:"))
        .map(|(before, after)| after.saturating_sub(before));
    println!(
        "{{\"phase\":\"rocksdb_generate_result\",\"blobs\":{blobs},\"batch_entries\":{batch_entries},\"publication_ms\":{:.3},\"publication_mops\":{:.3},\"drain_ms\":{:.3},\"total_ms\":{:.3},\"disk_bytes\":{},\"live_sst_bytes\":{},\"total_sst_bytes\":{},\"level_files\":{},\"cache_usage_bytes\":{},\"read_bytes\":{},\"write_bytes\":{}}}",
        publication.as_secs_f64() * 1_000.0,
        blobs as f64 / publication.as_secs_f64() / 1_000_000.0,
        drain.as_secs_f64() * 1_000.0,
        started.elapsed().as_secs_f64() * 1_000.0,
        directory_bytes(path),
        property(&database.db, "rocksdb.live-sst-files-size"),
        property(&database.db, "rocksdb.total-sst-files-size"),
        level_files_json(&database.db),
        database.cache.get_usage(),
        optional_number(read_bytes),
        optional_number(write_bytes),
    );
}

struct LookupResult {
    elapsed: Duration,
    samples: Vec<Duration>,
    read_bytes: Option<u64>,
    statistics_before: Statistics,
    statistics_after: Statistics,
}

fn concurrent_hot_lookups(
    database: &BenchmarkDatabase,
    blobs: u64,
    concurrency: usize,
    lookups: usize,
    hot_keys: usize,
) -> LookupResult {
    let hot_keys = hot_keys.min(blobs as usize);
    assert!(hot_keys > 0);
    for index in 0..hot_keys as u64 {
        assert_eq!(lookup(&database.db, index).physical_slot, index);
    }
    let statistics_before = statistics(&database.options);
    let read_before = process_field("/proc/self/io", "read_bytes:");
    let barrier = Barrier::new(concurrency + 1);
    let (elapsed, mut samples) = std::thread::scope(|scope| {
        let mut workers = Vec::with_capacity(concurrency);
        for worker in 0..concurrency {
            let barrier = &barrier;
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
                    let location = lookup(&database.db, index);
                    assert_eq!(location.physical_slot, index);
                    std::hint::black_box(location);
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
    LookupResult {
        elapsed,
        samples,
        read_bytes: read_before
            .zip(process_field("/proc/self/io", "read_bytes:"))
            .map(|(before, after)| after.saturating_sub(before)),
        statistics_before,
        statistics_after: statistics(&database.options),
    }
}

fn recover(path: &Path, blobs: u64, config: &BenchmarkConfig) {
    let cold = env_bool("ROCKSDB_INDEX_BENCH_COLD", true);
    let lookups = env_usize("ROCKSDB_INDEX_BENCH_LOOKUPS", 0);
    let minimum_concurrency = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1)
        .saturating_mul(2);
    let concurrency = env_usize("ROCKSDB_INDEX_BENCH_CONCURRENCY", minimum_concurrency);
    let hot_keys = env_usize("ROCKSDB_INDEX_BENCH_HOT_KEYS", 258_048);
    assert!(lookups == 0 || concurrency >= minimum_concurrency);
    assert!(!cold || evict_directory(path));
    let read_before = process_field("/proc/self/io", "read_bytes:");
    let rss_before = process_field("/proc/self/status", "VmRSS:");
    let started = Instant::now();
    let database = open_database(path, config, false);
    let recovery = started.elapsed();
    let read_bytes = read_before
        .zip(process_field("/proc/self/io", "read_bytes:"))
        .map(|(before, after)| after.saturating_sub(before));
    for index in [0, 1.min(blobs - 1), blobs / 2, blobs - 1] {
        assert_eq!(lookup(&database.db, index).physical_slot, index);
    }
    let expected_updates = env_optional_u64("ROCKSDB_INDEX_BENCH_EXPECTED_UPDATES", 0);
    if expected_updates > 0 {
        assert!(expected_updates <= blobs);
        let generation = env_optional_u64("ROCKSDB_INDEX_BENCH_EXPECTED_GENERATION", 2) as u32;
        for operation in [0, expected_updates / 2, expected_updates - 1] {
            validate_location(&database.db, permuted_index(operation, blobs), generation);
        }
    }
    println!(
        "{{\"phase\":\"rocksdb_recovery_result\",\"blobs\":{blobs},\"cold\":{cold},\"recovery_ms\":{:.3},\"read_bytes\":{},\"rss_before_kib\":{},\"rss_after_kib\":{},\"peak_rss_kib\":{},\"estimate_num_keys\":{},\"live_sst_bytes\":{},\"total_sst_bytes\":{},\"level_files\":{},\"cache_usage_bytes\":{},\"disk_bytes\":{}}}",
        recovery.as_secs_f64() * 1_000.0,
        optional_number(read_bytes),
        optional_number(rss_before),
        optional_number(process_field("/proc/self/status", "VmRSS:")),
        optional_number(process_field("/proc/self/status", "VmHWM:")),
        property(&database.db, "rocksdb.estimate-num-keys"),
        property(&database.db, "rocksdb.live-sst-files-size"),
        property(&database.db, "rocksdb.total-sst-files-size"),
        level_files_json(&database.db),
        database.cache.get_usage(),
        directory_bytes(path),
    );
    if lookups > 0 {
        let result = concurrent_hot_lookups(&database, blobs, concurrency, lookups, hot_keys);
        println!(
            "{{\"phase\":\"rocksdb_concurrent_lookup_result\",\"lookups\":{lookups},\"concurrency\":{concurrency},\"hot_keys\":{hot_keys},\"elapsed_ms\":{:.3},\"throughput_mops\":{:.3},\"latency\":{},\"read_bytes\":{},\"block_cache_hits\":{},\"block_cache_misses\":{},\"cache_usage_bytes\":{}}}",
            result.elapsed.as_secs_f64() * 1_000.0,
            lookups as f64 / result.elapsed.as_secs_f64() / 1_000_000.0,
            latency_json(result.samples),
            optional_number(result.read_bytes),
            optional_number(difference(
                result.statistics_after.block_cache_hits,
                result.statistics_before.block_cache_hits,
            )),
            optional_number(difference(
                result.statistics_after.block_cache_misses,
                result.statistics_before.block_cache_misses,
            )),
            database.cache.get_usage(),
        );
    }
}

struct MixedResult {
    elapsed: Duration,
    reads: Vec<Duration>,
    writes: Vec<Duration>,
    statistics_before: Statistics,
    statistics_after: Statistics,
    workload_read_bytes: Option<u64>,
    workload_write_bytes: Option<u64>,
}

fn concurrent_mixed_access(
    database: &BenchmarkDatabase,
    blobs: u64,
    concurrency: usize,
    operations: usize,
    write_percent: usize,
    hot_access_percent: usize,
    hotset_percent: usize,
) -> MixedResult {
    assert!(operations >= concurrency * DEFAULT_SAMPLE_INTERVAL);
    assert!(write_percent > 0 && write_percent < 100);
    assert!(concurrency >= 2);
    let write_operations = operations * write_percent / 100;
    let read_operations = operations - write_operations;
    let reader_threads = concurrency - 1;
    let barrier = Barrier::new(concurrency + 1);
    let statistics_before = statistics(&database.options);
    let process_read_before = process_field("/proc/self/io", "read_bytes:");
    let process_write_before = process_field("/proc/self/io", "write_bytes:");
    let (elapsed, mut reads, mut writes) = std::thread::scope(|scope| {
        let worker_barrier = &barrier;
        let writer = scope.spawn(move || {
            let write_options = WriteOptions::default();
            let mut samples = Vec::with_capacity(write_operations.div_ceil(DEFAULT_SAMPLE_INTERVAL));
            worker_barrier.wait();
            for operation in 0..write_operations {
                let index = mixed_key_id(operation as u64, 0, blobs, hot_access_percent, hotset_percent);
                let sampled = operation.is_multiple_of(DEFAULT_SAMPLE_INTERVAL).then(Instant::now);
                database
                    .db
                    .put_opt(
                        encode_key(scale_key(index)),
                        scale_location(index, 2).encode(),
                        &write_options,
                    )
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
                    let location = lookup(&database.db, index);
                    assert_eq!(location.physical_slot, index);
                    std::hint::black_box(location);
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
        statistics_before,
        statistics_after: statistics(&database.options),
        workload_read_bytes: process_read_before
            .zip(process_field("/proc/self/io", "read_bytes:"))
            .map(|(before, after)| after.saturating_sub(before)),
        workload_write_bytes: process_write_before
            .zip(process_field("/proc/self/io", "write_bytes:"))
            .map(|(before, after)| after.saturating_sub(before)),
    }
}

fn mixed(path: &Path, blobs: u64, config: &BenchmarkConfig) {
    let operations = env_usize("ROCKSDB_INDEX_BENCH_MIXED_OPERATIONS", 8_000_000);
    let write_percent = env_usize("ROCKSDB_INDEX_BENCH_MIXED_WRITE_PERCENT", 50);
    let hot_access_percent = env_usize("ROCKSDB_INDEX_BENCH_HOT_ACCESS_PERCENT", 0);
    let hotset_percent = env_usize("ROCKSDB_INDEX_BENCH_HOTSET_PERCENT", 1);
    let minimum_concurrency = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1)
        .saturating_mul(2);
    let concurrency = env_usize("ROCKSDB_INDEX_BENCH_CONCURRENCY", minimum_concurrency);
    let cold = env_bool("ROCKSDB_INDEX_BENCH_COLD", true);
    let sync_after = env_bool("ROCKSDB_INDEX_BENCH_SYNC_AFTER", true);
    let drain_after = env_bool("ROCKSDB_INDEX_BENCH_DRAIN_AFTER", true);
    assert!(concurrency >= minimum_concurrency);
    assert!(hot_access_percent <= 100 && hotset_percent > 0 && hotset_percent <= 100);
    assert!(!cold || evict_directory(path));
    let rss_before = process_field("/proc/self/status", "VmRSS:");
    let database = open_database(path, config, false);
    let rss_after_open = process_field("/proc/self/status", "VmRSS:");
    let total_read_before = process_field("/proc/self/io", "read_bytes:");
    let total_write_before = process_field("/proc/self/io", "write_bytes:");
    let result = concurrent_mixed_access(
        &database,
        blobs,
        concurrency,
        operations,
        write_percent,
        hot_access_percent,
        hotset_percent,
    );
    let sync_started = Instant::now();
    if sync_after {
        database.db.flush_wal(true).unwrap();
    }
    let sync = sync_started.elapsed();
    let drain_started = Instant::now();
    if drain_after {
        flush_memtable(&database.db);
        wait_for_background(&database.db);
    }
    let drain = drain_started.elapsed();
    let statistics_after_drain = statistics(&database.options);
    let total_read_bytes = total_read_before
        .zip(process_field("/proc/self/io", "read_bytes:"))
        .map(|(before, after)| after.saturating_sub(before));
    let total_write_bytes = total_write_before
        .zip(process_field("/proc/self/io", "write_bytes:"))
        .map(|(before, after)| after.saturating_sub(before));
    println!(
        "{{\"phase\":\"rocksdb_mixed_result\",\"blobs\":{blobs},\"operations\":{operations},\"concurrency\":{concurrency},\"writer_threads\":1,\"reader_threads\":{},\"write_percent\":{write_percent},\"hot_access_percent\":{hot_access_percent},\"hotset_percent\":{hotset_percent},\"elapsed_ms\":{:.3},\"throughput_mops\":{:.3},\"read_latency\":{},\"write_latency\":{},\"sync_after\":{sync_after},\"sync_ms\":{:.3},\"drain_after\":{drain_after},\"drain_ms\":{:.3},\"rss_before_kib\":{},\"rss_after_open_kib\":{},\"rss_after_kib\":{},\"peak_rss_kib\":{},\"workload_read_bytes\":{},\"workload_write_bytes\":{},\"total_read_bytes\":{},\"total_write_bytes\":{},\"block_cache_hits\":{},\"block_cache_misses\":{},\"internal_read_bytes\":{},\"internal_write_bytes\":{},\"wal_bytes\":{},\"flush_write_bytes\":{},\"compaction_read_bytes\":{},\"compaction_write_bytes\":{},\"stall_micros\":{},\"disk_bytes\":{},\"live_sst_bytes\":{},\"level_files\":{},\"cache_usage_bytes\":{}}}",
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
        optional_number(total_read_bytes),
        optional_number(total_write_bytes),
        optional_number(difference(
            result.statistics_after.block_cache_hits,
            result.statistics_before.block_cache_hits,
        )),
        optional_number(difference(
            result.statistics_after.block_cache_misses,
            result.statistics_before.block_cache_misses,
        )),
        optional_number(difference(
            statistics_after_drain.bytes_read,
            result.statistics_before.bytes_read,
        )),
        optional_number(difference(
            statistics_after_drain.bytes_written,
            result.statistics_before.bytes_written,
        )),
        optional_number(difference(
            statistics_after_drain.wal_bytes,
            result.statistics_before.wal_bytes,
        )),
        optional_number(difference(
            statistics_after_drain.flush_write_bytes,
            result.statistics_before.flush_write_bytes,
        )),
        optional_number(difference(
            statistics_after_drain.compaction_read_bytes,
            result.statistics_before.compaction_read_bytes,
        )),
        optional_number(difference(
            statistics_after_drain.compaction_write_bytes,
            result.statistics_before.compaction_write_bytes,
        )),
        optional_number(difference(
            statistics_after_drain.stall_micros,
            result.statistics_before.stall_micros,
        )),
        directory_bytes(path),
        property(&database.db, "rocksdb.live-sst-files-size"),
        level_files_json(&database.db),
        database.cache.get_usage(),
    );
}

fn churn(path: &Path, blobs: u64, config: &BenchmarkConfig) {
    let updates = env_u64("ROCKSDB_INDEX_BENCH_UPDATES");
    let generation = env_optional_u64("ROCKSDB_INDEX_BENCH_GENERATION", 2) as u32;
    let batch_entries = env_usize("ROCKSDB_INDEX_BENCH_BATCH_ENTRIES", DEFAULT_BATCH_ENTRIES);
    let drain = env_bool("ROCKSDB_INDEX_BENCH_DRAIN", true);
    let major_compact = env_bool("ROCKSDB_INDEX_BENCH_MAJOR_COMPACT", false);
    let replace = env_bool("ROCKSDB_INDEX_BENCH_REPLACE", false);
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
    let database = open_database(path, config, false);
    let statistics_before = statistics(&database.options);
    let rss_before = process_field("/proc/self/status", "VmRSS:");
    let disk_before = directory_bytes(path);
    let read_before = process_field("/proc/self/io", "read_bytes:");
    let write_before = process_field("/proc/self/io", "write_bytes:");
    let publication_started = Instant::now();
    let write_options = WriteOptions::default();
    let mut commits = Vec::new();
    let mut first = 0_u64;
    while first < updates {
        let end = (first + batch_entries as u64).min(updates);
        let started = Instant::now();
        let mut batch = WriteBatch::with_capacity_bytes((end - first) as usize * 72 * if replace { 2 } else { 1 });
        for operation in first..end {
            let index = permuted_index(cohort_start + operation, blobs);
            if replace {
                batch.delete(encode_key(scale_key(index)));
                let replacement = blobs + index;
                batch.put(
                    encode_key(scale_key(replacement)),
                    scale_location(replacement, generation).encode(),
                );
            } else {
                batch.put(encode_key(scale_key(index)), scale_location(index, generation).encode());
            }
        }
        database.db.write_opt(batch, &write_options).unwrap();
        commits.push(started.elapsed());
        first = end;
    }
    let publication = publication_started.elapsed();
    let persist_started = Instant::now();
    database.db.flush_wal(true).unwrap();
    let persist = persist_started.elapsed();
    let drain_started = Instant::now();
    if drain {
        flush_memtable(&database.db);
        wait_for_background(&database.db);
    }
    if major_compact {
        database.db.compact_range::<&[u8], &[u8]>(None, None);
        wait_for_background(&database.db);
    }
    let drain_elapsed = drain_started.elapsed();
    for operation in [0, updates / 2, updates - 1] {
        let index = permuted_index(cohort_start + operation, blobs);
        if replace {
            assert!(database.db.get(encode_key(scale_key(index))).unwrap().is_none());
            let replacement = blobs + index;
            validate_location(&database.db, replacement, generation);
        } else {
            validate_location(&database.db, index, generation);
        }
    }
    let statistics_after = statistics(&database.options);
    let read_bytes = read_before
        .zip(process_field("/proc/self/io", "read_bytes:"))
        .map(|(before, after)| after.saturating_sub(before));
    let write_bytes = write_before
        .zip(process_field("/proc/self/io", "write_bytes:"))
        .map(|(before, after)| after.saturating_sub(before));
    println!(
        "{{\"phase\":\"rocksdb_churn_result\",\"blobs\":{blobs},\"updates\":{updates},\"mutations\":{mutations},\"generation\":{generation},\"replace\":{replace},\"batch_entries\":{batch_entries},\"drain\":{drain},\"major_compact\":{major_compact},\"publication_ms\":{:.3},\"publication_mops\":{:.3},\"publication_mutation_mops\":{:.3},\"commit_latency\":{},\"persist_ms\":{:.3},\"drain_ms\":{:.3},\"rss_before_kib\":{},\"rss_after_kib\":{},\"peak_rss_kib\":{},\"disk_bytes_before\":{disk_before},\"disk_bytes_after\":{},\"live_sst_bytes\":{},\"total_sst_bytes\":{},\"level_files\":{},\"read_bytes\":{},\"write_bytes\":{},\"bytes_per_update\":{},\"bytes_per_mutation\":{},\"internal_read_bytes\":{},\"internal_write_bytes\":{},\"wal_bytes\":{},\"flush_write_bytes\":{},\"compaction_read_bytes\":{},\"compaction_write_bytes\":{},\"stall_micros\":{},\"cache_usage_bytes\":{}}}",
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
        property(&database.db, "rocksdb.live-sst-files-size"),
        property(&database.db, "rocksdb.total-sst-files-size"),
        level_files_json(&database.db),
        optional_number(read_bytes),
        optional_number(write_bytes),
        write_bytes.map_or_else(
            || "null".to_string(),
            |bytes| format!("{:.3}", bytes as f64 / updates as f64),
        ),
        write_bytes.map_or_else(
            || "null".to_string(),
            |bytes| format!("{:.3}", bytes as f64 / mutations as f64),
        ),
        optional_number(difference(statistics_after.bytes_read, statistics_before.bytes_read,)),
        optional_number(difference(
            statistics_after.bytes_written,
            statistics_before.bytes_written,
        )),
        optional_number(difference(statistics_after.wal_bytes, statistics_before.wal_bytes,)),
        optional_number(difference(
            statistics_after.flush_write_bytes,
            statistics_before.flush_write_bytes,
        )),
        optional_number(difference(
            statistics_after.compaction_read_bytes,
            statistics_before.compaction_read_bytes,
        )),
        optional_number(difference(
            statistics_after.compaction_write_bytes,
            statistics_before.compaction_write_bytes,
        )),
        optional_number(difference(
            statistics_after.stall_micros,
            statistics_before.stall_micros,
        )),
        database.cache.get_usage(),
    );
}

fn checkpoint(path: &Path, config: &BenchmarkConfig) {
    let major_compact = env_bool("ROCKSDB_INDEX_BENCH_MAJOR_COMPACT", false);
    let read_before = process_field("/proc/self/io", "read_bytes:");
    let write_before = process_field("/proc/self/io", "write_bytes:");
    let database = open_database(path, config, false);
    let started = Instant::now();
    flush_memtable(&database.db);
    wait_for_background(&database.db);
    if major_compact {
        database.db.compact_range::<&[u8], &[u8]>(None, None);
        wait_for_background(&database.db);
    }
    let elapsed = started.elapsed();
    let read_bytes = read_before
        .zip(process_field("/proc/self/io", "read_bytes:"))
        .map(|(before, after)| after.saturating_sub(before));
    let write_bytes = write_before
        .zip(process_field("/proc/self/io", "write_bytes:"))
        .map(|(before, after)| after.saturating_sub(before));
    println!(
        "{{\"phase\":\"rocksdb_checkpoint_result\",\"major_compact\":{major_compact},\"elapsed_ms\":{:.3},\"read_bytes\":{},\"write_bytes\":{},\"disk_bytes\":{},\"live_sst_bytes\":{},\"total_sst_bytes\":{},\"level_files\":{}}}",
        elapsed.as_secs_f64() * 1_000.0,
        optional_number(read_bytes),
        optional_number(write_bytes),
        directory_bytes(path),
        property(&database.db, "rocksdb.live-sst-files-size"),
        property(&database.db, "rocksdb.total-sst-files-size"),
        level_files_json(&database.db),
    );
}

#[test]
fn rocksdb_index_roundtrips_fixed_keys_and_locations() {
    let directory = tempfile::tempdir().unwrap();
    let config = BenchmarkConfig {
        cache_mib: 1,
        block_kib: 4,
        bloom_bits_per_key: 10,
        workers: 1,
        write_buffer_mib: 1,
        max_write_buffers: 2,
        collect_statistics: true,
    };
    let database = open_database(directory.path(), &config, true);
    let mut batch = WriteBatch::default();
    for index in 0..128 {
        batch.put(encode_key(scale_key(index)), scale_location(index, 1).encode());
    }
    let mut write_options = WriteOptions::default();
    write_options.set_sync(true);
    database.db.write_opt(batch, &write_options).unwrap();
    for index in [0, 1, 63, 127] {
        validate_location(&database.db, index, 1);
    }
    drop(database);

    let database = open_database(directory.path(), &config, false);
    assert_eq!(property(&database.db, "rocksdb.estimate-num-keys"), 128);
    validate_location(&database.db, 127, 1);
}

#[test]
#[ignore = "requires an explicitly selected large disk path"]
fn rocksdb_index_scale_benchmark() {
    assert_eq!(std::mem::size_of::<[u8; KEY_SIZE]>(), KEY_SIZE);
    assert_eq!(std::mem::size_of::<[u8; VALUE_SIZE]>(), VALUE_SIZE);
    let path = PathBuf::from(
        env::var("ROCKSDB_INDEX_BENCH_PATH").expect("ROCKSDB_INDEX_BENCH_PATH must be set for the ignored benchmark"),
    );
    assert!(path.is_absolute());
    let blobs = env_u64("ROCKSDB_INDEX_BENCH_BLOBS");
    assert!(blobs > 0);
    let config = BenchmarkConfig::from_env();
    println!(
        "{{\"phase\":\"rocksdb_config\",\"cache_mib\":{},\"block_kib\":{},\"bloom_bits_per_key\":{},\"workers\":{},\"write_buffer_mib\":{},\"max_write_buffers\":{},\"statistics\":{},\"buffered_io\":true,\"compression\":\"none\",\"compaction\":\"leveled_min_overlap\",\"data_block_index\":\"binary_and_hash\",\"format_version\":7}}",
        config.cache_mib,
        config.block_kib,
        config.bloom_bits_per_key,
        config.workers,
        config.write_buffer_mib,
        config.max_write_buffers,
        config.collect_statistics,
    );
    let mode = env::var("ROCKSDB_INDEX_BENCH_MODE").unwrap_or_else(|_| "recover".to_string());
    match mode.as_str() {
        "generate" => generate(&path, blobs, &config),
        "recover" => recover(&path, blobs, &config),
        "mixed" => mixed(&path, blobs, &config),
        "churn" => churn(&path, blobs, &config),
        "checkpoint" => checkpoint(&path, &config),
        _ => panic!("ROCKSDB_INDEX_BENCH_MODE must be generate, recover, mixed, churn, or checkpoint"),
    }
}
