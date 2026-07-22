use std::{
    env,
    error::Error,
    hash::BuildHasher,
    hint::black_box,
    io::{self, Write},
    num::NonZeroUsize,
    path::PathBuf,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use foyer::DefaultHasher;
use foyer_storage::test_utils::BenchBlockIndexer;
use index_db::{IndexDb, IndexDbOptions, Key};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;
const INSERT_BATCH: u64 = 65_536;
const LOOKUP_BATCH: u64 = 1_024;
const LATENCY_SAMPLE_LIMIT: usize = 200_000;
const KEY_SIZES: [usize; 4] = [32, 96, 256, 1024];

type AnyResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EngineKind {
    IndexDb,
    Memory,
}

impl EngineKind {
    fn from_env() -> AnyResult<Self> {
        match env::var("INDEX_BENCH_ENGINE") {
            Ok(value) if value == "index-db" => Ok(Self::IndexDb),
            Ok(value) if value == "memory" => Ok(Self::Memory),
            Ok(_) => Err(invalid("INDEX_BENCH_ENGINE accepts index-db or memory").into()),
            Err(env::VarError::NotPresent) => Err(invalid("INDEX_BENCH_ENGINE must be set").into()),
            Err(error) => Err(error.into()),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::IndexDb => "index-db",
            Self::Memory => "memory",
        }
    }
}

#[derive(Debug)]
struct Config {
    engine: EngineKind,
    path: Option<PathBuf>,
    entries: u64,
    hotset: u64,
    cache_capacity: usize,
    concurrency: usize,
    warmup: Duration,
    duration: Duration,
    profile_delay: Duration,
}

impl Config {
    fn from_env() -> AnyResult<Self> {
        let engine = EngineKind::from_env()?;
        let entries = env_u64("INDEX_BENCH_ENTRIES", 10_000_000)?;
        let hotset = env_u64("INDEX_BENCH_HOTSET", 10_000)?;
        if entries == 0 || hotset == 0 || hotset > entries {
            return Err(invalid("INDEX_BENCH_HOTSET must be in 1..=INDEX_BENCH_ENTRIES").into());
        }
        let cores = thread::available_parallelism().map(NonZeroUsize::get).unwrap_or(1);
        let concurrency = env_usize("INDEX_BENCH_CONCURRENCY", cores.saturating_mul(2))?;
        if concurrency == 0 {
            return Err(invalid("INDEX_BENCH_CONCURRENCY must be positive").into());
        }
        let path = env::var_os("INDEX_BENCH_PATH").map(PathBuf::from);
        if engine == EngineKind::IndexDb && path.is_none() {
            return Err(invalid("INDEX_BENCH_PATH is required for IndexDB").into());
        }
        Ok(Self {
            engine,
            path,
            entries,
            hotset,
            cache_capacity: env_usize("INDEX_BENCH_CACHE_MIB", 1024)?.saturating_mul(MIB),
            concurrency,
            warmup: Duration::from_secs(env_u64("INDEX_BENCH_WARMUP_SECONDS", 3)?),
            duration: Duration::from_secs(env_u64("INDEX_BENCH_SECONDS", 10)?),
            profile_delay: Duration::from_secs(env_u64("INDEX_BENCH_PROFILE_DELAY_SECONDS", 0)?),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct Query {
    index_key: Key,
    memory: u64,
}

#[derive(Debug)]
enum BenchIndex {
    IndexDb(IndexDb),
    Memory(BenchBlockIndexer),
}

impl BenchIndex {
    fn open(config: &Config) -> AnyResult<Self> {
        match config.engine {
            EngineKind::IndexDb => {
                let database = IndexDb::open(
                    config.path.as_ref().unwrap(),
                    IndexDbOptions {
                        write_buffer_capacity: 64 * MIB,
                        cache_capacity: config.cache_capacity,
                        max_disk_bytes: u64::MAX,
                    },
                )?;
                Ok(Self::IndexDb(database))
            }
            EngineKind::Memory => {
                let indexer = BenchBlockIndexer::new(config.concurrency.next_power_of_two());
                let mut start = 0;
                while start < config.entries {
                    let end = start.saturating_add(INSERT_BATCH).min(config.entries);
                    let mut batch = Vec::with_capacity((end - start) as usize);
                    for index in start..end {
                        batch.push((memory_hash(index), index + 1));
                    }
                    indexer.insert_batch(batch);
                    start = end;
                }
                Ok(Self::Memory(indexer))
            }
        }
    }

    #[inline]
    fn get(&self, query: Query) -> bool {
        match self {
            Self::IndexDb(database) => database
                .get(&query.index_key)
                .expect("IndexDB lookup must succeed")
                .is_some(),
            Self::Memory(indexer) => indexer.contains(query.memory),
        }
    }

    fn print_stats(&self) {
        if let Self::IndexDb(database) = self {
            let stats = database.stats();
            println!(
                "phase=index_stats level_files={:?} base_level={} cache_hits={} cache_misses={} cache_resident_mib={:.1} cache_data_mib={:.1} cache_metadata_mib={:.1} table_read_ops={} table_read_mib={:.1} filter_checks={} filter_positives={} data_cache_hits={} data_reads={}",
                stats.level_files,
                stats.base_level,
                stats.cache_hits,
                stats.cache_misses,
                as_mib(stats.cache_resident_bytes),
                as_mib(stats.cache_data_resident_bytes),
                as_mib(stats.cache_metadata_resident_bytes),
                stats.table_read_operations,
                as_mib(stats.table_read_bytes),
                stats.point_filter_checks,
                stats.point_filter_positives,
                stats.point_data_cache_hits,
                stats.point_data_reads,
            );
        }
    }
}

#[derive(Debug, Default)]
struct Measurements {
    operations: u64,
    hits: u64,
    elapsed: Duration,
    latencies: Vec<Duration>,
}

fn main() -> AnyResult<()> {
    let config = Config::from_env()?;
    println!(
        "index-hot-path: engine={} entries={} hotset={} cache_mib={} concurrency={} warmup_seconds={} seconds={} profile_delay_seconds={}",
        config.engine.label(),
        config.entries,
        config.hotset,
        config.cache_capacity / MIB,
        config.concurrency,
        config.warmup.as_secs(),
        config.duration.as_secs(),
        config.profile_delay.as_secs(),
    );

    let prepared = Instant::now();
    let queries = Arc::new(build_queries(config.hotset));
    let index = Arc::new(BenchIndex::open(&config)?);
    println!(
        "phase=prepare seconds={:.3} peak_rss_mib={}",
        prepared.elapsed().as_secs_f64(),
        peak_rss_mib()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
    );

    let warmup = run(index.clone(), queries.clone(), config.concurrency, config.warmup, false);
    if warmup.hits != warmup.operations {
        return Err(invalid(format!(
            "warmup lookup lost entries: hits={} operations={}",
            warmup.hits, warmup.operations
        ))
        .into());
    }
    println!(
        "phase=warmup operations={} seconds={:.3} ops_s={:.0}",
        warmup.operations,
        warmup.elapsed.as_secs_f64(),
        throughput(warmup.operations, warmup.elapsed),
    );
    index.print_stats();

    if !config.profile_delay.is_zero() {
        println!(
            "phase=profile_ready pid={} delay_seconds={}",
            std::process::id(),
            config.profile_delay.as_secs()
        );
        io::stdout().flush()?;
        thread::sleep(config.profile_delay);
    }

    let measured = run(index.clone(), queries, config.concurrency, config.duration, true);
    if measured.hits != measured.operations {
        return Err(invalid(format!(
            "measured lookup lost entries: hits={} operations={}",
            measured.hits, measured.operations
        ))
        .into());
    }
    println!(
        "phase=measure operations={} hits={} seconds={:.3} ops_s={:.0} ns_per_op={:.1} peak_rss_mib={}",
        measured.operations,
        measured.hits,
        measured.elapsed.as_secs_f64(),
        throughput(measured.operations, measured.elapsed),
        measured.elapsed.as_nanos() as f64 * config.concurrency as f64 / measured.operations.max(1) as f64,
        peak_rss_mib()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
    );
    print_latencies(&measured.latencies);
    index.print_stats();
    Ok(())
}

fn run(
    index: Arc<BenchIndex>,
    queries: Arc<Vec<Query>>,
    concurrency: usize,
    duration: Duration,
    sample: bool,
) -> Measurements {
    if duration.is_zero() {
        return Measurements::default();
    }
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut workers = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let index = index.clone();
        let queries = queries.clone();
        let barrier = barrier.clone();
        workers.push(thread::spawn(move || {
            let mut measurements = Measurements::default();
            let mut operation = worker as u64;
            barrier.wait();
            let started = Instant::now();
            let deadline = started + duration;
            while Instant::now() < deadline {
                for batch_offset in 0..LOOKUP_BATCH {
                    let query = queries[(mix64(operation) % queries.len() as u64) as usize];
                    if sample && batch_offset == 0 && measurements.latencies.len() < LATENCY_SAMPLE_LIMIT / concurrency
                    {
                        let lookup = Instant::now();
                        measurements.hits += u64::from(black_box(index.get(query)));
                        measurements.latencies.push(lookup.elapsed());
                    } else {
                        measurements.hits += u64::from(black_box(index.get(query)));
                    }
                    measurements.operations += 1;
                    operation = operation.wrapping_add(concurrency as u64);
                }
            }
            measurements.elapsed = started.elapsed();
            measurements
        }));
    }
    barrier.wait();
    let started = Instant::now();
    let mut measurements = Measurements::default();
    for worker in workers {
        let mut worker = worker.join().expect("index benchmark worker must not panic");
        measurements.operations += worker.operations;
        measurements.hits += worker.hits;
        measurements.latencies.append(&mut worker.latencies);
    }
    measurements.elapsed = started.elapsed();
    measurements
}

fn build_queries(hotset: u64) -> Vec<Query> {
    let hasher = DefaultHasher::default();
    (0..hotset)
        .map(|index| {
            let key = make_key(index, KEY_SIZES[index as usize % KEY_SIZES.len()]);
            let digest = blake3::hash(&key);
            let mut index_key = [0; index_db::KEY_SIZE];
            index_key.copy_from_slice(&digest.as_bytes()[..index_db::KEY_SIZE]);
            Query {
                index_key,
                memory: hasher.hash_one(&key),
            }
        })
        .collect()
}

fn make_key(index: u64, len: usize) -> Vec<u8> {
    let mut key = vec![0u8; len];
    let encoded = index.to_le_bytes();
    let prefix = encoded.len().min(len);
    key[..prefix].copy_from_slice(&encoded[..prefix]);
    for (offset, byte) in key[prefix..].iter_mut().enumerate() {
        *byte = mix64(index.wrapping_add(offset as u64)) as u8;
    }
    key
}

fn memory_hash(index: u64) -> u64 {
    let hasher = DefaultHasher::default();
    let key = make_key(index, KEY_SIZES[index as usize % KEY_SIZES.len()]);
    hasher.hash_one(key)
}

const fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn print_latencies(latencies: &[Duration]) {
    let mut values = latencies.to_vec();
    values.sort_unstable();
    println!(
        "phase=latency samples={} p50_ns={} p95_ns={} p99_ns={} p999_ns={} max_ns={}",
        values.len(),
        quantile(&values, 500, 1_000).as_nanos(),
        quantile(&values, 950, 1_000).as_nanos(),
        quantile(&values, 990, 1_000).as_nanos(),
        quantile(&values, 999, 1_000).as_nanos(),
        values.last().copied().unwrap_or_default().as_nanos(),
    );
}

fn quantile(values: &[Duration], numerator: usize, denominator: usize) -> Duration {
    if values.is_empty() {
        return Duration::ZERO;
    }
    let index = (values.len() - 1).saturating_mul(numerator) / denominator;
    values[index]
}

fn throughput(operations: u64, elapsed: Duration) -> f64 {
    operations as f64 / elapsed.as_secs_f64().max(f64::EPSILON)
}

fn as_mib(bytes: u64) -> f64 {
    bytes as f64 / MIB as f64
}

fn env_u64(name: &str, default: u64) -> AnyResult<u64> {
    match env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn env_usize(name: &str, default: usize) -> AnyResult<usize> {
    let value = env_u64(name, default as u64)?;
    usize::try_from(value).map_err(Into::into)
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(target_os = "linux")]
fn peak_rss_mib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
    line.split_ascii_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()
        .map(|kib| kib / KIB as u64)
}

#[cfg(not(target_os = "linux"))]
fn peak_rss_mib() -> Option<u64> {
    None
}
