use std::{
    env,
    error::Error,
    fs, io,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use foyer::{
    BlockEngineConfig, Compression, DeviceBuilder, EngineConfig, FsDeviceBuilder, Hint, HybridCache, HybridCachePolicy,
    HybridCacheProperties, Load, PsyncIoEngineConfig, RecoverMode, S3FifoConfig,
};
use foyer_extent::{
    CachePriority, DEFAULT_HIGH_PRIORITY_CAPACITY_PERCENT, DEFAULT_NORMAL_PRIORITY_CAPACITY_PERCENT, EngineValue,
    ExtentEngineConfig, ExtentEngineHandle, MAX_BLOB_KEY_SIZE,
};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;
const LATENCY_SAMPLE_TARGET: u64 = 200_000;
const ENTRY_OVERHEAD: usize = 64;

type AnyResult<T> = Result<T, Box<dyn Error>>;
type BenchCache = HybridCache<Bytes, EngineValue>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiskEngine {
    Block,
    Extent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PriorityWorkload {
    ScopeDb,
    HistoricalHigh,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadPattern {
    Random,
    Sequential,
}

impl ReadPattern {
    const fn label(self) -> &'static str {
        match self {
            Self::Random => "random",
            Self::Sequential => "sequential",
        }
    }
}

impl PriorityWorkload {
    const fn label(self) -> &'static str {
        match self {
            Self::ScopeDb => "scopedb",
            Self::HistoricalHigh => "historical-high",
        }
    }
}

impl DiskEngine {
    const fn label(self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::Extent => "extent",
        }
    }
}

#[derive(Debug)]
struct Config {
    root: PathBuf,
    engines: Vec<DiskEngine>,
    capacity_bytes: usize,
    memory_bytes: usize,
    queue_bytes: usize,
    wave_bytes: usize,
    block_size_bytes: usize,
    block_buffer_pool_bytes: usize,
    extent_slot_size: usize,
    extent_segment_size: usize,
    extent_index_cache_bytes: usize,
    extent_index_write_buffer_bytes: usize,
    extent_io_read_priority: Duration,
    extent_high_capacity_percent: u8,
    extent_normal_capacity_percent: u8,
    concurrency: usize,
    put_concurrency: usize,
    shards: usize,
    write_concurrency: usize,
    reads: u64,
    read_warmup: u64,
    read_hotset: u64,
    read_pattern: ReadPattern,
    storage_reads: bool,
    direct_io: bool,
    recover_only: bool,
    recover_write_wave: bool,
    populate_only: bool,
    reset: bool,
}

impl Config {
    fn from_env(workload: &Workload) -> AnyResult<Self> {
        let root = env::var_os("EXTENT_BENCH_PATH")
            .map(PathBuf::from)
            .ok_or_else(|| invalid("EXTENT_BENCH_PATH must point to a real benchmark disk"))?;
        if root == Path::new("/") {
            return Err(invalid("EXTENT_BENCH_PATH must not be the filesystem root").into());
        }

        let cores = std::thread::available_parallelism().map(NonZeroUsize::get).unwrap_or(1);
        let minimum_concurrency = cores.saturating_mul(2);
        let concurrency = env_usize("EXTENT_BENCH_CONCURRENCY", minimum_concurrency)?;
        if concurrency < minimum_concurrency {
            return Err(invalid(format!(
                "EXTENT_BENCH_CONCURRENCY must be at least 2x CPU cores ({minimum_concurrency})"
            ))
            .into());
        }

        let capacity_bytes = env_mib("EXTENT_BENCH_CAPACITY_MIB", 512)?;
        let queue_bytes = env_mib("EXTENT_BENCH_QUEUE_MIB", 256)?;
        let wave_bytes = env_mib("EXTENT_BENCH_WAVE_MIB", 64)?;
        if wave_bytes.saturating_add(workload.maximum_entry_size()) > queue_bytes {
            return Err(invalid("EXTENT_BENCH_WAVE_MIB plus the largest entry must fit EXTENT_BENCH_QUEUE_MIB").into());
        }

        let block_size_bytes = env_mib("EXTENT_BENCH_BLOCK_MIB", 64)?;
        if capacity_bytes / block_size_bytes < 3 {
            return Err(invalid("disk capacity must contain at least three Foyer blocks").into());
        }

        let direct_io = env_bool("EXTENT_BENCH_DIRECT", false)?;
        if direct_io && !cfg!(target_os = "linux") {
            return Err(invalid("direct I/O benchmark mode is only supported on Linux").into());
        }
        let recover_only = env_bool("EXTENT_BENCH_RECOVER_ONLY", false)?;
        let populate_only = env_bool("EXTENT_BENCH_POPULATE_ONLY", false)?;
        if recover_only && populate_only {
            return Err(
                invalid("EXTENT_BENCH_RECOVER_ONLY and EXTENT_BENCH_POPULATE_ONLY are mutually exclusive").into(),
            );
        }
        let extent_high_capacity_percent = env_percent(
            "EXTENT_BENCH_HIGH_CAPACITY_PERCENT",
            DEFAULT_HIGH_PRIORITY_CAPACITY_PERCENT,
        )?;
        let extent_normal_capacity_percent = env_percent(
            "EXTENT_BENCH_NORMAL_CAPACITY_PERCENT",
            DEFAULT_NORMAL_PRIORITY_CAPACITY_PERCENT,
        )?;
        if u16::from(extent_high_capacity_percent) + u16::from(extent_normal_capacity_percent) > 100 {
            return Err(invalid(
                "EXTENT_BENCH_HIGH_CAPACITY_PERCENT and EXTENT_BENCH_NORMAL_CAPACITY_PERCENT must sum to at most 100",
            )
            .into());
        }

        let put_concurrency = env_usize("EXTENT_BENCH_PUT_CONCURRENCY", concurrency)?;
        if put_concurrency == 0 {
            return Err(invalid("EXTENT_BENCH_PUT_CONCURRENCY must be positive").into());
        }

        let reads = env_u64("EXTENT_BENCH_READS", workload.entries.saturating_mul(2))?;
        let read_hotset = env_u64("EXTENT_BENCH_READ_HOTSET", workload.entries)?;
        if read_hotset == 0 || read_hotset > workload.entries {
            return Err(invalid("EXTENT_BENCH_READ_HOTSET must be in 1..=EXTENT_BENCH_ENTRIES").into());
        }

        Ok(Self {
            root,
            engines: parse_engines()?,
            capacity_bytes,
            memory_bytes: env_mib("EXTENT_BENCH_MEMORY_MIB", 256)?,
            queue_bytes,
            wave_bytes,
            block_size_bytes,
            block_buffer_pool_bytes: env_mib("EXTENT_BENCH_BLOCK_BUFFER_MIB", 256)?,
            extent_slot_size: env_kib("EXTENT_BENCH_SLOT_KIB", foyer_extent::DEFAULT_SLOT_SIZE / KIB)?,
            extent_segment_size: env_mib("EXTENT_BENCH_SEGMENT_MIB", 64)?,
            extent_index_cache_bytes: env_mib("EXTENT_BENCH_INDEX_CACHE_MIB", 1024)?,
            extent_index_write_buffer_bytes: env_mib("EXTENT_BENCH_INDEX_WRITE_BUFFER_MIB", 64)?,
            extent_io_read_priority: Duration::from_micros(
                env_optional_u64("EXTENT_BENCH_IO_READ_PRIORITY_US")?.unwrap_or(2_000),
            ),
            extent_high_capacity_percent,
            extent_normal_capacity_percent,
            concurrency,
            put_concurrency,
            shards: env_usize("EXTENT_BENCH_SHARDS", cores.next_power_of_two())?,
            write_concurrency: env_usize("EXTENT_BENCH_EXTENT_WRITE_CONCURRENCY", (cores / 2).clamp(1, 8))?,
            reads,
            read_warmup: env_u64("EXTENT_BENCH_READ_WARMUP", 0)?,
            read_hotset,
            read_pattern: parse_read_pattern()?,
            storage_reads: env_bool("EXTENT_BENCH_STORAGE_READS", false)?,
            direct_io,
            recover_only,
            recover_write_wave: env_bool("EXTENT_BENCH_RECOVER_WRITE_WAVE", false)?,
            populate_only,
            reset: env_bool("EXTENT_BENCH_RESET", !recover_only)?,
        })
    }
}

#[derive(Debug)]
struct Workload {
    entries: u64,
    payload_bytes: u64,
    entry_sizes: Vec<usize>,
    key_sizes: Vec<usize>,
    cycle_bytes: u64,
    priority: PriorityWorkload,
}

impl Workload {
    fn from_env() -> AnyResult<Self> {
        let entry_sizes = env_list_kib("EXTENT_BENCH_ENTRY_KIB", &[4, 16, 64, 256, 1024])?;
        let key_sizes = env_list_usize("EXTENT_BENCH_KEY_BYTES", &[32, 96, 256, 1024])?;
        if key_sizes.iter().any(|size| *size == 0 || *size > MAX_BLOB_KEY_SIZE) {
            return Err(invalid(format!(
                "EXTENT_BENCH_KEY_BYTES values must be in 1..={MAX_BLOB_KEY_SIZE}"
            ))
            .into());
        }
        let cycle_bytes = entry_sizes.iter().try_fold(0u64, |sum, size| {
            sum.checked_add(*size as u64)
                .ok_or_else(|| invalid("entry size pattern overflows u64"))
        })?;
        let requested_entries = env_optional_u64("EXTENT_BENCH_ENTRIES")?;
        let requested_payload = env_optional_usize("EXTENT_BENCH_PAYLOAD_MIB")?
            .map(|mib| checked_mul(mib, MIB, "EXTENT_BENCH_PAYLOAD_MIB"))
            .transpose()?
            .map(|bytes| bytes as u64);
        if requested_entries.is_some() && requested_payload.is_some() {
            return Err(invalid("set only one of EXTENT_BENCH_ENTRIES and EXTENT_BENCH_PAYLOAD_MIB").into());
        }

        let target_payload = requested_payload.unwrap_or(600 * MIB as u64);
        let entries = match requested_entries {
            Some(entries) if entries > 0 => entries,
            Some(_) => return Err(invalid("EXTENT_BENCH_ENTRIES must be positive").into()),
            None => entries_for_payload(target_payload, &entry_sizes, cycle_bytes),
        };
        let payload_bytes = patterned_bytes(entries, &entry_sizes, cycle_bytes);
        let priority = parse_priority_workload()?;
        Ok(Self {
            entries,
            payload_bytes,
            entry_sizes,
            key_sizes,
            cycle_bytes,
            priority,
        })
    }

    fn entry_size(&self, index: u64) -> usize {
        self.entry_sizes[index as usize % self.entry_sizes.len()]
    }

    fn key_size(&self, index: u64) -> usize {
        self.key_sizes[index as usize % self.key_sizes.len()]
    }

    fn maximum_entry_size(&self) -> usize {
        self.entry_sizes.iter().copied().max().unwrap()
    }

    fn wave_entries(&self, wave_bytes: usize) -> u64 {
        ((wave_bytes as u64).saturating_mul(self.entry_sizes.len() as u64) / self.cycle_bytes).max(1)
    }

    fn priority(&self, index: u64) -> CachePriority {
        match self.priority {
            PriorityWorkload::ScopeDb => match index % 10 {
                0 => CachePriority::High,
                1..=3 => CachePriority::Normal,
                _ => CachePriority::Low,
            },
            PriorityWorkload::HistoricalHigh if index < self.entries / 2 => CachePriority::High,
            PriorityWorkload::HistoricalHigh => CachePriority::Normal,
        }
    }
}

#[derive(Debug)]
struct BuiltCache {
    cache: BenchCache,
    extent: Option<ExtentEngineHandle>,
}

#[derive(Debug, Default)]
struct WriteMeasurements {
    operations: u64,
    bytes: u64,
    foreground: Duration,
    drain: Duration,
    latencies: Vec<Duration>,
}

#[derive(Debug, Default)]
struct ReadMeasurements {
    operations: u64,
    hits: u64,
    misses: u64,
    errors: u64,
    invalid: u64,
    hit_bytes: u64,
    hits_by_priority: [u64; 3],
    requests_by_priority: [u64; 3],
    latencies: Vec<Duration>,
    hit_latencies: Vec<Duration>,
    miss_latencies: Vec<Duration>,
    duration: Duration,
}

impl ReadMeasurements {
    fn merge(&mut self, other: Self) {
        self.operations += other.operations;
        self.hits += other.hits;
        self.misses += other.misses;
        self.errors += other.errors;
        self.invalid += other.invalid;
        self.hit_bytes += other.hit_bytes;
        for priority in 0..3 {
            self.hits_by_priority[priority] += other.hits_by_priority[priority];
            self.requests_by_priority[priority] += other.requests_by_priority[priority];
        }
        self.latencies.extend(other.latencies);
        self.hit_latencies.extend(other.hit_latencies);
        self.miss_latencies.extend(other.miss_latencies);
    }
}

#[derive(Debug, Clone, Copy)]
struct IoMeasurements {
    write_bytes: usize,
    write_ios: usize,
    read_bytes: usize,
    read_ios: usize,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> AnyResult<()> {
    if env::var_os("EXTENT_BENCH_PATH").is_none() {
        eprintln!("skipping foyer-engine benchmark: EXTENT_BENCH_PATH must name an explicit real disk");
        return Ok(());
    }
    let workload = Arc::new(Workload::from_env()?);
    let config = Config::from_env(&workload)?;
    fs::create_dir_all(&config.root)?;

    println!(
        "foyer-engine benchmark: path={}, engines={}, entries={}, payload_mib={:.1}, capacity_mib={}, memory_mib={}, entry_kib={}, key_bytes={}, priority_workload={}, read_pattern={}, read_hotset={}, read_warmup={}, read_source={}, concurrency={} (>=2x cores), put_concurrency={}, io={}, extent_read_priority_us={}, extent_priority_floors={}/{}, recover_only={}, recover_write_wave={}, populate_only={}",
        config.root.display(),
        config
            .engines
            .iter()
            .map(|engine| engine.label())
            .collect::<Vec<_>>()
            .join(","),
        workload.entries,
        as_mib(workload.payload_bytes),
        config.capacity_bytes / MIB,
        config.memory_bytes / MIB,
        join_sizes(&workload.entry_sizes, KIB),
        join_sizes(&workload.key_sizes, 1),
        workload.priority.label(),
        config.read_pattern.label(),
        config.read_hotset,
        config.read_warmup,
        if config.storage_reads { "storage" } else { "hybrid" },
        config.concurrency,
        config.put_concurrency,
        if config.direct_io { "direct" } else { "buffered" },
        config.extent_io_read_priority.as_micros(),
        config.extent_high_capacity_percent,
        config.extent_normal_capacity_percent,
        config.recover_only,
        config.recover_write_wave,
        config.populate_only,
    );

    for engine in config.engines.iter().copied() {
        run_engine(engine, &config, workload.clone()).await?;
    }
    Ok(())
}

async fn run_engine(engine: DiskEngine, config: &Config, workload: Arc<Workload>) -> AnyResult<()> {
    let path = config.root.join(engine.label());
    println!("\nengine={} path={}", engine.label(), path.display());

    if config.recover_only {
        recover_and_read(engine, config, workload, &path).await?;
        return Ok(());
    }
    if config.reset && path.exists() {
        fs::remove_dir_all(&path)?;
    }
    fs::create_dir_all(&path)?;

    let opened_at = Instant::now();
    let built = build_cache(engine, config, &path, RecoverMode::None).await?;
    println!(
        "engine={} phase=open_fresh seconds={:.3}",
        engine.label(),
        opened_at.elapsed().as_secs_f64()
    );

    let write = run_writes(&built.cache, config, workload.clone()).await?;
    let io = io_measurements(&built.cache);
    let elapsed = write.foreground + write.drain;
    println!(
        "engine={} phase=write operations={} logical_mib={:.1} foreground_seconds={:.3} drain_seconds={:.3} end_to_end_seconds={:.3} foreground_mib_s={:.1} end_to_end_mib_s={:.1} disk_write_mib={:.1} write_amp={:.3} disk_write_ios={} peak_rss_mib={}",
        engine.label(),
        write.operations,
        as_mib(write.bytes),
        write.foreground.as_secs_f64(),
        write.drain.as_secs_f64(),
        elapsed.as_secs_f64(),
        throughput_mib(write.bytes, write.foreground),
        throughput_mib(write.bytes, elapsed),
        as_mib(io.write_bytes as u64),
        ratio(io.write_bytes as u64, write.bytes),
        io.write_ios,
        peak_rss_mib()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
    );
    print_latencies(engine, "put_foreground", &write.latencies);
    print_extent_write_stats(engine, &built.extent)?;

    built.cache.close().await?;
    drop(built);
    let (logical, allocated) = directory_sizes(&path)?;
    println!(
        "engine={} phase=footprint logical_mib={:.1} allocated_mib={:.1}",
        engine.label(),
        as_mib(logical),
        as_mib(allocated),
    );

    if config.populate_only {
        return Ok(());
    }

    recover_and_read(engine, config, workload, &path).await
}

async fn recover_and_read(engine: DiskEngine, config: &Config, workload: Arc<Workload>, path: &Path) -> AnyResult<()> {
    let recovery_started = Instant::now();
    let recovered = build_cache(engine, config, path, RecoverMode::Strict).await?;
    let recovery = recovery_started.elapsed();
    println!(
        "engine={} phase=recovery seconds={:.3} entries={} rate_entries_s={:.0} peak_rss_mib={}",
        engine.label(),
        recovery.as_secs_f64(),
        workload.entries,
        workload.entries as f64 / recovery.as_secs_f64().max(f64::EPSILON),
        peak_rss_mib()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
    );

    if config.read_warmup > 0 {
        let warmup = run_reads(
            &recovered.cache,
            workload.clone(),
            config.read_warmup,
            config.concurrency,
            config.read_pattern,
            config.read_hotset,
            config.storage_reads,
        )
        .await?;
        println!(
            "engine={} phase=read_warmup operations={} hits={} misses={} errors={} invalid={} seconds={:.3} ops_s={:.0}",
            engine.label(),
            warmup.operations,
            warmup.hits,
            warmup.misses,
            warmup.errors,
            warmup.invalid,
            warmup.duration.as_secs_f64(),
            warmup.operations as f64 / warmup.duration.as_secs_f64().max(f64::EPSILON),
        );
    }

    let reads = run_reads(
        &recovered.cache,
        workload.clone(),
        config.reads,
        config.concurrency,
        config.read_pattern,
        config.read_hotset,
        config.storage_reads,
    )
    .await?;
    let io = io_measurements(&recovered.cache);
    println!(
        "engine={} phase=read operations={} hits={} misses={} errors={} invalid={} hit_ratio={:.3} seconds={:.3} ops_s={:.0} hit_mib_s={:.1} disk_read_mib={:.1} disk_read_ios={}",
        engine.label(),
        reads.operations,
        reads.hits,
        reads.misses,
        reads.errors,
        reads.invalid,
        ratio(reads.hits, reads.operations),
        reads.duration.as_secs_f64(),
        reads.operations as f64 / reads.duration.as_secs_f64().max(f64::EPSILON),
        throughput_mib(reads.hit_bytes, reads.duration),
        as_mib(io.read_bytes as u64),
        io.read_ios,
    );
    for priority in [CachePriority::Low, CachePriority::Normal, CachePriority::High] {
        let index = priority.to_byte() as usize;
        println!(
            "engine={} phase=retention priority={:?} requests={} hits={} hit_ratio={:.3}",
            engine.label(),
            priority,
            reads.requests_by_priority[index],
            reads.hits_by_priority[index],
            ratio(reads.hits_by_priority[index], reads.requests_by_priority[index]),
        );
    }
    print_latencies(engine, "get", &reads.latencies);
    print_latencies(engine, "get_hit", &reads.hit_latencies);
    print_latencies(engine, "get_miss", &reads.miss_latencies);
    print_extent_read_stats(engine, &recovered.extent);

    if !config.recover_only || config.recover_write_wave {
        let before_burst = run_reads(
            &recovered.cache,
            workload.clone(),
            config.reads,
            config.concurrency,
            config.read_pattern,
            config.read_hotset,
            config.storage_reads,
        )
        .await?;
        println!(
            "engine={} phase=read_before_write_burst operations={} hits={} misses={} errors={} invalid={} seconds={:.3} ops_s={:.0}",
            engine.label(),
            before_burst.operations,
            before_burst.hits,
            before_burst.misses,
            before_burst.errors,
            before_burst.invalid,
            before_burst.duration.as_secs_f64(),
            before_burst.operations as f64 / before_burst.duration.as_secs_f64().max(f64::EPSILON),
        );
        print_latencies(engine, "get_before_write_burst", &before_burst.latencies);
        print_latencies(engine, "get_hit_before_write_burst", &before_burst.hit_latencies);
        print_latencies(engine, "get_miss_before_write_burst", &before_burst.miss_latencies);

        let io_before = io_measurements(&recovered.cache);
        let (burst, under_burst) = run_read_under_write_burst(&recovered.cache, config, workload).await?;
        let io = io_delta(io_measurements(&recovered.cache), io_before);
        println!(
            "engine={} phase=read_under_write_burst read_operations={} hits={} misses={} errors={} invalid={} read_seconds={:.3} read_ops_s={:.0} burst_operations={} burst_mib={:.1} burst_foreground_seconds={:.3} burst_drain_seconds={:.3} disk_read_mib={:.1} disk_write_mib={:.1} hit_p99_inflation={:.3}",
            engine.label(),
            under_burst.operations,
            under_burst.hits,
            under_burst.misses,
            under_burst.errors,
            under_burst.invalid,
            under_burst.duration.as_secs_f64(),
            under_burst.operations as f64 / under_burst.duration.as_secs_f64().max(f64::EPSILON),
            burst.operations,
            as_mib(burst.bytes),
            burst.foreground.as_secs_f64(),
            burst.drain.as_secs_f64(),
            as_mib(io.read_bytes as u64),
            as_mib(io.write_bytes as u64),
            duration_ratio(
                quantile(&under_burst.hit_latencies, 990, 1_000),
                quantile(&before_burst.hit_latencies, 990, 1_000),
            ),
        );
        print_latencies(engine, "get_under_write_burst", &under_burst.latencies);
        print_latencies(engine, "get_hit_under_write_burst", &under_burst.hit_latencies);
        print_latencies(engine, "get_miss_under_write_burst", &under_burst.miss_latencies);
        print_latencies(engine, "put_burst_foreground", &burst.latencies);
        print_extent_write_stats(engine, &recovered.extent)?;
    }
    recovered.cache.close().await?;
    Ok(())
}

async fn build_cache(
    engine: DiskEngine,
    config: &Config,
    path: &Path,
    recover_mode: RecoverMode,
) -> AnyResult<BuiltCache> {
    let (engine_config, extent): (
        Box<dyn EngineConfig<Bytes, EngineValue, HybridCacheProperties>>,
        Option<ExtentEngineHandle>,
    ) = match engine {
        DiskEngine::Block => {
            #[cfg(target_os = "linux")]
            let device = FsDeviceBuilder::new(path.join("block-device"))
                .with_capacity(config.capacity_bytes)
                .with_direct(config.direct_io)
                .build()?;
            #[cfg(not(target_os = "linux"))]
            let device = FsDeviceBuilder::new(path.join("block-device"))
                .with_capacity(config.capacity_bytes)
                .build()?;
            let blocks = config.capacity_bytes / config.block_size_bytes;
            let clean_blocks = (blocks / 20).max(1);
            // Every flusher owns an active block. Keep enough blocks stable and clean even in
            // reduced-capacity smoke tests; production-scale devices are normally CPU-bound here.
            let flushers = config.write_concurrency.min((blocks / 4).max(1));
            let block = BlockEngineConfig::new(device)
                .with_io_engine_config(PsyncIoEngineConfig::new())
                .with_block_size(config.block_size_bytes)
                .with_indexer_shards(config.shards)
                .with_recover_concurrency(config.concurrency)
                .with_flushers(flushers)
                .with_reclaimers(config.write_concurrency.max(1))
                .with_buffer_pool_size(config.block_buffer_pool_bytes)
                .with_submit_queue_size_threshold(config.queue_bytes)
                .with_clean_block_threshold(clean_blocks)
                .with_tombstone_log(true);
            (Box::new(block), None)
        }
        DiskEngine::Extent => {
            let queue_entries = (config.queue_bytes / (4 * KIB)).max(1);
            let extent = ExtentEngineConfig::new(path.join("extent-engine"), config.capacity_bytes as u64)
                .with_slot_size(config.extent_slot_size)
                .with_segment_size(config.extent_segment_size)
                .with_write_concurrency(config.write_concurrency)
                .with_io_read_priority_duration(config.extent_io_read_priority)
                .with_index_cache_size(config.extent_index_cache_bytes)
                .with_index_write_buffer_size(config.extent_index_write_buffer_bytes)
                .with_priority_capacity_floors(
                    config.extent_high_capacity_percent,
                    config.extent_normal_capacity_percent,
                )
                .with_direct_io(config.direct_io)
                .with_queue_capacity_bytes(config.queue_bytes)
                .with_queue_capacity_entries(queue_entries)
                .with_write_batch_bytes((128 * MIB).min(config.queue_bytes))
                .with_write_batch_entries(4_096.min(queue_entries));
            let handle = extent.handle();
            (Box::new(extent), Some(handle))
        }
    };

    let cache = HybridCache::builder()
        .with_name(format!("{}-engine-benchmark", engine.label()))
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .with_flush_on_close(false)
        .memory(config.memory_bytes)
        .with_shards(config.shards)
        .with_eviction_config(S3FifoConfig::default())
        .with_weighter(|key: &Bytes, value: &EngineValue| key.len() + value.value().len() + ENTRY_OVERHEAD)
        .storage()
        .with_engine_config(engine_config)
        .with_recover_mode(recover_mode)
        .with_compression(Compression::None)
        .build()
        .await?;
    Ok(BuiltCache { cache, extent })
}

async fn run_writes(cache: &BenchCache, config: &Config, workload: Arc<Workload>) -> AnyResult<WriteMeasurements> {
    let wave_entries = workload.wave_entries(config.wave_bytes);
    let sample_stride = (workload.entries / LATENCY_SAMPLE_TARGET).max(1);
    let mut measurements = WriteMeasurements::default();
    let mut start = 0u64;
    let progress_stride = (workload.entries / 20).max(1);
    let mut next_progress = progress_stride;

    while start < workload.entries {
        let end = start.saturating_add(wave_entries).min(workload.entries);
        let worker_count = config.put_concurrency.min((end - start) as usize).max(1);
        let wave_started = Instant::now();
        let mut workers = Vec::with_capacity(worker_count);
        for worker in 0..worker_count {
            let cache = cache.clone();
            let workload = workload.clone();
            workers.push(tokio::task::spawn_blocking(move || {
                let mut result = WriteMeasurements::default();
                let mut index = start + worker as u64;
                while index < end {
                    let key = make_key(index, workload.key_size(index));
                    let value = make_value(index, workload.entry_size(index));
                    let priority = workload.priority(index);
                    let submitted = Instant::now();
                    cache.insert_with_properties(
                        key,
                        EngineValue::new(value, priority).expect("benchmark values must be non-empty"),
                        properties(priority),
                    );
                    if index.is_multiple_of(sample_stride) {
                        result.latencies.push(submitted.elapsed());
                    }
                    result.operations += 1;
                    result.bytes += workload.entry_size(index) as u64;
                    index += worker_count as u64;
                }
                result
            }));
        }
        for worker in workers {
            let result = worker.await?;
            measurements.operations += result.operations;
            measurements.bytes += result.bytes;
            measurements.latencies.extend(result.latencies);
        }
        measurements.foreground += wave_started.elapsed();

        let drain_started = Instant::now();
        cache.storage().wait().await;
        measurements.drain += drain_started.elapsed();
        start = end;

        if start >= next_progress || start == workload.entries {
            println!(
                "phase=write_progress entries={}/{} percent={:.1}",
                start,
                workload.entries,
                start as f64 * 100.0 / workload.entries as f64,
            );
            next_progress = next_progress.saturating_add(progress_stride);
        }
    }
    Ok(measurements)
}

async fn run_reads(
    cache: &BenchCache,
    workload: Arc<Workload>,
    reads: u64,
    concurrency: usize,
    pattern: ReadPattern,
    hotset: u64,
    storage_reads: bool,
) -> AnyResult<ReadMeasurements> {
    let sample_stride = (reads / LATENCY_SAMPLE_TARGET).max(1);
    let started = Instant::now();
    let worker_count = concurrency.min(reads as usize).max(1);
    let mut workers = Vec::with_capacity(worker_count);
    for worker in 0..worker_count {
        let cache = cache.clone();
        let workload = workload.clone();
        workers.push(tokio::spawn(async move {
            let mut result = ReadMeasurements::default();
            let mut operation = worker as u64;
            while operation < reads {
                let index = match pattern {
                    ReadPattern::Random => mix64(operation ^ 0x9e37_79b9_7f4a_7c15) % hotset,
                    ReadPattern::Sequential => operation % hotset,
                };
                let priority = workload.priority(index);
                result.requests_by_priority[priority.to_byte() as usize] += 1;
                let key = make_key(index, workload.key_size(index));
                let requested = Instant::now();
                let outcome = if storage_reads {
                    match cache.storage().load(&key).await {
                        Ok(Load::Entry {
                            key: loaded_key, value, ..
                        }) => {
                            result.hits += 1;
                            result.hits_by_priority[priority.to_byte() as usize] += 1;
                            result.hit_bytes += value.value().len() as u64;
                            if loaded_key != key
                                || value.priority() != priority
                                || !validate_value(index, workload.entry_size(index), value.value())
                            {
                                result.invalid += 1;
                            }
                            ReadOutcome::Hit
                        }
                        Ok(Load::Piece { piece, .. }) => {
                            result.hits += 1;
                            result.hits_by_priority[priority.to_byte() as usize] += 1;
                            result.hit_bytes += piece.value().value().len() as u64;
                            if piece.key() != &key
                                || piece.value().priority() != priority
                                || !validate_value(index, workload.entry_size(index), piece.value().value())
                            {
                                result.invalid += 1;
                            }
                            ReadOutcome::Hit
                        }
                        Ok(Load::Miss) => {
                            result.misses += 1;
                            ReadOutcome::Miss
                        }
                        Ok(Load::Throttled) | Err(_) => {
                            result.errors += 1;
                            ReadOutcome::Error
                        }
                    }
                } else {
                    match cache.get(&key).await {
                        Ok(Some(entry)) => {
                            result.hits += 1;
                            result.hits_by_priority[priority.to_byte() as usize] += 1;
                            result.hit_bytes += entry.value().value().len() as u64;
                            if entry.key() != &key
                                || entry.value().priority() != priority
                                || !validate_value(index, workload.entry_size(index), entry.value().value())
                            {
                                result.invalid += 1;
                            }
                            ReadOutcome::Hit
                        }
                        Ok(None) => {
                            result.misses += 1;
                            ReadOutcome::Miss
                        }
                        Err(_) => {
                            result.errors += 1;
                            ReadOutcome::Error
                        }
                    }
                };
                if operation.is_multiple_of(sample_stride) {
                    let latency = requested.elapsed();
                    result.latencies.push(latency);
                    match outcome {
                        ReadOutcome::Hit => result.hit_latencies.push(latency),
                        ReadOutcome::Miss => result.miss_latencies.push(latency),
                        ReadOutcome::Error => {}
                    }
                }
                result.operations += 1;
                operation += worker_count as u64;
            }
            result
        }));
    }

    let mut measurements = ReadMeasurements::default();
    for worker in workers {
        measurements.merge(worker.await?);
    }
    measurements.duration = started.elapsed();
    Ok(measurements)
}

#[derive(Debug, Clone, Copy)]
enum ReadOutcome {
    Hit,
    Miss,
    Error,
}

async fn run_read_under_write_burst(
    cache: &BenchCache,
    config: &Config,
    workload: Arc<Workload>,
) -> AnyResult<(WriteMeasurements, ReadMeasurements)> {
    let burst_entries = workload.wave_entries(config.wave_bytes);
    let sample_stride = (burst_entries / LATENCY_SAMPLE_TARGET).max(1);
    let writer_cache = cache.clone();
    let writer_workload = workload.clone();
    let started = Arc::new(tokio::sync::Notify::new());
    let writer_started = started.clone();
    let writer = tokio::task::spawn_blocking(move || {
        let foreground = Instant::now();
        let mut measurements = WriteMeasurements::default();
        for offset in 0..burst_entries {
            let index = writer_workload.entries.saturating_add(offset);
            let key = make_key(index, writer_workload.key_size(index));
            let value = make_value(index, writer_workload.entry_size(index));
            let priority = writer_workload.priority(index);
            let submitted = Instant::now();
            writer_cache.insert_with_properties(
                key,
                EngineValue::new(value, priority).expect("benchmark values must be non-empty"),
                properties(priority),
            );
            if offset.is_multiple_of(sample_stride) {
                measurements.latencies.push(submitted.elapsed());
            }
            measurements.operations += 1;
            measurements.bytes += writer_workload.entry_size(index) as u64;
            if offset == 0 {
                writer_started.notify_one();
            }
        }
        measurements.foreground = foreground.elapsed();
        measurements
    });

    started.notified().await;
    let reads = run_reads(
        cache,
        workload,
        config.reads,
        config.concurrency,
        config.read_pattern,
        config.read_hotset,
        config.storage_reads,
    )
    .await?;
    let mut writes = writer.await?;
    let drain = Instant::now();
    cache.storage().wait().await;
    writes.drain = drain.elapsed();
    Ok((writes, reads))
}

fn make_key(index: u64, len: usize) -> Bytes {
    let mut key = vec![0u8; len];
    let encoded = index.to_le_bytes();
    let prefix = encoded.len().min(len);
    key[..prefix].copy_from_slice(&encoded[..prefix]);
    for (offset, byte) in key[prefix..].iter_mut().enumerate() {
        *byte = mix64(index.wrapping_add(offset as u64)) as u8;
    }
    Bytes::from(key)
}

fn make_value(index: u64, len: usize) -> Bytes {
    let mut value = vec![index as u8; len];
    value[..8].copy_from_slice(&index.to_le_bytes());
    value[8..16].copy_from_slice(&(len as u64).to_le_bytes());
    Bytes::from(value)
}

fn validate_value(index: u64, len: usize, value: &Bytes) -> bool {
    value.len() == len
        && value.get(..8) == Some(index.to_le_bytes().as_slice())
        && value.get(8..16) == Some((len as u64).to_le_bytes().as_slice())
        && value.get(16).copied() == Some(index as u8)
}

fn properties(priority: CachePriority) -> HybridCacheProperties {
    let hint = match priority {
        CachePriority::Low => Hint::Low,
        CachePriority::Normal | CachePriority::High => Hint::Normal,
    };
    HybridCacheProperties::default().with_hint(hint)
}

const fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn print_latencies(engine: DiskEngine, operation: &str, latencies: &[Duration]) {
    println!(
        "engine={} phase=latency operation={} samples={} p50_us={} p95_us={} p99_us={} p999_us={} max_us={}",
        engine.label(),
        operation,
        latencies.len(),
        quantile(latencies, 500, 1_000).as_micros(),
        quantile(latencies, 950, 1_000).as_micros(),
        quantile(latencies, 990, 1_000).as_micros(),
        quantile(latencies, 999, 1_000).as_micros(),
        latencies.iter().copied().max().unwrap_or_default().as_micros(),
    );
}

fn quantile(values: &[Duration], numerator: usize, denominator: usize) -> Duration {
    if values.is_empty() {
        return Duration::ZERO;
    }
    let mut values = values.to_vec();
    values.sort_unstable();
    let index = (values.len() - 1).saturating_mul(numerator) / denominator;
    values[index]
}

fn duration_ratio(value: Duration, baseline: Duration) -> f64 {
    value.as_secs_f64() / baseline.as_secs_f64().max(f64::EPSILON)
}

fn io_delta(after: IoMeasurements, before: IoMeasurements) -> IoMeasurements {
    IoMeasurements {
        write_bytes: after.write_bytes.saturating_sub(before.write_bytes),
        write_ios: after.write_ios.saturating_sub(before.write_ios),
        read_bytes: after.read_bytes.saturating_sub(before.read_bytes),
        read_ios: after.read_ios.saturating_sub(before.read_ios),
    }
}

fn print_extent_write_stats(engine: DiskEngine, handle: &Option<ExtentEngineHandle>) -> AnyResult<()> {
    let Some(handle) = handle else {
        return Ok(());
    };
    if let Some(stats) = handle.physical_write_stats() {
        println!(
            "engine={} phase=extent_write physical_mib={:.1} physical_runs={} data_mib={:.1} owner_mib={:.1} index_mib={:.1} allocator_mib={:.1}",
            engine.label(),
            as_mib(stats.total_bytes()),
            stats.total_runs(),
            as_mib(stats.data_bytes),
            as_mib(stats.owner_bytes),
            as_mib(stats.index_bytes),
            as_mib(stats.allocator_bytes),
        );
    }
    if let Some(stats) = handle.io_scheduler_stats() {
        println!(
            "engine={} phase=extent_io_scheduler enabled={} read_priority_us={} write_operations={} read_priority_waits={} write_limit_waits={} total_write_wait_ms={:.3} maximum_write_wait_us={} active_reads={} active_writes={} waiting_writes={}",
            engine.label(),
            stats.enabled(),
            stats.read_priority_duration.as_micros(),
            stats.write_operations,
            stats.read_priority_waits,
            stats.write_limit_waits,
            stats.total_write_wait.as_secs_f64() * 1_000.0,
            stats.maximum_write_wait.as_micros(),
            stats.active_reads,
            stats.active_writes,
            stats.waiting_writes,
        );
    }
    if let Some(index) = handle.index_stats() {
        println!(
            "engine={} phase=extent_index live_entries={} wal_mib={:.1} sst_files={} sst_mib={:.1} cache_resident_mib={:.1}",
            engine.label(),
            index.live_entries,
            as_mib(index.wal_bytes),
            index.sst_files,
            as_mib(index.sst_bytes),
            as_mib(index.cache_resident_bytes),
        );
    }
    if let Some(occupancy) = handle.priority_occupancy() {
        println!(
            "engine={} phase=extent_priority usable_segments={} high_segments={} high_floor={} high_borrowed={} high_mib={:.1} normal_segments={} normal_floor={} normal_borrowed={} normal_mib={:.1} low_segments={} low_mib={:.1}",
            engine.label(),
            occupancy.usable_segments(),
            occupancy.occupied_segments(CachePriority::High),
            occupancy.capacity_floor_segments(CachePriority::High),
            occupancy.borrowed_segments(CachePriority::High),
            as_mib(occupancy.used_bytes(CachePriority::High)),
            occupancy.occupied_segments(CachePriority::Normal),
            occupancy.capacity_floor_segments(CachePriority::Normal),
            occupancy.borrowed_segments(CachePriority::Normal),
            as_mib(occupancy.used_bytes(CachePriority::Normal)),
            occupancy.occupied_segments(CachePriority::Low),
            as_mib(occupancy.used_bytes(CachePriority::Low)),
        );
    }
    if let Some(writes) = handle.write_stats() {
        println!(
            "engine={} phase=extent_pipeline accepted={} dropped={} shutdown_dropped={} shed_low={} shed_normal={} shed_high={} completed={} storage_rejected={} completed_batches={} failed_batches={}",
            engine.label(),
            writes.accepted_commands,
            writes.dropped_commands,
            writes.shutdown_dropped_commands,
            writes.shed_low_commands,
            writes.shed_normal_commands,
            writes.shed_high_commands,
            writes.completed_commands,
            writes.storage_rejected_puts,
            writes.completed_batches,
            writes.failed_batches,
        );
        if writes.storage_rejected_puts > 0
            || writes.failed_batches > 0
            || writes.completed_commands != writes.accepted_commands
        {
            return Err(io::Error::other("Extent benchmark did not process every accepted write").into());
        }
    }
    if let Some(checkpoint) = handle.checkpoint_stats() {
        println!(
            "engine={} phase=extent_checkpoint published={} requested={} durable={} in_flight={} dirty_changes={} failed={}",
            engine.label(),
            checkpoint.published_epoch,
            checkpoint.requested_epoch,
            checkpoint.durable_epoch,
            checkpoint
                .in_flight_epoch
                .map_or_else(|| "none".to_string(), |epoch| epoch.to_string()),
            checkpoint.dirty_changes,
            checkpoint.failed,
        );
        if checkpoint.failed
            || checkpoint.in_flight_epoch.is_some()
            || checkpoint.durable_epoch < checkpoint.published_epoch
            || checkpoint.dirty_changes > 0
        {
            return Err(io::Error::other("Extent benchmark ended behind its recovery frontier").into());
        }
    }
    if let Some(error) = handle.background_error() {
        return Err(io::Error::other(format!("Extent benchmark background failure: {error}")).into());
    }
    Ok(())
}

fn print_extent_read_stats(engine: DiskEngine, handle: &Option<ExtentEngineHandle>) {
    let Some(handle) = handle else {
        return;
    };
    if let Some(read) = handle.read_stats() {
        println!(
            "engine={} phase=extent_read calls={} data_slots={} data_runs={} data_mib={:.1}",
            engine.label(),
            read.calls,
            read.data_slots,
            read.data_runs,
            as_mib(read.data_bytes),
        );
    }
    if let Some(index) = handle.index_read_stats() {
        println!(
            "engine={} phase=extent_index_read cache_hits={} cache_misses={} read_ops={} read_mib={:.1} filter_checks={} filter_positives={} false_positives={} data_reads={}",
            engine.label(),
            index.cache_hits,
            index.cache_misses,
            index.read_operations,
            as_mib(index.read_bytes),
            index.filter_checks,
            index.filter_positives,
            index.false_positives,
            index.data_reads,
        );
    }
}

fn io_measurements(cache: &BenchCache) -> IoMeasurements {
    let stats = cache.statistics();
    IoMeasurements {
        write_bytes: stats.disk_write_bytes(),
        write_ios: stats.disk_write_ios(),
        read_bytes: stats.disk_read_bytes(),
        read_ios: stats.disk_read_ios(),
    }
}

fn entries_for_payload(target: u64, sizes: &[usize], cycle_bytes: u64) -> u64 {
    let cycles = target / cycle_bytes;
    let mut entries = cycles.saturating_mul(sizes.len() as u64);
    let mut bytes = cycles.saturating_mul(cycle_bytes);
    let mut position = 0usize;
    while bytes < target {
        bytes = bytes.saturating_add(sizes[position] as u64);
        entries += 1;
        position = (position + 1) % sizes.len();
    }
    entries.max(1)
}

fn patterned_bytes(entries: u64, sizes: &[usize], cycle_bytes: u64) -> u64 {
    let cycles = entries / sizes.len() as u64;
    let remainder = entries % sizes.len() as u64;
    cycles
        .saturating_mul(cycle_bytes)
        .saturating_add(sizes[..remainder as usize].iter().map(|size| *size as u64).sum::<u64>())
}

fn directory_sizes(path: &Path) -> io::Result<(u64, u64)> {
    let mut logical = 0u64;
    let mut allocated = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            let child = directory_sizes(&entry.path())?;
            logical = logical.saturating_add(child.0);
            allocated = allocated.saturating_add(child.1);
        } else if metadata.is_file() {
            logical = logical.saturating_add(metadata.len());
            allocated = allocated.saturating_add(allocated_bytes(&metadata));
        }
    }
    Ok((logical, allocated))
}

#[cfg(unix)]
fn allocated_bytes(metadata: &fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;

    metadata.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
fn allocated_bytes(metadata: &fs::Metadata) -> u64 {
    metadata.len()
}

#[cfg(target_os = "linux")]
fn peak_rss_mib() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
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

fn parse_engines() -> AnyResult<Vec<DiskEngine>> {
    let value = env::var("EXTENT_BENCH_ENGINES").unwrap_or_else(|_| "block,extent".to_string());
    let mut engines = Vec::new();
    for item in value.split(',').map(str::trim).filter(|item| !item.is_empty()) {
        let engine = match item {
            "block" | "foyer" => DiskEngine::Block,
            "extent" => DiskEngine::Extent,
            _ => return Err(invalid("EXTENT_BENCH_ENGINES accepts block and extent").into()),
        };
        if !engines.contains(&engine) {
            engines.push(engine);
        }
    }
    if engines.is_empty() {
        return Err(invalid("EXTENT_BENCH_ENGINES must not be empty").into());
    }
    Ok(engines)
}

fn parse_priority_workload() -> AnyResult<PriorityWorkload> {
    match env::var("EXTENT_BENCH_PRIORITY_WORKLOAD") {
        Ok(value) if value == "scopedb" => Ok(PriorityWorkload::ScopeDb),
        Ok(value) if value == "historical-high" => Ok(PriorityWorkload::HistoricalHigh),
        Ok(_) => Err(invalid("EXTENT_BENCH_PRIORITY_WORKLOAD accepts scopedb and historical-high").into()),
        Err(env::VarError::NotPresent) => Ok(PriorityWorkload::ScopeDb),
        Err(error) => Err(error.into()),
    }
}

fn parse_read_pattern() -> AnyResult<ReadPattern> {
    match env::var("EXTENT_BENCH_READ_PATTERN") {
        Ok(value) if value == "random" => Ok(ReadPattern::Random),
        Ok(value) if value == "sequential" => Ok(ReadPattern::Sequential),
        Ok(_) => Err(invalid("EXTENT_BENCH_READ_PATTERN accepts random and sequential").into()),
        Err(env::VarError::NotPresent) => Ok(ReadPattern::Random),
        Err(error) => Err(error.into()),
    }
}

fn env_list_kib(name: &str, default: &[usize]) -> AnyResult<Vec<usize>> {
    env_list_usize(name, default)?
        .into_iter()
        .map(|value| checked_mul(value, KIB, name).map_err(Into::into))
        .collect()
}

fn env_list_usize(name: &str, default: &[usize]) -> AnyResult<Vec<usize>> {
    let Some(value) = env::var_os(name) else {
        return Ok(default.to_vec());
    };
    let value = value
        .into_string()
        .map_err(|_| invalid(format!("{name} must be valid UTF-8")))?;
    let values = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| invalid(format!("{name} must be a comma-separated integer list")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() || values.contains(&0) {
        return Err(invalid(format!("{name} values must be positive")).into());
    }
    Ok(values)
}

fn env_kib(name: &str, default: usize) -> AnyResult<usize> {
    checked_mul(env_usize(name, default)?, KIB, name).map_err(Into::into)
}

fn env_mib(name: &str, default: usize) -> AnyResult<usize> {
    checked_mul(env_usize(name, default)?, MIB, name).map_err(Into::into)
}

fn env_usize(name: &str, default: usize) -> AnyResult<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .map_err(|_| invalid(format!("{name} must be a positive integer")))
            .and_then(|value| {
                if value == 0 {
                    Err(invalid(format!("{name} must be positive")))
                } else {
                    Ok(value)
                }
            })
            .map_err(Into::into),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn env_percent(name: &str, default: u8) -> AnyResult<u8> {
    match env::var(name) {
        Ok(value) => value
            .parse::<u8>()
            .map_err(|_| invalid(format!("{name} must be an integer between 0 and 100")))
            .and_then(|value| {
                if value <= 100 {
                    Ok(value)
                } else {
                    Err(invalid(format!("{name} must be at most 100")))
                }
            })
            .map_err(Into::into),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn env_u64(name: &str, default: u64) -> AnyResult<u64> {
    match env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .map_err(|_| invalid(format!("{name} must be a positive integer")))
            .and_then(|value| {
                if value == 0 {
                    Err(invalid(format!("{name} must be positive")))
                } else {
                    Ok(value)
                }
            })
            .map_err(Into::into),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn env_optional_usize(name: &str) -> AnyResult<Option<usize>> {
    match env::var(name) {
        Ok(value) => {
            Ok(Some(value.parse::<usize>().map_err(|_| {
                invalid(format!("{name} must be a non-negative integer"))
            })?))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn env_optional_u64(name: &str) -> AnyResult<Option<u64>> {
    match env::var(name) {
        Ok(value) => {
            Ok(Some(value.parse::<u64>().map_err(|_| {
                invalid(format!("{name} must be a non-negative integer"))
            })?))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn env_bool(name: &str, default: bool) -> AnyResult<bool> {
    match env::var(name) {
        Ok(value) => match value.as_str() {
            "1" | "true" | "yes" => Ok(true),
            "0" | "false" | "no" => Ok(false),
            _ => Err(invalid(format!("{name} must be true/false or 1/0")).into()),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn checked_mul(value: usize, unit: usize, name: &str) -> io::Result<usize> {
    value
        .checked_mul(unit)
        .ok_or_else(|| invalid(format!("{name} overflows usize")))
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn as_mib(bytes: u64) -> f64 {
    bytes as f64 / MIB as f64
}

fn throughput_mib(bytes: u64, duration: Duration) -> f64 {
    as_mib(bytes) / duration.as_secs_f64().max(f64::EPSILON)
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    numerator as f64 / denominator.max(1) as f64
}

fn join_sizes(values: &[usize], divisor: usize) -> String {
    values
        .iter()
        .map(|value| (value / divisor).to_string())
        .collect::<Vec<_>>()
        .join(",")
}
