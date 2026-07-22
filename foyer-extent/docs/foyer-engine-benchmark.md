# Foyer engine benchmark

`foyer_engine_compare` compares BlockEngine and ExtentEngine below the same Foyer HybridCache. It
does not depend on ScopeDB. Both sides use `Bytes` keys, `EngineValue`, S3FIFO, write-on-insertion,
the same memory capacity and shards, the same seeded randomized workload, and the same concurrency.
The production boundary being exercised is specified in
[Foyer integration design](foyer-integration.md); this document defines validation procedure rather
than engine behavior.

The scenario generator is counter-based: a seed and operation number always produce the same
request regardless of task scheduling. Entry size, key size, ScopeDB priority, key content, value
content, write order, and random read selection use independent streams. Writes use an
allocation-free random permutation, so every offered key is visited exactly once rather than being
sampled with replacement. The benchmark prints the decimal and hexadecimal seed on its first line.
`EXTENT_BENCH_SEED` accepts a decimal or `0x`-prefixed `u64`; use at least three unrelated seeds for
an acceptance result and retain every seed with its log. The default seed is fixed so a failure is
reproducible without extra configuration.

Every fresh image stores a scenario manifest next to the engine directory. Recover-only runs reject
a missing or mismatched manifest, including a different seed, size distribution, observed size
quantiles, priority workload, or scenario-generator version. Images created by an older benchmark
must therefore be repopulated.

`EXTENT_BENCH_REWRITE_PASSES` adds measured same-value rewrite passes after the initial population.
Each pass visits every existing key exactly once through an independent seeded random permutation,
waits for the storage queue at the same byte-wave boundaries as initial population, and reports
foreground latency, end-to-end throughput, and physical read/write I/O separately. This isolates
idempotent-update overhead without changing the recovered key/value set. Keep the default of zero
for capacity and recovery comparisons; use at least three unrelated seeds and alternate candidate
and baseline run order when using rewrite passes as a regression gate.

The benchmark refuses to run without `EXTENT_BENCH_PATH`; point it at a real SSD directory, never a
tmpfs. It deletes only the selected `block` and `extent` children when reset is enabled.

## Smoke test

```shell
EXTENT_BENCH_PATH=/path/on/ssd/extent-smoke \
EXTENT_BENCH_CAPACITY_MIB=512 \
EXTENT_BENCH_PAYLOAD_MIB=600 \
cargo bench -p foyer-extent --bench foyer_engine_compare
```

For a three-seed comparison, use separate roots so every seed owns an independently populated
image:

```shell
for seed in 0x243f6a8885a308d3 0x13198a2e03707344 0xa4093822299f31d0; do
  EXTENT_BENCH_SEED="$seed" \
  EXTENT_BENCH_PATH="/path/on/ssd/extent-${seed}" \
  EXTENT_BENCH_CAPACITY_MIB=512 \
  EXTENT_BENCH_PAYLOAD_MIB=600 \
  cargo bench -p foyer-extent --bench foyer_engine_compare
done
```

The default `uniform-list` entry distribution uses 4, 16, 64, 256, and 1024 KiB. Configured size
choices are selected uniformly by independent seeded streams instead of cycling in a fixed order.
Set `EXTENT_BENCH_ENTRY_DISTRIBUTION=log-normal` for a positive, skewed distribution. Its controls
are `EXTENT_BENCH_ENTRY_MIN_KIB`, `EXTENT_BENCH_ENTRY_MEDIAN_KIB`, and
`EXTENT_BENCH_ENTRY_MAX_KIB`. The maximum is both the unbounded distribution's p99.9 and a hard cap;
this convention uniquely determines the distribution from a median and maximum without inventing
a Gaussian standard deviation or generating negative sizes.

Log-normal sizes are prepared as a fixed 65,536-bucket quantile table, rounded to KiB, then selected
by an independent counter-based random stream. Floating-point distribution setup therefore remains
outside measured engine work. The benchmark prints the configured distribution and its exact
observed p50/p95/p99/p99.9/max for every seed.

Default key sizes are 32, 96, 256, and 1024 bytes; custom key sizes must be at least eight bytes so
the generated keys remain unique. Access concurrency defaults to twice the detected CPU core count
and values below that are rejected. Put concurrency uses the same value by default;
`EXTENT_BENCH_PUT_CONCURRENCY` can override it for an explicit write-side control run without
weakening concurrent read validation.

`EXTENT_BENCH_PUT_MEAN_INTERVAL_US` enables a seeded Poisson arrival schedule for low-rate write
controls. Its exponential inter-arrival samples use an independent counter stream and are truncated
at eight times the mean, retaining more than 99.9% of the distribution while bounding one test
pause. This mode requires `EXTENT_BENCH_PUT_CONCURRENCY=1`; keep the tested payload within one wave
when measuring batching so an intentional wave drain does not split the arrival process. The
default is zero (`immediate`) and adds no sleeps to throughput or large-data runs.

Storage-only reads default to one full hotset warmup. Warmup uses a randomized permutation without
replacement, so every candidate is visited before measurement; `EXTENT_BENCH_READ_WARMUP` can
override the operation count, including zero for an explicitly cold control. The primary measured
read uses an independent random stream. The benchmark then uses one matched random stream for both
the no-writer control and the read-under-write phase, eliminating request-mix noise from the p99
inflation comparison. It reports
separate hit and miss latency distributions and `hit_p99_inflation` relative to the no-writer phase,
plus foreground and drain time for the burst. Keeping hits separate prevents intentionally fast
misses after eviction from hiding storage-read contention. The read side alone uses the configured
concurrency and therefore retains the benchmark's `2 * cores` minimum while the writer submits the
competing wave.
Recover-only runs do not mutate the cache and therefore skip this phase.

Best-effort priority shedding is reported as an observed pipeline outcome, not treated as
corruption. The run still fails if any accepted command is unfinished, a storage write or batch
fails, or the durable checkpoint trails the published recovery frontier.

Crash-recovery validation can set `EXTENT_BENCH_READ_PATTERN=sequential` and
`EXTENT_BENCH_READS` equal to `EXTENT_BENCH_READ_HOTSET` to visit every candidate key exactly once.
Larger sequential read counts wrap within the hotset; they do not implicitly continue into a
post-recovery write-wave range.
`EXTENT_BENCH_STORAGE_READS=1` bypasses HybridCache memory lookup and exercises the storage engine
directly. `EXTENT_BENCH_READ_HOTSET` bounds the repeated key range, while
`EXTENT_BENCH_READ_WARMUP` performs a separate warmup before the measured read phase. Together
these switches isolate a page-cache and engine-index hot path without allowing the memory cache to
hide it.
Set `EXTENT_BENCH_RECOVER_WRITE_WAVE=1` on a recover-only run to append and drain one write wave
after strict recovery, concurrently validate reads, and close with a new durable checkpoint. Both
switches are disabled by default and do not affect normal comparison runs.

`EXTENT_BENCH_PRIORITY_WORKLOAD=scopedb` is the default mixed workload. The
`historical-high` validation mode writes high-priority entries in the first half and normal entries
in the second half. It directly checks that normal demand can recover its protected floor without
allowing later normal churn to erase high-priority occupancy in the shared-capacity region.

Reported Extent read bytes and I/O operations include both payload reads recorded through Foyer's
device statistics and FixedRecordLSM reads. The separate `extent_read` and `extent_index_read`
records provide that total's phase-local decomposition; warmup work is subtracted before the
primary measured-read record. Extent write statistics include the final metadata checkpoint
performed by `wait` or close.

`EXTENT_BENCH_IO_READ_PRIORITY_US` controls Extent's cooperative payload-I/O policy and accepts
zero as a complete bypass. The benchmark prints `extent_io_scheduler` records with the number and
duration of actual write-admission waits. For scheduler A/B runs, keep initial payload plus the
write wave within usable extent capacity and require the same hit set and comparable physical I/O
on both sides. A run that also changes eviction, accepted writes, or hit ratio measures a different
workload and cannot establish scheduler latency benefit.

`EXTENT_BENCH_READ_RUN_KIB` and `EXTENT_BENCH_WRITE_RUN_KIB` override Extent's runtime-only maximum
payload I/O request sizes. They do not alter the persistent layout, so recover-only runs can compare
multiple read-run sizes against the same image. Keep values page-aligned. The Extent write record
reports payload, directory, index, and allocator run and sync counts separately so a write-run
experiment does not attribute metadata calls to payload splitting. `EXTENT_BENCH_WRITE_BATCH_DELAY_US`
controls the optional sparse-arrival microbatch window and defaults to zero; compare a nonzero
candidate with zero under the same seeds and alternating run order before enabling it. The
`extent_layout` record separates usable payload from planned
directory bytes, sparse logical directory size, and the soft Index target. The write record also
splits index WAL, SST, and manifest calls/bytes/syncs and reports compaction input/output bytes;
these counters describe userspace and FixedRecordLSM operations rather than device-internal write
amplification. `extent_reclaim` reports invalidated extents/bytes, time waiting for an already
captured checkpoint, generation-invalidation time, and total time. A correct normal reclaim shows no
increase in `extent_directory_read` or index reads/writes beyond unrelated background maintenance.
`extent_index` reports its indexed-cardinality upper bound and the cumulative number of stale
locations checked and discarded by ordinary compaction.

## 300 GiB buffered-I/O run

Choose a capacity that fits the device and the desired eviction pressure. This example offers 300
GiB into a 240 GiB cache so retention and reclaim are exercised.

```shell
EXTENT_BENCH_PATH=/mnt/local-nvme/extent-300g-buffered \
EXTENT_BENCH_CAPACITY_MIB=245760 \
EXTENT_BENCH_PAYLOAD_MIB=307200 \
EXTENT_BENCH_MEMORY_MIB=2048 \
EXTENT_BENCH_QUEUE_MIB=512 \
EXTENT_BENCH_WAVE_MIB=128 \
EXTENT_BENCH_ENTRY_CHARGE_KIB=4 \
EXTENT_BENCH_DIRECT=0 \
cargo bench -p foyer-extent --bench foyer_engine_compare
```

Repeat with `EXTENT_BENCH_DIRECT=1` on Linux. Do not combine buffered and direct results in one
comparison table.

## 200 GiB bounded log-normal run

This example interprets 1 MiB as both p99.9 and the hard size cap. It offers 200 GiB into a 128 GiB
cache, exercising sustained reclaim without requiring a 200 GiB retained image. Run engines
separately when the device cannot hold both images, but keep the same seed and configuration and
drop the page cache before each run.

```shell
EXTENT_BENCH_PATH=/mnt/local-nvme/extent-200g-lognormal \
EXTENT_BENCH_ENGINES=extent \
EXTENT_BENCH_CAPACITY_MIB=131072 \
EXTENT_BENCH_PAYLOAD_MIB=204800 \
EXTENT_BENCH_MEMORY_MIB=256 \
EXTENT_BENCH_QUEUE_MIB=512 \
EXTENT_BENCH_WAVE_MIB=64 \
EXTENT_BENCH_ENTRY_DISTRIBUTION=log-normal \
EXTENT_BENCH_ENTRY_MIN_KIB=1 \
EXTENT_BENCH_ENTRY_MEDIAN_KIB=64 \
EXTENT_BENCH_ENTRY_MAX_KIB=1024 \
EXTENT_BENCH_KEY_BYTES=8,32,96,256 \
EXTENT_BENCH_DIRECT=1 \
cargo bench -p foyer-extent --bench foyer_engine_compare
```

Repeat with at least three unrelated seeds and alternate Block/Extent order. Do not delete a
populated image until its cold-recovery trials and raw-log copy have completed.

## 100-million-entry recovery run

Populate once, then repeat recovery without rewriting the cache:

```shell
EXTENT_BENCH_PATH=/mnt/local-nvme/extent-100m \
EXTENT_BENCH_CAPACITY_MIB=450000 \
EXTENT_BENCH_ENTRIES=100000000 \
EXTENT_BENCH_ENTRY_KIB=4 \
EXTENT_BENCH_KEY_BYTES=32,96,256,1024 \
EXTENT_BENCH_POPULATE_ONLY=1 \
cargo bench -p foyer-extent --bench foyer_engine_compare

EXTENT_BENCH_PATH=/mnt/local-nvme/extent-100m \
EXTENT_BENCH_CAPACITY_MIB=450000 \
EXTENT_BENCH_ENTRIES=100000000 \
EXTENT_BENCH_ENTRY_KIB=4 \
EXTENT_BENCH_KEY_BYTES=32,96,256,1024 \
EXTENT_BENCH_READS=1000000 \
EXTENT_BENCH_RECOVER_ONLY=1 \
cargo bench -p foyer-extent --bench foyer_engine_compare
```

`EXTENT_BENCH_POPULATE_ONLY=1` closes after the initial write workload and reports the durable
footprint without performing the normal reopen, reads, or write burst. It is mutually exclusive
with `EXTENT_BENCH_RECOVER_ONLY` and leaves a clean image for repeated cold-recovery trials.

Important tuning variables remain explicit: `EXTENT_BENCH_SEED`, `EXTENT_BENCH_ENGINES`,
`EXTENT_BENCH_CONCURRENCY`,
`EXTENT_BENCH_PUT_CONCURRENCY`, `EXTENT_BENCH_SHARDS`, `EXTENT_BENCH_BLOCK_MIB`,
`EXTENT_BENCH_BLOCK_BUFFER_MIB`,
`EXTENT_BENCH_ENTRY_DISTRIBUTION`, `EXTENT_BENCH_ENTRY_KIB`, `EXTENT_BENCH_ENTRY_MIN_KIB`,
`EXTENT_BENCH_ENTRY_MEDIAN_KIB`, `EXTENT_BENCH_ENTRY_MAX_KIB`,
`EXTENT_BENCH_EXTENT_MIB`, `EXTENT_BENCH_ENTRY_CHARGE_KIB`, `EXTENT_BENCH_INDEX_CACHE_MIB`,
`EXTENT_BENCH_INDEX_WRITE_BUFFER_MIB`, `EXTENT_BENCH_EXTENT_WRITE_CONCURRENCY`, and
`EXTENT_BENCH_IO_READ_PRIORITY_US`, `EXTENT_BENCH_READ_RUN_KIB`, and
`EXTENT_BENCH_WRITE_RUN_KIB`. Priority-isolation experiments may also override
`EXTENT_BENCH_HIGH_CAPACITY_PERCENT` and `EXTENT_BENCH_NORMAL_CAPACITY_PERCENT`; their sum must not
exceed 100.

## Index-only hot path

`index_hot_path` compares the FixedLSM point-lookup path with the block engine's real sharded
in-memory index. Copy a populated FixedLSM directory before opening it because every database open
creates a fresh WAL generation:

```shell
cp -a --reflink=auto /path/to/extent-engine/index-lsm /path/to/index-profile

INDEX_BENCH_ENGINE=fixed \
INDEX_BENCH_PATH=/path/to/index-profile \
INDEX_BENCH_ENTRIES=10000000 \
INDEX_BENCH_HOTSET=10000 \
INDEX_BENCH_CACHE_MIB=256 \
cargo bench -p foyer-extent --bench index_hot_path

INDEX_BENCH_ENGINE=memory \
INDEX_BENCH_ENTRIES=10000000 \
INDEX_BENCH_HOTSET=10000 \
cargo bench -p foyer-extent --bench index_hot_path
```

Both modes prepare their index before warmup and use the same deterministic lookup stream.
`INDEX_BENCH_CONCURRENCY` defaults to twice the detected core count. Set
`INDEX_BENCH_CACHE_MIB=0` to isolate a page-cache-only FixedLSM path. For an external profiler,
`INDEX_BENCH_PROFILE_DELAY_SECONDS` inserts a delay after warmup and immediately before the measured
phase; preparation and recovery therefore remain outside the captured lookup window.
