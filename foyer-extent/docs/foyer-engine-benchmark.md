# Foyer engine benchmark

`foyer_engine_compare` compares BlockEngine and ExtentEngine below the same Foyer HybridCache. It
does not depend on ScopeDB. Both sides use `Bytes` keys, `EngineValue`, S3FIFO, write-on-insertion,
the same memory capacity and shards, the same deterministic workload, and the same concurrency.

The benchmark refuses to run without `EXTENT_BENCH_PATH`; point it at a real SSD directory, never a
tmpfs. It deletes only the selected `block` and `extent` children when reset is enabled.

## Smoke test

```shell
EXTENT_BENCH_PATH=/path/on/ssd/extent-smoke \
EXTENT_BENCH_CAPACITY_MIB=512 \
EXTENT_BENCH_PAYLOAD_MIB=600 \
cargo bench -p foyer-extent --bench foyer_engine_compare
```

The default entry sizes are 4, 16, 64, 256, and 1024 KiB. Default key sizes are 32, 96, 256, and
1024 bytes. Access concurrency defaults to twice the detected CPU core count and values below that
are rejected. Put concurrency uses the same value by default; `EXTENT_BENCH_PUT_CONCURRENCY` can
override it for an explicit write-side control run without weakening concurrent read validation.

After the cold-memory read phase, the benchmark repeats the read workload on the same cache to
establish a warm steady-state control, then runs it again while a new-key write wave is being
submitted. It reports
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
`EXTENT_BENCH_READS` equal to the offered entry count to visit every candidate key exactly once.
Larger sequential read counts continue into the deterministic new-key range, which can validate a
write wave appended after recovery.
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
in the second half. It directly checks that historical high occupancy above its floor is returned
to normal demand instead of permanently starving it.

Reported Extent read bytes and I/O operations include both payload reads recorded through Foyer's
device statistics and FixedRecordLSM reads. The separate `extent_read` and `extent_index_read`
records provide that total's decomposition. Extent write statistics include the final metadata
checkpoint performed by `wait` or close.

`EXTENT_BENCH_IO_READ_PRIORITY_US` controls Extent's cooperative payload-I/O policy and accepts
zero as a complete bypass. The benchmark prints `extent_io_scheduler` records with the number and
duration of actual write-admission waits. For scheduler A/B runs, keep initial payload plus the
write wave within usable extent capacity and require the same hit set and comparable physical I/O
on both sides. A run that also changes eviction, accepted writes, or hit ratio measures a different
workload and cannot establish scheduler latency benefit.

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
EXTENT_BENCH_SLOT_KIB=16 \
EXTENT_BENCH_DIRECT=0 \
cargo bench -p foyer-extent --bench foyer_engine_compare
```

Repeat with `EXTENT_BENCH_DIRECT=1` on Linux. Do not combine buffered and direct results in one
comparison table.

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

Important tuning variables remain explicit: `EXTENT_BENCH_ENGINES`, `EXTENT_BENCH_CONCURRENCY`,
`EXTENT_BENCH_PUT_CONCURRENCY`, `EXTENT_BENCH_SHARDS`, `EXTENT_BENCH_BLOCK_MIB`,
`EXTENT_BENCH_BLOCK_BUFFER_MIB`,
`EXTENT_BENCH_EXTENT_MIB`, `EXTENT_BENCH_SLOT_KIB`, `EXTENT_BENCH_INDEX_CACHE_MIB`,
`EXTENT_BENCH_INDEX_WRITE_BUFFER_MIB`, `EXTENT_BENCH_EXTENT_WRITE_CONCURRENCY`, and
`EXTENT_BENCH_IO_READ_PRIORITY_US`. Priority-isolation experiments may also override
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
