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
are rejected.

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

Reported Extent read bytes and I/O operations include both payload reads recorded through Foyer's
device statistics and FixedRecordLSM reads. The separate `extent_read` and `extent_index_read`
records provide that total's decomposition. Extent write statistics include the final metadata
checkpoint performed by `wait` or close.

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
EXTENT_BENCH_READS=1000000 \
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

Important tuning variables remain explicit: `EXTENT_BENCH_ENGINES`, `EXTENT_BENCH_CONCURRENCY`,
`EXTENT_BENCH_SHARDS`, `EXTENT_BENCH_BLOCK_MIB`, `EXTENT_BENCH_BLOCK_BUFFER_MIB`,
`EXTENT_BENCH_SEGMENT_MIB`, `EXTENT_BENCH_SLOT_KIB`, `EXTENT_BENCH_INDEX_CACHE_MIB`,
`EXTENT_BENCH_INDEX_WRITE_BUFFER_MIB`, and `EXTENT_BENCH_EXTENT_WRITE_CONCURRENCY`.
