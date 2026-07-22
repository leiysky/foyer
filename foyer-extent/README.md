# foyer-extent

The `foyer-extent` package provides `ExtentEngine`, a Foyer disk engine backed by `ExtentStore` and
IndexDB. The project name comes from the cache extent: the fixed-size append, seal,
generation, and reclaim unit owned by `ExtentPool`.

The public cache object is `Entry`: a complete variable-length opaque key of at most 1 KiB, a
non-empty variable-length value, and a priority. Both key and value use `Bytes`, so cloning a hit is
cheap. Foyer owns the memory tier and hybrid coordination. `ExtentEngine` owns the non-blocking
engine submission boundary and one hard-bounded ordered disk queue. `ExtentStore` owns lookup,
publication, and checkpoint coordination; its concrete `Reclaimer` owns allocation pressure,
generation-reuse fencing, and priority-aware reclaim.

The balanced engine defaults use a 256 MiB submission budget, 128 MiB idle write batches, an 8 MiB
write batch while reads are active, and a 30-second periodic checkpoint request in addition to
the 256 MiB published-byte trigger. High and normal priorities have borrowable 10% and 70% logical
extent capacity floors; low priority uses unprotected capacity. Durable-index and payload reads use
independent hard, non-waiting `min(2 * available_parallelism, 64)` admission limits. The synchronous
payload I/O scheduler gives an active entry-payload read a bounded 2 ms head start over newly
admitted writes; reads never wait behind writes, and
writes proceed after the bound so sustained reads cannot starve publication. This is a cooperative
admission layer: reads, syncs, and default single-concurrency writes execute on the calling thread;
parallel-write configurations use one persistent bounded pool rather than per-batch OS threads.
No io_uring dependency is involved.

Low- and normal-priority writes are progressively shed before the queue is full, with earlier
shedding while reads are active; high-priority puts retain the hard queue budget. Puts and deletes
share the same hard entry and byte bounds. A rejected put does not enqueue a compensating delete,
so overload cannot become unbounded control debt; mutable callers encode freshness in the key as
required by the cache contract. An ordered worker batch retains only the final command per complete
key and coalesces all surviving puts into page-aligned writes. Checkpoints group payload durability
once per captured epoch. Reclaim adds one recovery-critical generation-state sync without syncing
unrelated dirty payload; it does not scan the victim or copy retained payload. An
`ExtentEngineHandle` exposes queue depth, publication/durability frontiers, asynchronous write
outcomes, active read admission, scheduler waits, physical I/O, reclaim work, and the first sticky
background failure. These observations do not turn fire-and-forget puts into acknowledged writes.
The public `Cache` facade exposes this handle directly through `engine_handle()`, together with
`storage_usage()` and the shared Foyer `statistics()`, so a production canary does not need to retain
an internal builder config solely for observability. `estimated_entry_count()` returns the larger
of the memory-resident count and live extents' physical record count. It avoids systematic
memory/disk overlap double-counting but can include overwritten records inside an extent, so it is
telemetry rather than an exact cardinality.

The Foyer-facing queue, pipeline, and recovery state is also exported through its metrics registry as
`foyer_storage_engine_command_total`, `foyer_storage_engine_batch_total`,
`foyer_storage_engine_queue_entries`, `foyer_storage_engine_queue_bytes`,
`foyer_storage_engine_checkpoint`, `foyer_storage_engine_read_total`,
`foyer_storage_engine_priority_extents`, `foyer_storage_engine_priority_allocated_bytes`,
`foyer_storage_engine_readers`, `foyer_storage_engine_duration`,
`foyer_storage_engine_recovery_total`, and `foyer_storage_engine_healthy`. Queue gauges are updated
at reservation ownership changes; the worker refreshes checkpoint frontiers on every batch and
periodic checkpoint tick. `storage_usage()` is an O(1) snapshot over a fixed file set: it combines
allocated blocks for the preallocated data/state files with IndexDB's atomic disk-budget
counter.
The shared physical-I/O counters include both payload and index reads, including reads that finish
as a validated cache miss, and all payload, checkpoint, and index writes. Index counters split WAL,
SST, and manifest writes/syncs and report flush and compaction bytes. Cumulative IndexDB
counters are reconciled exactly once so concurrent lookups cannot double-count index I/O.

Stored Entries occupy contiguous byte ranges packed within cache extents. Adjacent allocations in
one publication batch share page-aligned I/O frames; an I/O frame is not a capacity or reclaim unit.
Stored Entries are indexed directly by location, and a cache extent is reused as one generation.
Object ranges, application-specific key encoding, and remote-storage behavior belong outside the
project.

Capacity is the only production static input. Stable format 1 owns a 64 MiB cache extent, a 4 KiB I/O
frame, and a 4 KiB Entry planning charge. The charge sizes the soft index target; it neither rounds
physical Entry allocations nor caps how many small Entries may be packed into an extent. The data
file and both allocator-state copies fit the configured capacity. The EntryIndex target may exceed
its plan and report that pressure without rejecting a cache write. Changing these choices, layout
derivation, record encoding, or an incompatible embedded-index format after the first production
freeze requires an `EXTENT_FORMAT_VERSION` bump. Before that freeze, Format 1 may be replaced in
place and development cache images remain expendable.
Layout overrides remain available only as a test and benchmark escape hatch. Runtime I/O, queue,
batching, checkpoint, priority-floor, and index-memory settings can change across reopens. The
production builder exposes only operational policy: direct I/O, index-cache and read-admission
budgets, priority floors, and the engine throttle. Implementation-level queue, batching, write-run,
and checkpoint overrides are grouped under the explicitly non-production test tuning object.

The durable exact index is the workspace-private `foyer-index-db` crate. RocksDB support is gated
behind the `rocksdb-benchmark` feature and exists only as an industrial comparison point.

`foyer-extent` must be resolved with `foyer` from the same fork revision. A compile-time
`DISK_ENGINE_API_VERSION` assertion rejects an incompatible Foyer engine boundary. Both extension
crates are `publish = false`; the fork workspace, rather than crates.io version coincidence, is the
distribution unit.

The production target is Linux SSD. Buffered I/O also supports macOS and Windows development
builds; direct I/O is Linux-only. The Foyer boundary supports entry insertion, lookup, deletion,
waiting, close, and recovery. Online `HybridCache::clear()` currently returns an error: a correct
implementation requires an atomic extent-generation replacement and must not be emulated with an
O(live entries) tombstone pass. Close and reopen with `RecoverMode::None` to reset the cache.
Reset removes only Extent-owned files below the configured directory; it does not recursively
delete that directory or unrelated caller files.

Graceful close rejects new submissions, completes at most the atomic batch already executing,
discards the unstarted queue tail, and publishes one final durable checkpoint. The discarded tail
is explicitly counted. This bounds shutdown by one batch plus checkpoint work without exposing a
partially published entry; cache writes remain best effort and the source remains authoritative.

Current-format round-trip, invalid-magic/version, checkpoint-tail-discard, and process-crash tests
cover the payload, allocator, checkpoint, and reclaim publication paths. Extent has one persisted
layout and no compatibility reader or migration path.

Design documentation is organized by boundary:

- [`docs/architecture.md`](docs/architecture.md) — system goals, layers, invariants, and document
  map;
- [`docs/cache-contract.md`](docs/cache-contract.md) — Entry identity, best-effort semantics, and
  integrity;
- [`docs/foyer-integration.md`](docs/foyer-integration.md) — Foyer engine adaptation, queues,
  lifecycle, and observability;
- [`docs/extent-store.md`](docs/extent-store.md) — stable format 1 physical layout, checkpoint, reclaim, and
  failure model; and
- [`docs/entry-index.md`](docs/entry-index.md) — overlays, IndexDB, recovery, and index-space
  accounting.

[`docs/foyer-engine-benchmark.md`](docs/foyer-engine-benchmark.md) defines the BlockEngine
comparison procedure. Dated measurements and production gates remain separate validation evidence,
not design truth.
