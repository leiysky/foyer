# foyer-extent

The `foyer-extent` package provides `ExtentEngine`, a Foyer disk engine backed by `ExtentStore` and
FixedRecordLSM. The project name comes from the cache extent: the fixed-size append, seal,
generation, and reclaim unit owned by `ExtentPool`.

The public cache object is `Entry`: a complete variable-length opaque key of at most 1 KiB, a
non-empty variable-length value, and a priority. Both key and value use `Bytes`, so cloning a hit is
cheap. Foyer owns the memory tier and hybrid coordination. `ExtentEngine` owns the non-blocking
engine submission boundary and one put-bounded ordered disk queue. `ExtentStore` owns lookup,
publication, and checkpoint coordination; its concrete `Reclaimer` owns allocation pressure,
generation-reuse fencing, and priority-aware reclaim.

The balanced engine defaults use a 256 MiB submission budget, 128 MiB idle write batches, an 8 MiB
write batch while reads are active, and a one-second periodic checkpoint request in addition to
the 256 MiB published-byte trigger. High and normal priorities have borrowable 10% and 70% logical
extent capacity floors; low priority uses unprotected capacity. Physical reads have a hard, non-waiting
`2 * available_parallelism` admission limit. The synchronous payload I/O scheduler gives an active
entry-payload read a bounded 2 ms head start over newly admitted writes; reads never wait behind writes, and
writes proceed after the bound so sustained reads cannot starve publication. This is a cooperative
admission layer: the calling thread retains the buffer and executes `pread`, `pwrite`, or
`fdatasync`; no extra executor or io_uring dependency is involved. Set the read-priority duration
to zero for a full runtime bypass.

Low- and normal-priority writes are progressively shed before the queue is full, with earlier
shedding while reads are active; high-priority puts retain the hard queue budget. Ordered deletes
may temporarily overcommit that budget so a rejected update cannot leave an older value visible;
pending queue gauges expose the overcommit instead of blocking the caller. An
`ExtentEngineHandle` exposes queue depth, publication/durability frontiers, asynchronous write
outcomes, active read admission, scheduler waits, physical I/O, reclaim work, and the first sticky
background failure. These observations do not turn fire-and-forget puts into acknowledged writes.
The public `Cache` facade exposes this handle directly through `engine_handle()`, together with
`storage_usage()` and the shared Foyer `statistics()`, so a production canary does not need to retain
an internal builder config solely for observability. `estimated_entry_count()` returns the larger
of the memory-resident count and the disk index's live count. It avoids systematic overlap
double-counting and is intended only as a low-cost telemetry estimate.

The Foyer-facing queue, pipeline, and recovery state is also exported through its metrics registry as
`foyer_storage_engine_command_total`, `foyer_storage_engine_batch_total`,
`foyer_storage_engine_queue_entries`, `foyer_storage_engine_queue_bytes`,
`foyer_storage_engine_checkpoint`, `foyer_storage_engine_read_total`,
`foyer_storage_engine_priority_extents`, `foyer_storage_engine_priority_allocated_bytes`,
`foyer_storage_engine_readers`, `foyer_storage_engine_duration`,
`foyer_storage_engine_recovery_total`, and `foyer_storage_engine_healthy`. Queue gauges are updated
at reservation ownership changes; the worker refreshes checkpoint frontiers on every batch and
periodic checkpoint tick. `storage_usage()` is an O(1) snapshot over a fixed file set: it combines
allocated blocks for the preallocated data/state files and sparse directory with
FixedRecordLSM's atomic disk-budget counter.
The shared physical-I/O counters include both payload and index reads, including reads that finish
as a validated cache miss, and all payload, checkpoint, and index writes. Cumulative FixedRecordLSM
counters are reconciled exactly once so concurrent lookups cannot double-count index I/O.

Stored Entries occupy contiguous byte ranges packed within cache extents. Adjacent allocations in
one publication batch share page-aligned I/O frames; an I/O frame is not a capacity or reclaim unit.
Each Stored Entry has one fixed directory record, and a cache extent is reused as one generation.
Object ranges, application-specific key encoding, and remote-storage behavior belong outside the
project.

Capacity is the only production static input. The V6 format owns a 64 MiB cache extent, a 4 KiB I/O
frame, and a 4 KiB Entry planning charge. The charge sizes planned directory and index targets; it
neither rounds physical Entry allocations nor caps how many small Entries may be packed into an
extent. The data file, planned directory budget, and allocator state fit the configured capacity.
The sparse directory address space and EntryIndex target may exceed their plans and report that
pressure without rejecting a cache write. Changing these choices, layout derivation, record
encoding, or an incompatible embedded-index format requires an `EXTENT_FORMAT_VERSION` bump.
Layout overrides remain available only as a test and benchmark escape hatch. Runtime I/O, queue,
batching, checkpoint, frequency, and index-memory settings can change across reopens.

The durable exact index is the workspace-private `foyer-fixed-lsm` crate. RocksDB support is gated
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

Compatibility CI reconstructs a frozen complete V3 store image and verifies that V6 rejects it and
can recreate the expendable cache without leaving its legacy owner file behind. V6 round-trip,
tail-recovery, and process-crash tests cover the current directory, allocator, checkpoint, and
reclaim publication paths.

Design documentation is organized by boundary:

- [`docs/architecture.md`](docs/architecture.md) — system goals, layers, invariants, and document
  map;
- [`docs/cache-contract.md`](docs/cache-contract.md) — Entry identity, best-effort semantics, and
  integrity;
- [`docs/foyer-integration.md`](docs/foyer-integration.md) — Foyer engine adaptation, queues,
  lifecycle, and observability;
- [`docs/extent-store.md`](docs/extent-store.md) — V6 physical layout, checkpoint, reclaim, and
  failure model; and
- [`docs/entry-index.md`](docs/entry-index.md) — overlays, FixedRecordLSM, recovery, and index-space
  accounting.

[`docs/foyer-engine-benchmark.md`](docs/foyer-engine-benchmark.md) defines the BlockEngine
comparison procedure. Dated measurements and production gates remain separate validation evidence,
not design truth.
