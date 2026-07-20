---
status: superseded by ADR 0007
---

# Use RocksDB for the durable EntryIndex

FixedRecordLSM passed the narrower fixed-record evaluation, so ADR 0007 supersedes this proposal.
RocksDB remains the industrial benchmark and fallback design; it is not a selectable production
backend.

## Context

ScopeDB still needs its own cache-extent data plane. Priority-aware admission, immutable blob payloads,
generation-fenced reclamation, bounded disk space, and large sequential payload writes are not a
general KV engine's job. The open question is narrower: whether ScopeDB should also maintain the
durable 24-byte-key to 32-byte-location index.

ADR 0005's paged base solved Foyer's entry loss and full-index recovery scan. A FASTER/F2-style
tag-and-owner-pointer prototype then showed that an apparently faster compact hash index loses once
exact-key validation performs the mandatory owner read. redb had unsuitable copy-on-write
amplification, and Fjall was useful as a lightweight screen but not as the industrial performance
reference. The custom index therefore had to clear a matched RocksDB baseline before acceptance.

The application path also changes the weight of index-only microbenchmarks. `CacheBackend::Extent`
checks ScopeDB's blob memory cache before invoking the disk engine and inserts successful disk reads
back into that cache. Repeated hot blobs normally do not execute persistent-index lookup at all.
Sustained hot-index lookup remains a useful CPU ceiling, but it is not representative enough to
justify a private durable engine by itself.

## Decision

Keep the custom `ExtentStore` and `ExtentPool`, and replace only `EntryIndex` with RocksDB, subject to the
300 GiB integrated acceptance run. The integration uses the existing engine-facing key/location
interface; RocksDB does not own payload placement, cache priority, reclamation, or range assembly.

The index has three layers:

1. A Rust `FrequencySketch` retains TinyLFU observations and reclamation frequency estimates. It is
   advisory and may start empty after recovery.
2. A bounded active overlay contains newly published exact mutations. Checkpoint capture rotates it
   into one immutable frozen overlay in constant time, so reads and later writes continue while
   durability I/O runs.
3. RocksDB stores the durable exact key/location state. Lookup checks active overlay, frozen overlay,
   and then RocksDB. Deletes are explicit tombstones in both overlays and become RocksDB deletes in
   the checkpoint batch.

The overlay is required for crash ordering, not as another durable LSM level. A publication must not
write directly to a WAL that a concurrent checkpoint can accidentally sync. Instead, checkpointing
uses this order:

1. Under the engine mutation lock, capture the allocator state and rotate dirty index mutations.
2. Release the lock and persist the payload durability fence and captured allocator state.
3. Apply the frozen mutations as one RocksDB `WriteBatch` with WAL enabled and `sync=true`.
4. Retire the frozen overlay only after the batch succeeds. Newer active mutations remain volatile
   until their allocator state is captured by a later checkpoint.

This preserves the existing `payload -> allocator -> index` invariant. A crash before the RocksDB
batch can leak unreachable slots but cannot expose an invalid location. A crash after its sync can
recover only locations whose allocator generation is already durable. Strict insert waits for the
publication epoch's checkpoint exactly as it does today.

The balanced static RocksDB configuration is buffered I/O, 16 KiB data blocks, a 10 bits/key full
Bloom filter, binary-and-hash data-block index, current table format, leveled compaction with
minimum-overlap priority, no compression, 64 MiB memtables, four write buffers, and background jobs
bounded by available cores. Index/filter blocks share the configured metadata-cache budget and L0
metadata is pinned. Runtime configuration may tune cache size and worker count, but the first
production comparison does not carry multiple compaction policies.

During canary development, index kind is a static disk-incarnation choice so PagedIndex remains an
A/B and rollback baseline. It is not intended as a permanent runtime switch: delete the losing
backend after the integrated gate. RocksDB uses a directory within the existing index space budget;
the current dual-region PagedIndex reservation is large enough for the measured steady state and
must be checked against transient compaction space before publication.

## Evidence

The matched benchmark used rust-rocksdb 0.24.0, which embeds RocksDB 10.4.2, on the two-core
i8g.large instance-store NVMe under `/work`. Both engines used a 512 MiB metadata-cache budget,
buffered I/O, the same exact 24-byte keys and 32-byte locations, four clients, and one writer plus
three readers for mixed access. Compression was disabled to avoid rewarding synthetic key/value
compressibility. RocksDB statistics were disabled for latency runs and enabled only to decompose
churn I/O.

| 100M state | PagedIndex | RocksDB |
| --- | ---: | ---: |
| Clean cold open | 49.9 ms, 38.7 MB read | 41.3 ms, 16.1 MB read |
| Dirty cold open | 1.436 s, 311 MB read, about 495 MiB RSS | 1.426 s, 76.6 MB read, about 90 MiB peak RSS |
| Persistent index space | 13.63 GB dual-region file | 6.34 GB directory |
| Warm 258,048-key point lookup, four clients | 2.49-2.63 M/s | 0.847 M/s |
| Uniform 50/50 mixed, eight million operations | 0.167 M/s | 0.229 M/s |
| 90%/1% hot-set 50/50 mixed | 0.321 M/s | 0.419 M/s |

PagedIndex's warm point-read p50/p99 was approximately 0.8/1.15 microseconds versus RocksDB's
2.31/2.98 microseconds. That is a real threefold CPU-path advantage. In the uniform mixed run,
however, RocksDB improved total throughput by 37%; its read p50/p99 was 10.37/119.19 microseconds
versus PagedIndex's 5.37/113.15, while its write p50/p99 was 2.33/19.77 microseconds versus
5.36/86.41. In the hot-set run RocksDB improved total throughput by 30%; PagedIndex retained lower
read latency, while RocksDB had substantially lower write p99.

Three consecutive 4,194,304-update RocksDB churn generations accounted for 239.6, 321.6, and
233.9 physical bytes per update, averaging about 265 bytes. There were no write stalls. The first
generation comprised about 247 MB of WAL, 268 MB of flush output, and 489 MB of compaction output.
PagedIndex's full-delta merge read 6.50 GB, wrote 6.54 GB, took 18.26 seconds including preparation,
and produced about 1.56 KiB of merge output per update. The first three RocksDB generations therefore
wrote about 5.9 times fewer physical bytes per update, and each drained in 6.1-7.0 seconds including
mutation publication and WAL sync. This is not yet a long-run bottom-level write-amplification
claim: the RocksDB directory grew from 6.34 GB to about 6.75 GB, so a longer churn sequence and a
major-compaction accounting run remain part of the acceptance baseline.

These results fail ADR 0005's custom-engine gate: its material hot-lookup win is accompanied by
more than 10% regressions in mixed throughput, write amplification, dirty-recovery resources, and
space. ScopeDB's blob memory tier further reduces the production weight of that isolated hot-index
win.

## Consequences and remaining gate

- Recovery no longer scans live entries or rebuilds a full in-memory map. Work is bounded by the
  manifest and recoverable WAL/memtable tail, independent of the 100M durable base.
- RocksDB owns WAL, SST validation, filters, leveled compaction, and decades of crash testing.
  ScopeDB retains only the domain-specific overlay and checkpoint ordering.
- The C++ dependency is material. A first release build on the two-core ARM development host took
  9 minutes 35 seconds and required `libclang`; benchmark-only support remains feature-gated so it
  does not tax ordinary builds. Production adoption must explicitly accept build size, security
  updates, and FFI operations.
- A 512 MiB block cache produced approximately 0.8-1.1 GiB peak process RSS in mixed runs because
  memtables, cache metadata, filters, and allocator overhead are additional. Runtime defaults must
  budget total process memory rather than equating cache capacity with RSS.
- Background compaction read 5.31-8.39 GB during the cold mixed workloads and can interfere with
  payload I/O. Device-level latency under the full ScopeDB workload remains the decisive risk.
- Before acceptance, implement the overlay/checkpoint adapter and pass crash-boundary tests, then
  run the existing 300 GiB broad-entry-size workload with `core x 2` concurrent access. It must
  retain priority behavior and complete content validation while comparing payload throughput,
  read/write p99 and p99.9, total device amplification, compaction interference, recovery, RSS, and
  disk headroom against PagedIndex.

## References

- [RocksDB block cache](https://github.com/facebook/rocksdb/wiki/Block-Cache)
- [RocksDB setup and basic tuning](https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning)
- [rust-rocksdb 0.24.0 release](https://github.com/rust-rocksdb/rust-rocksdb/releases/tag/v0.24.0)
