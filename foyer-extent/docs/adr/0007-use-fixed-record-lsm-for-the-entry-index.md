---
status: accepted
---

# Use FixedRecordLSM for the EntryIndex

ScopeDB will use its Rust-native FixedRecordLSM as the sole durable `EntryKey -> EntryLocation`
index in ExtentStore. The choice keeps recovery proportional to manifest/SST metadata plus a
bounded WAL tail, supports the cache's high-churn point workload, and avoids carrying a
general-purpose C++ database in normal ScopeDB builds.

## Boundary

ExtentStore remains the key-to-Entry owner, while ExtentPool owns only physical placement.
FixedRecordLSM sees fixed 24-byte keys, fixed 32-byte locations, atomic put/delete batches, point
lookups, and one opaque `u64` application state. It has no range or payload-layout knowledge.

There is no runtime index selector. The former journal, PagedIndex, redb, Fjall, and chunk-hash
implementations are removed rather than retained as permanent comparison groups. RocksDB remains
behind the `rocksdb-benchmark` feature as the industrial reference. Foyer remains the production
cache control and rollback engine outside ExtentStore.

## Why this implementation

The workload does not need arbitrary key/value lengths, iteration, snapshots, transactions, column
families, merge operators, TTL, compression, or a public comparator. Specializing the established
LSM structure removes those surfaces while preserving the required pieces: checksummed batch WAL,
immutable SSTs, Bloom filters, a bounded block cache, partitioned leveled compaction, and
alternating checksummed manifests.

The ExtentStore adapter maintains active and frozen overlays. Checkpoint capture is short and
serialized; payload and allocator durability precede the synced LSM batch. The live-entry count is
stored as application state, so recovery never scans all keys. A base revision plus post-I/O
overlay recheck closes the concurrent lookup/checkpoint miss race.

## Evidence and tradeoffs

Matched index-only tests on the two-core i8g.large instance-store SSD used 100 million live records,
buffered I/O, four clients, and RocksDB 10.4.2 as the reference.

| Metric | FixedRecordLSM | RocksDB |
| --- | ---: | ---: |
| Uniform 50/50 mixed throughput | 0.272 Mops/s | 0.228 Mops/s |
| 90%/1% hot-set mixed throughput | 0.586 Mops/s | 0.438 Mops/s |
| Uniform read p99 | 153.6 us | 119.5 us |
| Hot-set read p99 | 93.8 us | 129.2 us |
| Replacement publication | 4.824 M mutations/s | 0.941 M mutations/s |
| Replacement bytes/mutation | 605.8 B | 496.8 B |
| Final index after churn | 7.18 GB | 6.66 GB |
| Churn peak RSS | 412 MiB | 204 MiB |

FixedRecordLSM is faster on mixed/hot churn but has a 28.5% worse uniform read p99, 22% more
index-only bytes per replacement, and about twice the churn RSS. These are accepted only because
the integrated cache is payload-I/O dominated and the ScopeDB-level results remain within their
gates.

The integrated 300 GiB run stored and fully revalidated 7,602,593 Entries spanning 4-128 KiB. It
sustained 359.5 MiB/s payload submission, reopened in 60.7 ms warm and 71.6 ms after dropping the
page cache, and produced 2.532/2.977 ms recovered-read p99/p99.9. Peak RSS was 1.19 GiB.

A later 10-million-entry hot-path profile compared the persistent index with Foyer's in-memory
block index at four clients. Borrowing cached data pages in place, probing L0 Bloom filters before
absent data blocks, and lazily pinning Bloom pages raised FixedRecordLSM from 0.858 to a three-run
median of 1.250 Mops/s and reduced median p50 from 2.37 to 1.64 microseconds. With a one-million-key
working set, raising the cache budget from 256 MiB to 1 GiB raised throughput from 0.587 to
1.002 Mops/s and reduced p99 from 7.34 to 2.67 microseconds; only 39 data-page misses occurred
during the measured ten million lookups. The same index changes improved the complete storage path
by only 2.6%, confirming that a resident index is no longer its primary bottleneck.

The final development-host validation and its production-canary boundary are recorded in
the [storage validation closeout](../validation-closeout-2026-07-20.md).

## Configuration policy

The static format and compaction policy are fixed at their balanced values. The index cache defaults
to a lazily populated 1 GiB upper bound; one eighth is a bounded pinned-Bloom budget and the rest is
a shared evictable page cache. Runtime configuration may tune the total cache, allocation/service
sizes, FixedRecordLSM write-buffer budget, and checkpoint size/interval. It does not expose the
cache split, block format, Bloom shape, level topology, or compaction style. A low-benefit
algorithm variant should be rejected; a materially better design must replace this one after a
matched RocksDB and ScopeDB/Foyer evaluation.

## Consequences

- Clean recovery is not an O(live entries) rebuild, and a bounded dirty WAL tail is the only replay.
- Normal ScopeDB builds remain Rust-native and do not link RocksDB.
- ScopeDB now owns WAL compatibility, compaction correctness, corruption policy, transient space
  amplification, and upgrade testing; this maintenance burden is the principal cost.
- Production replacement of Foyer still requires canary evidence for statement latency,
  object-store traffic, recovery, crash safety, and long-running churn.
- If integrated write amplification, tail latency, or correctness stops meeting the gates,
  FixedRecordLSM is replaced by RocksDB behind the same narrow adapter; another selectable backend
  is not added.
