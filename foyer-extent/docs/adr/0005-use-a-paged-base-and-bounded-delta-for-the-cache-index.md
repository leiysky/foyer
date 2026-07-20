---
status: rejected
---

# Use a paged base and bounded delta for the extent cache index

This rejected design is retained only as historical evidence. Its implementation and benchmark
backend were removed after ADR 0007 selected FixedRecordLSM.

## Context

ADR 0002 replaced candidate buckets with an exact dual-generation journal. That removed index
admission loss and made metadata writes sequential, but recovery still decoded every live record and
rebuilt one in-memory hash map. A 100-million-blob test on the two-core i8g.large host took a median
37.039 seconds, read 6.50 GB, and used about 7.13 GiB RSS. Preloading the file did not materially
help, which identifies decode, hashing, allocation, and map construction rather than storage
bandwidth as the structural limit.

The cache needs exact point lookup, bounded recovery, crash-safe publication, and cheap writes. It
does not need ordered iteration in the foreground or the multiple levels and range-query machinery
of a general-purpose LSM tree. A hot metadata page must never cause another index-file read.

## Decision

Use a deliberately two-level LSM-shaped index:

- The base is an immutable sequence of checksummed 4 KiB pages sorted by the complete 24-byte key.
  Each page contains up to 63 fixed-size records.
- An in-memory fence directory stores the maximum complete key of every base page. Recovery reads
  this directory, but does not read or rebuild the base.
- The mutable delta is an exact in-memory hash overlay persisted as sequential checksummed pages.
  It is bounded to `min(live capacity / 4, 4,194,304)` records.
- Lookup checks the active delta, a frozen compaction delta, and then the base. Base lookup binary
  searches the directory, reads one page on a cache miss, validates it, and searches it exactly.
- The metadata cache validates a page once and stores compact decoded key/location entries. A full
  page needs at most 3,024 bytes of entry payload, leaving the nominal 4 KiB page budget for cache
  bookkeeping. Valid and corrupt pages are both cached. A cache hit performs no file read.
- When the delta is full, its hash map is rotated into an immutable epoch in constant time. New
  mutations use a fresh overlay while the old overlay is sorted and merged with the base. This
  avoids copying millions of entries while holding the index state lock.
- Compaction streams the old base and sorted frozen delta into the inactive region, writes its fence
  directory, syncs it, and finally commits a new root. Each region has two alternating
  superblocks. A crash before root commit recovers the previous generation.
- A lookup that performs page I/O rechecks both the base identity and overlays after I/O. It can
  return an old value only at a valid point before a concurrent mutation; it cannot publish a
  location from a replaced generation.

This is a base-plus-delta or two-level LSM, not a general LSM engine. There are no L0 runs, levels,
Bloom filters, size-tiered/leveled policy, or compaction tuning knobs. Multiple runs would add read
amplification and policy surface without improving this cache's exact point-lookup contract.

The static layout is intentionally fixed. The 25% delta is efficient for ordinary capacities; the
4M-record cap bounds large-cache recovery memory and time. Existing metadata-cache, write-batch,
metadata-flush, and checkpoint runtime settings remain the tuning boundary.

This decision is provisional until it clears a matched RocksDB baseline. Foyer, redb, and Fjall are
useful implementation screens, but none is the performance reference for choosing a custom index.
RocksDB's current leveled engine is the SOTA industrial baseline. The comparison uses the same
24-byte exact key, 32-byte location, buffered I/O, disabled compression, 512 MiB bounded block and
metadata cache, WAL durability boundary, and two background workers on the two-core host. It must
include all of the following rather than an empty-fill microbenchmark:

- 100 million live entries, both a clean checkpoint and 4,194,304 uncheckpointed durable updates;
- eight million operations with one writer and three readers (`core x 2`), at 50/50 read/write,
  for both uniform access and 90% access to a 1% hot set;
- repeated churn cycles so steady-state compaction, not only the first L0 flush, determines write
  amplification;
- open/recovery time and physical reads, throughput, read/write p50/p99/p99.9, process and engine
  read/write amplification, disk footprint, and RSS under the same cache budget; and
- the 300 GiB variable-entry-size ScopeDB adapter workload after the isolated index comparison.

The custom index is accepted only if it has a material ScopeDB advantage: at least a 20% gain in a
primary recovery, tail-latency, throughput, or device-amplification metric, with no more than a 10%
regression in the other critical latency and throughput metrics and no new correctness or recovery
scan risk. Otherwise ScopeDB should use RocksDB behind the index interface instead of maintaining a
private storage engine. Runtime knobs may explore the Pareto frontier, but the pass/fail result uses
one balanced static configuration for each engine.

## Evidence

The isolated scale test directly generated a valid compacted base because 100 million 16 KiB
payload slots would exceed the development device. It validates index behavior and does not claim
to be a full 1.6 TB cache run. All files were on the i8g.large instance-store NVMe under `/work`.

| 100M state | Cold recovery median | Physical read | Recovery RSS |
| --- | ---: | ---: | ---: |
| Full-map journal | 37.039 s | 6,501,593,088 B | about 7.13 GiB |
| Paged base, empty delta | 49.9 ms | 38,653,952 B | about 39 MiB |
| Paged base, full 4,194,304-record delta | 1.436 s | 311,353,344 B | about 495 MiB |

The empty-delta state is about 742 times faster than the old journal median. The deliberately worst
bounded-delta state remains about 26 times faster and uses about fourteen times less memory. The
index file is 13,625,540,608 bytes versus 16,253,984,768 bytes for the journal layout.

With 4,096 base pages warmed, four clients (`core x 2`) performed eight million exact lookups per
run at 2.49-2.63 million lookups/s. Median sampled latency was about 0.8 microseconds and p99 about
1.15 microseconds. Every run recorded eight million metadata-cache hits and zero additional page
reads or bytes. Unit tests assert the same invariant from the engine's physical-read counters.

A matched experiment cached the original encoded 4 KiB page and decoded records during every
lookup. With the full delta present it sustained 2.41-2.48 million lookups/s, below the
2.49-2.59 million/s decoded-page representation, while using 4,096 bytes instead of at most 3,024
bytes for record payload. That variant was removed rather than retained as another mode.

An 8-byte tag-and-owner-pointer hash prototype tested the FASTER/F2-style alternative. Without the
mandatory owner lookup it appeared faster, but that was not ScopeDB's exact-key contract. Adding a
real 6.4 GB owner sidecar and a bounded owner-page cache made a cold uniform mixed run sustain only
0.129 million operations/s versus the paged index's 0.167 million/s. Under 90%/1% hot-set access it
reached 0.268 million/s versus 0.321 million/s. The extra owner read also couples index correctness
to data placement, so this representation is rejected rather than optimized further.

Fjall remains a lightweight integration screen, not the acceptance baseline. At 10 million entries
its bulk load sustained 3.30 million inserts/s and cold open took 1.97 ms, but four-client point
lookup over a warmed 258,048-key set sustained only 0.81 million/s while allocating approximately
539 MiB RSS for the configured 512 MiB cache. A 4,194,304-update churn pass sustained 0.405 million
updates/s and accounted for 167.9 physical bytes per update. These results do not justify replacing
the custom path, and they do not substitute for the RocksDB comparison above.

A full 100M compaction merged 4,194,304 delta records. Constant-time overlay rotation happened
under the state lock; out-of-lock snapshot materialization and sorting brought total preparation to
641 ms. The streaming merge took 17.62 seconds, read 6.50 GB, and wrote 6.54 GB.
Cold recovery after root publication returned to 50.5 ms. This is approximately 1.56 KiB of index
merge output per mutation at the largest supported base, or about 9.5% of a 16 KiB payload before
the small delta append. Larger ScopeDB blobs reduce that ratio. Compaction I/O interference and
checkpoint tail latency remain canary signals; adding more LSM levels is justified only if measured
interference exceeds the accepted bound.

An 8 GiB buffered ScopeDB adapter comparison used 31,356 entries spanning 4 KiB through 1 MiB and
four clients on the same host. Extent sustained 421.7 MiB/s versus Foyer's 397.6 MiB/s, reopened in
3.84 ms versus 57.93 ms, and had recovered-read p50/p99 of 0.047/0.343 ms versus
0.115/1.124 ms. Extent's write-admission p99 was 43.1 ms versus Foyer's 20.6 ms, while its p99.9
was 43.5 ms versus Foyer's 335.8 ms. The paged index therefore removes the recovery bottleneck and
does not regress the tested read path, but it does not claim universal latency dominance.

## Consequences

- Recovery work and memory are bounded by the fence directory plus delta, not total live entries.
- Exact full-key comparison retains the no-entry-loss property of the journal.
- Hot base lookup adds CPU and one sharded cache lock, but no storage I/O. Overlay lookup remains an
  in-memory hash lookup.
- Base corruption is detected per page and returned as a cache miss. Open does not scan every base
  page merely to prove it, because that would recreate the original recovery problem.
- Compaction rewrites one base and can consume device bandwidth for tens of seconds at 100 million
  entries. It is asynchronous and crash-safe, but production must observe metadata cache hits,
  metadata page reads, corruption, delta occupancy, and compaction/checkpoint latency.
- The on-disk index layout changes. Existing incompatible cache incarnations are discarded and
  rebuilt from authoritative object storage; there is no migration path for cache-only data.
