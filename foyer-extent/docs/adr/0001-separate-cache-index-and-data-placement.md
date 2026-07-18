---
status: accepted
---

# Separate cache index placement from data placement

## Context

ScopeDB caches immutable Celty object-range blobs whose authoritative copy remains in object
storage.
The cache needs three properties at once:

- bounded recovery I/O independent of populated entry count;
- high, normal, and low residency priorities for manifests/indexes, demand data, and prefetch;
- large sequential physical writes after the cache reaches steady-state capacity.

The original fixed engine coupled hash-bucket placement to data placement. It made lookup and
recovery simple, but a full-cache benchmark returned to nearly random 64 KiB writes. Foyer produced
large writes, but its block eviction does not implement ScopeDB's priority contract and its recovery
was proportional to more persisted metadata than ScopeDB needs.

## Decision

Keep logical indexing independent from physical placement:

- A compact checksummed copy-on-write index owns lookup, TinyLFU admission, priority, and recovery
  visibility. Keys are stored inline so collisions are distinguishable after restart. Keys within
  one 256 KiB object-offset span share a two-choice placement pair to aggregate COW page publication;
  25% index headroom absorbs the deliberate hash correlation.
- Cache blobs append to priority-separated segments. Owner records support sequential reclaim;
  location generations and checksums turn every stale or torn reference into a miss.
- A dual-copy allocator checkpoint records bounded segment state. Publication is ordered as payload,
  allocator state, then index.
- Lower-priority segments may be evicted to admit higher-priority values. Same-priority reclaim may
  promote hot entries through one persisted reserve-segment transaction.
- Keep Foyer and Extent behind one cache adapter during migration. `disk.cache.engine` selects the
  cache engine and uses a distinct disk incarnation.

## Consequences

Recovery reads bounded allocator metadata and lazily loads index buckets instead of scanning data.
Steady-state writes remain multi-megabyte runs, and priority pressure has deterministic direction.
The cost is more persistent metadata, copy amplification when hot entries are promoted, and a
serialized allocator/index publication order. The engine exports reclaim, promotion, physical-byte,
and checkpoint metrics so this cost is observable rather than implicit.

The cache is not a source of truth. After any crash, an entry may be the last checkpointed value or a
miss, but it must never be bytes belonging to another key or generation. Process-abort tests enforce
that invariant at checkpoint, compact-reclaim, and whole-segment-eviction boundaries.

## Evidence

On the Linux development SSD path with direct I/O and 512 MiB written into 384 MiB capacity,
SegmentEngine sustained 722.7 MiB/s versus 566.1 MiB/s for fixed placement and 734.7 MiB/s for
drained Foyer. At 768 MiB written into the same capacity, three-run medians were 943.2 MiB/s for
SegmentEngine, 406.8 MiB/s for fixed, and 941.5 MiB/s for drained Foyer. In the ScopeDB priority
workload, SegmentEngine reopened in 0.88 ms versus 15.40 ms
for Foyer and achieved a 96.8% versus 62.0% weighted first-read hit rate, while remaining 1.74 times
slower on submit plus drain. This supports the architecture but not an unconditional default switch;
deployment traces must show that recovery and avoided object-store reads outweigh the remaining
write-path cost.

A later sustained run used 3,276,800 16 KiB entries: 50 GiB of payload inserted into 40 GiB of
capacity. SegmentEngine retained 78.8% and reopened in 1.752 ms, while Foyer retained 64.2% and
reopened in 1,825.789 ms. Foyer's 4 KiB record alignment expands a 16 KiB value plus its key and
header to 20 KiB, accounting for the capacity loss. The default 256 MiB Segment checkpoint
threshold sustained only 74.7 MiB/s versus Foyer's 613.9 MiB/s. A 4 GiB threshold improved Segment
to 166.8 MiB/s without changing retention or bounded recovery, proving that much of the pre-capacity
cost is checkpoint policy, at the cost of allowing up to 4 GiB of recently accepted cache data to
become misses after a crash. The remaining gap after capacity pressure comes from publishing each
64 MiB victim independently. The next engine iteration should batch reclaim state transitions while
preserving the payload-before-state-before-index durability order.

The same 50 GiB workload was repeated with buffered I/O and an explicit final file sync. At the
default 256 MiB checkpoint threshold, Segment improved from 74.7 to 179.6 MiB/s; at 4 GiB it
improved from 166.8 to 316.0 MiB/s. Foyer fell from 613.9 to 479.5 MiB/s because its large block
writes already suit direct I/O. The buffered 4 GiB Segment/Foyer gap was therefore 1.52 times rather
than 3.68 times. Linux write coalescing is a useful deployment choice for the current copy-on-write
index, but it does not replace batched reclaim or a sequential persistent index log.

In the three-run ScopeDB 64 KiB priority workload, with final file sync included for both engines,
buffered Foyer and Segment medians were 2,494.5 and 3,555.2 ms. Direct medians were 2,053.5 and
3,701.9 ms. Buffered I/O narrowed the write gap from 1.80 to 1.43 times while preserving Segment's
96.8% versus 62.0% weighted hit rate and sub-millisecond reopen. Keep buffered I/O as the Segment
default; treat direct I/O as a device- and engine-specific tuning choice.

A production-shape follow-up ran on an AWS `i8g.large` with cache files on XFS-backed local NVMe
instance storage. In three buffered 16 KiB runs, each inserting 50 GiB into 40 GiB, Segment and
drained Foyer had median durable throughput of 314.2 and 318.3 MiB/s. Segment therefore came within
1.3% of Foyer without increasing its 256 MiB metadata-flush threshold. Segment retained 78.8%
versus Foyer's 64.2% and reopened in 0.601 ms versus 649.479 ms. Foyer remained 1.84 times faster on
cold local-device hit reads, so the architecture trades raw cold-read service for compact capacity,
priority, and bounded recovery rather than dominating every dimension.

The same node then ran two 64 KiB ScopeDB adapter trials, each inserting 50 GiB through
`CacheService` into 40 GiB. Durable submit-plus-close time was 137.137-137.203 seconds for Foyer and
138.310-138.318 seconds for Segment, leaving only a 0.81-0.86% Segment deficit. Foyer reopened in
187.34-194.72 ms and retained 74.9% of every priority; Segment reopened in 0.63-0.82 ms, retained
100% of high and normal entries and 65.8% of low entries, and improved the weighted hit rate from
74.8% to 98.3%. This clears the synthetic performance and recovery gate for a canary, but not the
production default gate. A trace replay and canary still need to measure object-store misses and
bytes, query tail latency, queue saturation, checkpoint latency, and write amplification on the
deployment hardware.

A 300 GiB buffered endurance run on the same local NVMe expanded logical entry sizes to
4/16/64/256/1,024 KiB and inserted them into 240 GiB of cache. Both engines completed a validated
scan of all 1,175,843 entries without corruption. Segment took 911.586 seconds for durable write
versus Foyer's 791.844 seconds, a 15.1% time cost, but reopened in 1.05 ms versus 535.87 ms and
improved priority-weighted byte hit rate from 77.3% to 97.7%. Segment retained 100% of high and
normal bytes and 55.9% of low bytes; Foyer retained 78.2% of every class. Full-scan times differed
by less than 0.5%.

The run also identifies the next architectural work. The exact mixed-size schedule consumes 8.4%
extra data slots because every range tail rounds up to 64 KiB, and `iostat` showed 41.3 KiB average
Segment writes versus 125.5 KiB for Foyer even though both moved about 386 MiB/s at the device.
Multiple size classes or tail packing should recover capacity for small ranges; a range-level
location descriptor and a more sequential metadata publication path should reduce per-64-KiB owner
and index writes. These costs make the decision a verified recovery/residency tradeoff, not proof
that Segment dominates Foyer in all workloads.

As an intermediate operational control, ScopeDB now exposes a single 4 KiB-aligned physical
`slot_size` and records it in the cache incarnation. A 300 GiB/240 GiB rerun with 32 KiB slots
retained 61.3% of low-priority bytes versus 55.9% with 64 KiB slots, raising weighted byte hit rate
from 97.7% to 98.0%. It sustained 331.9 versus 337.0 MiB/s, reopened in 1.76 versus 1.05 ms, and
used 909 MiB peak RSS. This is a capacity-biased option, not a new default.

Process-abort recovery was then compared at the same scale. Separate Segment and Foyer processes
were killed with `SIGKILL` immediately after accepting 270 GiB of the 300 GiB schedule, without
drain, close, or final sync. A fresh process reopened each dirty 240 GiB cache and validated every
surviving hit in the 1,058,259-entry submitted prefix. Segment reopened in 2.35 ms versus 901.94 ms
for Foyer, retained 100% of high and normal bytes versus Foyer's 86.9%, and produced a 98.4% versus
86.7% priority-weighted byte hit rate. Low-priority retention was 69.4% for Segment and 86.9% for
Foyer, reflecting the explicit priority-for-padding tradeoff. Complete scans took 485.429 and
483.099 seconds and found no corrupt hit. This verifies service-process crash recovery; it does not
simulate loss of the Linux page cache during a host power failure.

Mixed reads revealed a separate reclaim failure mode: promoting every hot slot can copy almost a
whole segment to free one slot. In an 8 GiB pilot the old implementation had written 132.5 GB by
90% completion. Reclaim now promotes at most the hottest one eighth of a same-priority segment,
which frees at least seven eighths and bounds promotion-only amortized data writes to 14.3%. With
one weighted read per write, the fixed 50 GiB/40 GiB run completed at 334.5 MiB/s and issued 59.44
GiB of process-accounted writes (1.189x payload), versus Foyer's 389.3 MiB/s and 50.86 GiB (1.017x).
Segment's mixed batch p50/p99 was 45.36/205.87 ms versus Foyer's 21.40/220.24 ms. It retained 100%
of high and normal bytes and reached a 97.5% final weighted byte hit rate, versus Foyer's 78.0% in
every priority. The cap makes write amplification bounded; it does not remove the local-read and
per-part metadata cost of splitting large logical ranges.

The fixed path remained stable when scaled to 300 GiB/240 GiB with the same one-read-per-write
schedule. It sustained 329.4 MiB/s, wrote 1.197x payload, and reported 41.96/243.40 ms mixed batch
p50/p99 across 4,593 batches. Reopen was 1.84 ms, final weighted byte hit rate was 97.5%, and high
and normal byte retention remained 100%. A complete 1,175,843-entry scan found no corruption. The
near-identical 50 and 300 GiB amplification demonstrates that the cap remains bounded across
repeated full-capacity reclaim cycles.

Per-file write accounting subsequently isolated index-page publication. In an 8 GiB/6 GiB
broad-size run, owners and allocator state together wrote only 10.3 MiB, while the index wrote
642.8 MiB in 135,360 calls. Range-local placement plus 25% headroom reduced that to 259.6 MiB in
61,786 calls with zero index replacement/rejection, identical priority retention, 3.9% fewer total
process writes, and 2.9% higher payload throughput. At 50 GiB/37.5 GiB it reduced process writes
from 60.21 to 57.42 GiB and improved throughput from 330.2 to 346.4 MiB/s while retaining every high
and normal byte. The index format version changed so an older cache is rejected and recreated rather
than reopened under a different placement function. The optimization does not remove fixed-slot
tail padding or per-blob owners; size classes/tail packing and a true range descriptor remain
separate work.

The selection boundary was tested directly. For complete 64 KiB entries, 64 KiB slots sustained
410.4 MiB/s versus 396.5 MiB/s for 32 KiB with identical retention. For equally frequent
4/16/32/48/64 KiB entries, 32 KiB slots sustained 276.5 versus 208.9 MiB/s and retained all normal
bytes; 64 KiB slots retained only 88.2% of normal bytes. Current Celty data fetches coalesce blocks
into ranges no larger than 64 KiB. Keep 64 KiB as the default and use the existing cache put-size
histogram to justify an explicit 32 KiB deployment setting. Multiple dynamic size classes remain
the architectural solution when one node has both substantial short-tail and full-range traffic.
