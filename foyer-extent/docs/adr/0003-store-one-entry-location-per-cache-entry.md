---
status: accepted
---

# Store one EntryLocation per cache Entry

## Context

The former ScopeDB extent adapter split every logical cache range into fixed-size physical keys. In
the 4/16/64/256/1,024 KiB acceptance schedule, one logical entry became
4.6 physical index records and owner identities on average. The sequential index journal removes the
random COW publication cost, but it still replays, locks, mutates, and looks up every part. Reads also
issue one physical Stored Entry read per part. This is now the largest known representation mismatch
between ScopeDB and the extent layout.

Naively sharding the current engine is not the next step. The two-core i8g workload is I/O-waiting,
not CPU-saturated, and fixed per-shard capacity would strand disk space under skew. A shared free
extent pool would avoid that loss but adds allocator and reclaim coordination before contention has
been demonstrated.

## Decision

Map one immutable ScopeDB byte range to one cache Entry and make that Entry the native ExtentStore record:

- Keep object-range parsing in the ScopeDB adapter and pass an opaque `EntryKey` to ExtentStore. For
  the extent layout, one index
  location names the first of a contiguous run of 64 KiB physical slots and records the full logical
  length and checksum. The expected range length remains part of lookup validation, so a different
  length at the same object offset replaces the old cache value just as it does today.
- Allocate the complete range inside one extent. If the current extent lacks enough consecutive
  slots, seal it and move to another extent; do not introduce cross-extent descriptors. With the
  1 MiB ScopeDB entry limit and 64 MiB default extents, the maximum boundary waste is 15 slots.
- Persist an owner record for every occupied slot using the same range identity. This deliberately
  keeps current-tail recovery linear and self-describing. Reclaim treats only the owner whose
  physical slot matches the index location as live, so eviction and promotion count the range once.
- Bound same-priority promotion by occupied slots rather than entry count. A promoted range is never
  split, and total promoted slots remain at most one eighth of the target extent.
- Reuse `read_run_size`, `write_run_size`, batching, checkpoint, and priority settings. Keep the
  balanced 64 KiB slot and 64 MiB extent defaults and add no range-specific tuning knob.
- Treat extent generation as an optimistic read fence. Single-Entry reads validate generation,
  verify the payload checksum and complete stored key, and recheck generation after I/O without an
  owner-file read. Foyer's engine contract loads one key at a time, so Extent keeps no parallel
  batch-read path. A reclaim that reuses a physical slot during the read therefore produces a miss
  even if the replacement payload has the same CRC32 checksum.
- Change the persistent extent incarnation and replace the part-based production path if the
  experiment passes. Use the saved part-based benchmark binary for A/B rather than retaining two
  production modes.

The durability order remains `Stored Entry payload + slot owners -> allocator state -> EntryIndex`. A
published Entry is therefore either entirely addressable at its committed extent generation or a
miss. The design does not attempt size classes or tail packing, so it removes per-part metadata and
I/O overhead but does not claim to recover the existing final-slot padding.

## Acceptance gate

The implementation must pass exact-capacity, mixed-size, repeated-reclaim, generation-fence, and
process-abort tests. On the i8g local NVMe broad-size workload it must preserve 100% high/normal
retention and return no partial or corrupt hit. Serial and concurrent phases are both required; the
concurrent phase uses at least twice the host CPU count and reports aggregate throughput plus
p50/p95/p99/max latency for foreground enqueue, background write batches, enqueue-to-publication,
logical point reads, and mixed read groups. Keep the design only if it materially improves at least
one known bottleneck—10% end-to-end throughput, 20% mixed-read p50, or a twofold reduction in index
records/recovery memory—without more than a 5% regression in the other reported throughput or p99,
full-scan, and recovery measurements. Otherwise delete the prototype.

## Evaluation

The core-times-two acceptance run used four access clients on the two-core `i8g.large`, buffered
I/O, a 64 KiB slot, a 64 MiB extent, a one-second checkpoint interval, and the same
4/16/64/256/1,024 KiB deterministic schedule for both binaries. It inserted 300 GiB into 240 GiB,
performed 4,593 mixed read groups while writes were active, issued 65,536 recovered point reads,
and then content-validated all 1,175,843 logical entries.

| Dimension | Part-based journal | Range-native journal | Result |
| --- | ---: | ---: | --- |
| Submit + drain throughput | 353.9 MiB/s | 354.8 MiB/s | Pass; +0.3% |
| Reopen | 983.49 ms | 323.67 ms | Pass; 3.0x faster |
| Point-read p50 / p99 | 0.970 / 8.853 ms | 0.973 / 8.815 ms | Pass; p99 flat |
| Mixed-group p50 / p99 | 74.44 / 473.38 ms | 63.85 / 400.11 ms | Pass; p50 -14%, p99 -15% |
| Background batch p99 | 657.70 ms | 765.81 ms | Fail; +16% |
| Enqueue-to-publication p99 | 1,144.59 ms | 1,024.94 ms | Pass, but still about one second |
| Full validated scan | 475.55 s | 480.43 s | Pass; +1.0% while retaining more bytes |
| High / normal / low byte retention | 100% / 100% / 53.5% | 100% / 100% / 55.1% | Pass |
| Reclaimed extents | 1,496 | 1,533 | Acceptable trade-off; +2.5% |
| Total engine writes | 342,353 MiB | 341,488 MiB | Pass; -0.3% |
| Index writes | 1,169.5 MiB | 109.8 MiB | Pass; -90.6% |
| Peak RSS | 980,668 KiB | 509,996 KiB | Pass; -48.0% |
| Incorrect hits / index rejection | 0 / 0 | 0 / 0 | Pass |

The 8 GiB and 50 GiB A/Bs showed the same throughput parity, roughly threefold reopen improvement,
and large index/RSS reduction. A cold-oriented 50 GiB recover-only repeat measured point-read p99
at 8.532 ms for parts and 8.541 ms for ranges, while range p50 improved from 0.462 to 0.199 ms; this
rules out the apparent one-run 50 GiB p99 regression. Raising `read_run_size` from 64 to 256 KiB
reduced physical read runs by 65% but did not improve end-to-end p99, so the balanced static default
remains 64 KiB and the existing runtime knob remains available.

An independent 300 GiB recover-only process reopened the range-native cache in 312.27 ms versus
323.67 ms in the write run and completed a second 1,175,843-entry content scan with identical
retention and no incorrect hit. Recovery is therefore stable across processes; the remaining gate
is publication latency rather than replay correctness or variance.

The representation cleared throughput, recovery, reclaim, memory, and content-correctness gates.
ADR 0004 then removed its remaining latency regression. On the same 300 GiB workload, the accepted
epoch build sustained 354.4 MiB/s, kept point p99 at 8.779 ms, reduced mixed-group p99 to 331.26 ms,
and reduced background-batch p99 to 397.02 ms. That is 40% below the saved part-based journal's
657.70 ms rather than 16% above it. Enqueue-to-publication p99 fell to 774.49 ms, also below the
part-based 1,144.59 ms. The range-native representation is therefore accepted, and no second
part-based production mode is retained.

A later concurrency audit found that the range path already applied the post-I/O generation fence,
while the crate-level point path and its small-entry batch path stopped after their pre-I/O check.
The production ScopeDB adapter used the protected range path, but the public engine contract was
still incomplete. The fix applies the same post-I/O fence to all three paths. Its deterministic
regression reuses the same physical slot between validation and payload I/O and writes a different
512-byte value with an intentionally constructed identical CRC32; the stale read must return a miss.
The formal 24-hour run was restarted with the fenced binary rather than treating the earlier
ScopeDB-only path coverage as evidence for the whole crate.
