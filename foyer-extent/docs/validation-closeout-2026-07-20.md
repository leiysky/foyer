# ExtentEngine storage validation closeout

Date: 2026-07-20

This closes the synthetic development-host performance phase. ExtentEngine has enough evidence to
enter an opt-in ScopeDB production canary with BlockEngine as the rollback path. It is not yet
evidence for making ExtentEngine the unconditional default: its structural recovery and priority
advantages are verified, while storage-read p95 and p99 remain behind BlockEngine.

## Frozen implementation and environment

- Final implementation: `1e0c8b1` (`perf(fixed-lsm): optimize resident index reads`).
- Benchmark and profiling harness: `485a6a0` (`test(extent): profile index hot paths`) plus the
  final implementation above.
- Host: AWS `i8g.large`, two Arm cores, one thread per core, 36 MiB shared L3.
- Device: 435.9 GiB Amazon EC2 NVMe instance storage mounted at `/work`; all engine data was stored
  there rather than in tmpfs.
- Toolchain: `rustc 1.98.0-nightly (bd08c9e71 2026-06-25)`, `fio 3.32`.
- Access concurrency: four clients, satisfying the `2 * cores` gate.
- Engine runs used buffered I/O unless a result explicitly says direct I/O.
- A 16 GiB direct-I/O fio control with 1 MiB requests, queue depth 32, and one job measured
  518 MiB/s sequential read and 413 MiB/s sequential write. These numbers characterize this
  virtualized development device; they are not production-device targets.

## Complete storage hot path

The matched comparison reopened the existing 10-million-entry images, bypassed HybridCache memory
with `storage().load`, warmed 200,000 operations over a 10,000-key set, then measured two million
random hits with four clients. Entries were 1 KiB, the index cache limit was 1 GiB, and every
returned key, value, and priority was validated.

| Metric | ExtentEngine | BlockEngine | Extent delta |
| --- | ---: | ---: | ---: |
| Recovery for this run | 0.114 s | 4.217 s | 37.0x faster |
| Read throughput | 133,314 ops/s | 146,754 ops/s | -9.2% |
| Get p50 | 23 us | 22 us | +4.5% |
| Get p95 | 65 us | 54 us | +20.4% |
| Get p99 | 104 us | 79 us | +31.6% |
| Accounted storage reads | 3,009.4 MiB | 9,016.6 MiB | -66.6% |
| Recovery peak RSS | 140 MiB | 1,263 MiB | -88.9% |

Both sides returned two million valid hits with zero misses, errors, or invalid values. Extent's
index optimization changed the complete path from 129,917 to 133,314 ops/s (+2.6%), p50 from 24 to
23 us, and p99 from 106 to 104 us. The payload read, checksum, and scheduling path now dominates;
further constant-factor index work is not justified by the end-to-end gain.

The index is not literally free. On the same 10-million-entry image its optimized resident
index-only p50 was 1.64 us, about 7% of the complete storage p50, while Foyer's in-memory index was
still roughly 10.3x faster in a pure lookup benchmark. The production conclusion is narrower:
given enough index-cache residency, the persistent index is not the architectural bottleneck of an
Extent storage hit.

Primary evidence:

- `storage-hot-10m-extent-final-cache1024-hotset10k.log`
- `storage-hot-10m-block-hotset10k.log`
- `storage-hot-10m-extent-hotset10k.log`

## Index residency and accepted configuration

For a 10,000-key hot set, three final FixedRecordLSM runs measured 1.264, 1.250, and 1.193 Mops/s;
their throughput median was 1.250 Mops/s and median p50 was 1.641 us. The pre-optimization result
was 0.858 Mops/s at 2.370 us p50, so the retained changes improved throughput by 45.8% and p50 by
30.8%.

For a one-million-key hot set, 256 MiB was too small for resident data pages: it produced
0.587 Mops/s at 5.954/7.343 us p95/p99. A 1 GiB limit produced 1.002 Mops/s at
2.450/2.665 us p95/p99, a 70.8% throughput gain and 63.7% lower p99. Only 39 data-page misses were
recorded during its measured ten million lookups.

The accepted static policy is therefore:

- a lazy 1 GiB total index-cache limit by default;
- one eighth reserved for bounded, table-local pinned Bloom pages;
- the remaining seven eighths used as a shared evictable page cache;
- no runtime knob for this internal split, Bloom shape, or block format.

The total cache limit remains a runtime knob. It is an upper bound rather than an eager allocation,
so smaller deployments can tune it down and large ScopeDB caches can retain a larger working set.
A shared-Bloom-hash experiment improved the index-only microbenchmark by only about 3% and was
reverted under the rule that low-benefit complexity is not retained.

The final sampled profile attributed 20.4% of cycles to record search, 18.3% to table lookup, 16.4%
to Bloom probing, 13.3% to the primary compare-and-swap path, and only 3.4% to the resident data
cache lookup. No `perf.data` is retained in the archive; these percentages are diagnostic context,
not an acceptance gate.

Primary evidence:

- `index-hot-10m-fixed-final-run1.log`
- `index-hot-10m-fixed-final-run2.log`
- `index-hot-10m-fixed-final-run3.log`
- `index-hot-10m-fixed-cache256-hotset10k.log`
- `index-hot-10m-memory-hotset10k.log`
- `index-hot-10m-fixed-opt4-hotset1m-cache256.log`
- `index-hot-10m-fixed-opt4-hotset1m-cache1024.log`

## Recovery

After dropping the Linux page cache before each trial, the controlled 10-million-entry runs were:

| Trial | ExtentEngine | BlockEngine |
| --- | ---: | ---: |
| 1 | 0.141 s | 4.618 s |
| 2 | 0.140 s | 4.595 s |
| 3 | 0.141 s | 4.649 s |
| Median | 0.141 s | 4.618 s |

Extent recovery was 32.8x faster at this scale. It opens manifests, fence summaries, allocator
state, and a bounded WAL tail; detailed index and payload pages remain lazy. BlockEngine rebuilds
its in-memory index from all recovered entries, so its time and RSS scale with entry count. This is
the structural result the engine was designed to obtain.

The final Block-versus-Extent recovery experiment reached 10 million, not 100 million, entries. The
architecture removes the all-entry rebuild and the 10-million result validates the expected trend,
but this report does not present an unmeasured 100-million number as fact.

Primary evidence: the six `recovery-10m-20260720-*-cold-*.log` files.

## Priority correctness under capacity pressure

The final fresh image offered 12 GiB into a 3 GiB Extent cache using 4/16/64/256/1,024 KiB values,
32/96/256/1,024-byte keys, and the ScopeDB priority mix: 10% high, 30% normal, and 60% low. All
46,127 asynchronous puts were accepted and completed; 19,006 entries remained after reclaim. The
physical occupancy was five high segments at the five-segment floor, 37 normal segments at a
33-segment floor plus four borrowed segments, and four low segments. This verifies that floors are
minimum protection rather than fixed partitions.

Two independent strict reopens and complete four-client storage scans returned exactly the same
result:

| Priority | Offered | Hits | Retention |
| --- | ---: | ---: | ---: |
| High | 4,613 | 4,613 | 100% |
| Normal | 13,839 | 13,839 | 100% |
| Low | 27,675 | 554 | 2.0% |

Both scans reported 19,006 hits, 27,121 misses, zero read errors, and zero invalid hits. An invalid
hit means any loaded key, generated value bytes, or stored priority differed from the offered
entry. Recovery took 13 ms and 12 ms.

A second, larger full scan offered 24 GiB into 6 GiB. It retained all 9,226 high and all 27,677
normal entries, 1,390 of 55,350 low entries (2.5%), and again reported zero errors and invalid hits.
The matched smaller BlockEngine pressure run retained roughly the same fraction of every priority
(66.6% high, 67.4% normal, and 67.6% low), while Extent retained 100% high and normal. This shows
that Extent's observed result comes from priority isolation rather than uniform eviction luck.

Primary evidence:

- `priority-final-populate.log`
- `priority-final-recover-fullscan-run1.log`
- `priority-final-recover-fullscan-run2.log`
- `priority-floor-pressure-final-fullscan.log`
- `priority-floor-smoke-20260720.log`

The final code audit found the measured behavior aligned with the following implementation
invariants:

- percentage floors are rounded up to whole reclaim segments, and open rejects both percentages
  that sum above 100% and rounded floors that exceed usable segment capacity;
- a low insert may reclaim only low segments; normal and high first reclaim low, then capacity
  borrowed above the opposite protected floor, then their own oldest segment;
- priority is checksummed in both the durable index location and owner record, while exact key bytes
  remain in the checksummed stored blob;
- a read validates location bounds and generation before I/O, then checksum and generation again
  after I/O, so a reclaimed location cannot become a stale hit;
- reclaim persists source/target roles, publishes index removals or promotions, and checkpoints
  before generation reuse; failpoint tests cover interruption before and after those transitions;
- same-priority hot promotion is capped at one eighth of a source segment, while cross-priority
  reclaim performs no promotion that could prevent borrowed capacity from being repaid.

## Crash and durability boundary

Separate buffered- and direct-I/O runs killed the service process without drain or graceful close.
Strict recovery, a validated scan, an appended write wave, and a second strict recovery completed
without corrupt hits. These are process-abort tests. Direct I/O removes page-cache buffering from
payload I/O but does not emulate loss of device volatile state, controller failure, or a full power
cut; no power-loss claim is made.

The graceful-shutdown policy remains best effort: already queued writes may be discarded to bound
shutdown time, while the published durable prefix must remain recoverable. Cache misses and
throttled inserts are normal fallback outcomes, not corruption.

Primary evidence: the six `kill-priority-*20260720*.log` files.

## Decision and remaining production gates

Development-host tuning is closed. Do not continue optimizing the FixedRecordLSM hot path without
new ScopeDB evidence that index CPU or index I/O is again material. The next useful experiment is a
production-shaped, opt-in canary, not another synthetic constant-factor variant.

The canary must retain BlockEngine as a fast rollback and compare:

- statement and cache-hit p50/p95/p99/p99.9, especially the current storage-read tail gap;
- hit ratio split by high, normal, and low priority;
- object-store fallback bytes and requests, which price cache misses directly;
- recovery time and RSS after normal restart and process abort;
- asynchronous queue shedding, shutdown drops, drain throughput, and write amplification;
- corruption/reset counters and long-running reclaim/compaction stability.

The 24-hour development run was intentionally stopped in order to finish cross-engine alignment.
The i8g.large host has only two virtualized cores, no production traffic, and a different storage
stack from a ScopeDB node. Those limits, the unmeasured 100-million-entry cross-engine recovery,
the absent power-loss test, and the p95/p99 storage-read gap prevent a direct-default decision.

## Archived evidence

Raw logs, fio JSON, and text perf-stat outputs are intentionally kept outside the repository at:

```text
/Users/leiysky/.codex/archives/extent-engine/2026-07-20-i8g-large
```

The archive contains no `perf.data` and no regenerable cache image. `RAW_SHA256SUMS` authenticates
the extracted files. The compressed handoff bundle is
`extent-engine-results-20260720-no-perf-data.tar.zst`; its SHA-256 is
`6b2a70ff228e28bfb0bd214a87e3233459884b9124346a9c98ab41d281629127`.
