# ExtentEngine storage validation closeout

Date: 2026-07-20; V5 upstream-port, large-value, and run-layout reruns: 2026-07-21

> Before the first stable release, the validated development-V6 layout was assigned stable format
> 1 and a new format-family magic. The layout and I/O paths measured below did not change; the old
> development formats remain intentionally incompatible.
>
> The original sections are the historical development-V3 baseline. The candidate validated below
> used the incompatible packed development-V5 layout. The 2026-07-21 section repeats the random
> write-throughput, capacity-retention, read-tail, and recovery gates after porting it onto upstream
> main. A fresh
> process-abort/power-loss campaign and long-running reclaim soak remain separate production gates.

Current behavior is specified by the [architecture](architecture.md),
[ExtentStore](extent-store.md), and [EntryIndex](entry-index.md) design documents. This report is
dated evidence and does not override them.

The original V3 result closed the synthetic development-host performance phase. The V5 small-value
rerun below confirms the main performance and retention conclusions on the upstream-based port; the
200 GiB large-value rerun and run-coalescing follow-up bound where those conclusions apply.
ExtentEngine has enough evidence to enter an opt-in ScopeDB production canary with BlockEngine as
the rollback path. It is not yet evidence for making ExtentEngine the unconditional default.

## V5 upstream-main port validation (2026-07-21)

### Frozen implementation and method

- Candidate: `1d6b025` on `dev/extent-engine-upstream-port`, ported from the feature branch onto
  upstream `165cde3`. The Block baseline used the same binary and generic storage-engine SPI; its
  Block algorithm was unchanged from upstream main.
- Block recovery variant: upstream PR #1296 commit `f3c6f7b`, applied only to a comparison binary.
- Host: AWS `i8g.large`, two Arm cores and 16 GiB RAM. Engine data was on XFS over local NVMe at
  `/work`.
- Each run offered 24 GiB into a 16 GiB cache with a 256 MiB memory budget, 512 MiB write queue,
  and 64 MiB write waves. Direct I/O was enabled and the Linux page cache was dropped before every
  full run and cold-recovery trial.
- Values were independently selected from 4/8/16 KiB and keys from 8/32/96/256 bytes. ScopeDB's
  60/30/10 low/normal/high priority mix was used.
- The counter-based generator visited every write exactly once in a seeded random permutation.
  Three unrelated seeds were used: `0x243f6a8885a308d3`, `0x13198a2e03707344`, and
  `0xa4093822299f31d0`. Engine order alternated Block/Extent, Extent/Block, Block/Extent.
- Four clients and four put workers were used. Each measured read phase followed 100,000 random
  storage-only warmups with 500,000 random storage-only requests.

An initial 512 MiB smoke case was rejected as a benchmark scenario because it left only six usable
extents and could not represent the configured priority floors. A corrected 2 GiB smoke case passed
before the 16/24 GiB runs. The six full runs completed about 2.696 million puts each with zero
invalid reads or read errors. Every Extent run also reported zero dropped, rejected, shed, or failed
batches in both the population and concurrent-write phases.

### Write path

The core results were stable across all three random layouts:

| Seed | Block end-to-end | Extent end-to-end | Block write amp | Extent write amp | Block peak RSS | Extent peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `243f` | 278.8 MiB/s | 375.5 MiB/s | 1.431 | 1.072 | 1,346 MiB | 721 MiB |
| `1319` | 280.3 MiB/s | 375.2 MiB/s | 1.432 | 1.072 | 1,289 MiB | 695 MiB |
| `a409` | 280.0 MiB/s | 375.0 MiB/s | 1.431 | 1.072 | 1,344 MiB | 704 MiB |

Median decomposition identifies where the difference occurs:

| Metric | Block | Extent | Extent delta |
| --- | ---: | ---: | ---: |
| Foreground submission | 3,927.3 MiB/s | 3,459.2 MiB/s | -11.9% |
| Foreground time | 6.258 s | 7.105 s | +13.5% |
| Drain time | 81.506 s | 58.412 s | -28.3% |
| End-to-end time | 87.780 s | 65.497 s | -25.4% |
| End-to-end throughput | 280.0 MiB/s | 375.2 MiB/s | +34.0% |
| Physical write traffic | 35,180.0 MiB | 26,333.6 MiB | -25.1% |
| Write amplification | 1.431 | 1.072 | -25.1% |
| Physical write operations | 35,214 | 36,853 | +4.7% |
| Full-run peak RSS | 1,344 MiB | 704 MiB | -47.6% |
| Allocated disk footprint | 16,320.0 MiB | 15,712.5 MiB | -3.7% |

Extent does not win at API submission: its foreground phase is 0.847 seconds slower. It wins in
the asynchronous drain, which is 23.094 seconds shorter and writes 8,846.4 MiB fewer bytes. That
more than pays back the foreground cost and accounts for the end-to-end improvement. Extent issues
4.7% more physical writes despite writing fewer bytes, so its average write is smaller; the 34%
gain on this NVMe host must not be projected unchanged onto an IOPS-limited device.

Extent's median physical-write breakdown was 25,722.0 MiB of framed data, 170.1 MiB of directory
traffic, 434.9 MiB of index traffic, and 6.9 MiB of allocator traffic. Directory, index, and
allocator traffic total 611.9 MiB, or 2.49% of offered payload and 2.32% of Extent physical writes.
The index had a median 318 false positives in 628,583 measured filter checks (0.051%).

V5's same-value content digest does not enlarge the 32-byte index location or 64-byte directory
record, so it adds zero persistent bytes per entry. The separate paired three-seed validation in
the 2026-07-21 content-digest archive measured the digest candidate 3.4% faster on initial tmpfs
writes and 10.8% faster on same-value rewrites than the collision-safe full-payload-read baseline.
All rewrite passes issued zero physical reads and writes. Its 8 GiB scaling run repeated two full
same-value passes with zero physical I/O. Those paired results find no measurable write-throughput
regression from the digest; the 611.9 MiB above is total Extent metadata traffic, not incremental
digest storage.

### Capacity retention and reads

The read workload samples the complete offered key universe after capacity pressure, so a miss is
primarily an eviction outcome rather than storage-read latency. Median results were:

| Metric | Block | Extent | Extent delta |
| --- | ---: | ---: | ---: |
| Overall hit ratio | 19.6% | 60.7% | +41.1 percentage points |
| Low-priority hit ratio | 19.5% | 34.5% | +15.0 percentage points |
| Normal-priority hit ratio | 19.7% | 100.0% | +80.3 percentage points |
| High-priority hit ratio | 19.6% | 100.0% | +80.4 percentage points |
| All-request throughput | 180,269 ops/s | 54,100 ops/s | -70.0% |
| Storage-hit p99 | 152 us | 157 us | +3.3% |
| Mixed hit/miss get p99 | 131 us | 150 us | +14.5% |
| Hit-p99 inflation during a 64 MiB write burst | 1.014x | 1.014x | equal |

The all-request throughput number is not an equal-I/O read comparison. Extent retained 3.1 times
as many sampled keys and therefore performed about four times as many direct data reads, while a
Block miss returns without payload I/O. On requests that actually hit storage, p99 differs by only
five microseconds. The material read-path advantage is retention: Extent preserved every sampled
normal- and high-priority key while Block evicted priorities uniformly. A hit-only throughput test
is still required when comparing raw storage-read service capacity on a production device.

### Cold recovery and upstream PR #1296

Each variant reopened the exact image produced by its seed three times, with the page cache dropped
before each trial. Values below are the per-seed medians:

| Seed | Main Block | PR #1296 Block | Extent | Main Block RSS | PR #1296 RSS | Extent RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `243f` | 0.421 s | 0.352 s | 0.160 s | 55 MiB | 37 MiB | 151 MiB |
| `1319` | 0.489 s | 0.406 s | 0.164 s | 91 MiB | 58 MiB | 153 MiB |
| `a409` | 0.475 s | 0.405 s | 0.160 s | 91 MiB | 58 MiB | 151 MiB |

PR #1296 shortens Block recovery by 14.7-17.0% across the three images; the median paired gain is
16.4%. It reduces recovery RSS by 32.7-36.3%. This patch is worth rebasing for the Block rollback
path even though it does not change Block's write amplification or priority-blind retention.

Extent recovery is 62.0-66.5% shorter than main Block, or 2.6-3.1 times faster at this scale. Its
standalone cold-recovery RSS is consistently 151-153 MiB, higher than both Block variants, despite
using 47.6% less peak RSS over the complete write/read process. Production evaluation must keep
recovery latency and recovery RSS as separate metrics rather than assuming that one predicts the
other.

### 200 GiB bounded log-normal large-value validation

The second V5 campaign tested whether the small-value result survives a ScopeDB-like large-value
tail. A symmetric Gaussian is not a well-defined size model from only a median and maximum, and it
admits negative samples. The benchmark therefore used a positive bounded log-normal distribution:
minimum 1 KiB, p50 64 KiB, unbounded p99.9 mapped to 1 MiB, and a hard 1 MiB maximum. Its 65,536
entry quantile table is deterministic and fingerprinted as `3f1fa85d4f9ce725`.

- Baseline candidate: `a1fb70a`; the binary SHA-256 was
  `3ccdb8e3c8035c92235b729e3d935e0487601017e27b8b7d57f96571facfee78`.
- Run-layout candidate: `084c734`; the binary SHA-256 was
  `66ec659257e8b4a4e7e2da57b1e51056afa13d002ad4a7753718b16241ca57bc`.
- Each run offered 200 GiB into a 128 GiB cache, retaining the 256 MiB memory budget, 512 MiB write
  queue, 64 MiB write wave, direct I/O, key-size distribution, priority mix, concurrency, random
  permutation, warmup, and measured-read count from the smaller campaign.
- The same three seeds produced 2,197,844, 2,201,993, and 2,200,252 entries. Observed
  p50/p95/p99/p99.9/max sizes were 64/280/514-517/1024/1024 KiB.
- Engine order alternated for the first two seeds. The third seed ran Block and Extent in separate
  processes so their full-run high-water RSS values would be independent. The page cache was
  dropped before every independent full run and every cold-recovery trial.

Write medians show that Extent's advantage is workload-dependent:

| Metric | Block | Extent | Extent delta |
| --- | ---: | ---: | ---: |
| Foreground time | 10.698 s | 9.850 s | -7.9% |
| Drain time | 521.465 s | 525.667 s | +0.8% |
| End-to-end time | 532.163 s | 535.249 s | +0.6% |
| End-to-end throughput | 384.8 MiB/s | 382.6 MiB/s | -0.6% |
| Physical write traffic | 210,268.0 MiB | 211,302.6 MiB | +0.5% |
| Write amplification | 1.027 | 1.032 | +0.5% |
| Physical write operations | 41,127 | 264,701 | +543.6% |
| Allocated footprint | 130,433.2 MiB | 124,415.0 MiB | -4.6% |
| Independent full-run peak RSS (`a409`) | 1,692 MiB | 861 MiB | -49.1% |

The result does not indicate a capacity-limit rejection bug: all three Extent runs accepted and
completed every initial put with zero dropped, shutdown-dropped, shed, storage-rejected, or failed
batches. Large values instead amortize Block's fixed per-record overhead. Extent's median directory,
index, and allocator traffic totals only 860.7 MiB, or 0.42% of offered payload, but its physical
write shape is much finer. Block averaged 5.11 MiB per write operation; Extent averaged 0.80 MiB
and issued 6.44 times as many operations. On this NVMe device that produces a small 0.6%
end-to-end regression rather than the small-value workload's 34% gain.

Retention remains the architectural advantage, while the large-value read path exposes a new gate:

| Metric | Block | Extent | Extent delta |
| --- | ---: | ---: | ---: |
| Overall hit ratio | 51.6% | 59.5% | +7.9 percentage points |
| Low-priority hit ratio | 51.7% | 32.2% | -19.5 percentage points |
| Normal-priority hit ratio | 51.5% | 100.0% | +48.5 percentage points |
| High-priority hit ratio | 51.8% | 100.0% | +48.2 percentage points |
| Hit-payload throughput | 489.5 MiB/s | 481.7 MiB/s | -1.6% |
| Storage-hit p50 | 666 us | 555 us | -16.7% |
| Storage-hit p95 | 1,540 us | 1,993 us | +29.4% |
| Storage-hit p99 | 2,247 us | 3,488 us | +55.2% |
| Storage-hit p99.9 | 2,986 us | 6,624 us | +121.8% |
| Physical read operations | 258,135 | 601,857 | +133.2% |

Extent performed about 2.02 payload runs per hit and covered 24.8 4 KiB data frames per hit, while
Block issued one physical read per hit. The frame count measures byte-range coverage, not 24.8
separate reads. Extent's index read only about 7.4 MiB in 941 operations at the median and
its false-positive rate was 0.049%, so the index is not the source of the tail.

#### Run-layout follow-up

The durable layout was already contiguous: one EntryIndex location names one packed Stored Entry
range. The defect was a runtime 64 KiB read-run maximum inherited from the former fixed-slot
layout. A common 64 KiB value exceeds that limit after its Stored Entry header, key, and direct-I/O
alignment are included, so one logical hit was split into multiple positional reads.

The tuning A/B populated one 20 GiB randomized image per seed into a 16 GiB cache, then reopened the
same image with 64 KiB and 2 MiB read limits. The page cache was dropped before every run. Each
trial used 100,000 randomized storage-only warmups followed by 500,000 randomized storage-only
requests. Medians across the same three unrelated seeds were:

| Read-run maximum | Payload calls/hit | Hit payload | Hit p50 | Hit p95 | Hit p99 | Hit p99.9 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 KiB | 2.025 | 481.5 MiB/s | 563 us | 2,005 us | 3,557 us | 6,692 us |
| 2 MiB | 1.000 | 481.7 MiB/s | 678 us | 1,553 us | 2,238 us | 2,972 us |
| Delta | -50.6% | +0.0% | +20.4% | -22.5% | -37.1% | -55.6% |

The 2 MiB limit is large enough to cover the configured 1 MiB value cap plus metadata and both
alignment fragments, and it produced exactly one payload call for every hit in all three seeds.
The throughput remained device-bandwidth-bound. The p50 tradeoff is real, but 678 us is close to
Block's 666 us on the full-scale control, while the old Extent p50 advantage came with a severe
multi-call tail.

The `a409` tuning image also tested 256 KiB and 1 MiB limits. They produced 1.074 and 1.001 payload
calls per hit respectively; 2 MiB was retained because it was the only tested limit with exactly
one call per hit and had the best p99.9. This changes only runtime I/O slicing and does not advance
the persistent format.

Write runs were tested separately on the same 20 GiB distribution:

| Write-run maximum | Payload runs | Total physical runs | End-to-end | Peak RSS |
| --- | ---: | ---: | ---: | ---: |
| 1 MiB | 22,192 | 25,963 | 401.1 MiB/s | 399 MiB |
| 4 MiB | 6,862 | 10,678 | 401.5 MiB/s | 413 MiB |
| 8 MiB | 4,294 | 8,052 | 401.6 MiB/s | 440 MiB |

Larger write runs removed 69.1-80.6% of payload calls but improved throughput by at most 0.12% and
raised peak RSS by 14-41 MiB. The write default therefore remains 1 MiB; reducing syscall count
alone is not a sufficient reason to increase a synchronous write's scheduler occupancy.

Finally, the optimized candidate repeated the complete 200 GiB `a409` run with a 2 MiB read limit
and the unchanged 1 MiB write limit. This is one full-scale confirmation rather than a new
three-seed write result. Population completed at 382.8 MiB/s with 1.032 write amplification and
868 MiB peak RSS, effectively unchanged from the baseline candidate's 382.6 MiB/s, 1.032, and
861 MiB. The primary read comparison on the matched seed was:

| Metric | Block | Extent, 64 KiB | Extent, 2 MiB |
| --- | ---: | ---: | ---: |
| Payload calls/hit | 1.000 | 2.022 | 1.000 |
| Hit-payload throughput | 489.5 MiB/s | 481.7 MiB/s | 481.7 MiB/s |
| Storage-hit p50 | 666 us | 561 us | 670 us |
| Storage-hit p95 | 1,540 us | 1,993 us | 1,535 us |
| Storage-hit p99 | 2,247 us | 3,537 us | 2,197 us |
| Storage-hit p99.9 | 2,986 us | 6,615 us | 2,921 us |

Compared with Block, optimized Extent was +0.6% at p50 and -0.3%, -2.2%, and -2.2% at p95, p99,
and p99.9. Two additional cold read trials kept p50 within 668-669 us, p95 within 1,537-1,542 us,
p99 within 2,228-2,233 us, and p99.9 within 2,914-2,969 us, with exactly one payload call per hit
and zero read errors or invalid values. A concurrent 64 MiB write burst retained 1.000x hit-p99
inflation, accepted all 717 puts, and reported zero drops or failures. The synthetic large-value
read-tail blocker is therefore closed. Production evaluation must still record latency by value
size and payload calls per hit so a different value cap or device can invalidate this tuning
explicitly.

Three cold opens per engine and seed had global medians of 1.703 seconds and 197 MiB for Block,
versus 0.021 seconds and 68 MiB for Extent. Extent was about 81.1 times faster and used 65.5% less
recovery RSS at this scale. All measured reads returned zero errors and zero invalid values.

The generated images were deliberately deleted after the recovery trials. Raw logs, deterministic
scenario metadata, and checksums are retained outside the repository at:

```text
/Users/leiysky/.codex/archives/extent-engine/2026-07-21-lognormal-200g
/Users/leiysky/.codex/archives/extent-engine/2026-07-21-run-layout-optimization
```

### V5 port decision

The upstream-based port passes the synthetic correctness, capacity-retention, and bounded-recovery
gates. Its write result is not universal: Extent is 34.0% faster for the 4/8/16 KiB workload and
0.6% slower for the bounded large-value workload. Priority-aware retention remains valuable, but
the 2 MiB read-run limit now brings the bounded large-value workload's p95 and higher latency to
parity with Block. The Block #1296 recovery patch should be retained during rebase. Extent remains
an opt-in canary candidate until production value-size-weighted metrics confirm the synthetic
result and the port has fresh process-abort coverage and a long reclaim soak.

Raw logs and checksums are intentionally outside the repository at:

```text
/Users/leiysky/.codex/archives/extent-engine/2026-07-21-upstream-port
```

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
physical occupancy was five high extents at the five-extent floor, 37 normal extents at a
33-extent floor plus four borrowed extents, and four low extents. This verifies that floors are
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

- percentage floors are rounded up to whole reclaim extents, and open rejects both percentages
  that sum above 100% and rounded floors that exceed usable extent capacity;
- a low insert may reclaim only low extents; normal and high first reclaim low, then capacity
  borrowed above the opposite protected floor, then their own oldest extent;
- priority is checksummed in both the durable index location and owner record, while exact key bytes
  remain in the checksummed Stored Entry;
- a read validates location bounds and generation before I/O, then checksum and generation again
  after I/O, so a reclaimed location cannot become a stale hit;
- reclaim persists source/target roles, publishes index removals or promotions, and checkpoints
  before generation reuse; failpoint tests cover interruption before and after those transitions;
- same-priority hot promotion is capped at one eighth of a source extent, while cross-priority
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
