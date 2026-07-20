---
status: accepted
---

# Decouple cache publication from durable metadata checkpoint

## Context

The original buffered path synchronized payload, allocator state, journal pages, and the journal
superblock while holding the ExtentStore mutation lock. At 300 GiB this left background-batch p99 at
766 milliseconds and enqueue-to-publication p99 at 1.02 seconds. Raising the metadata threshold
only converted frequent 0.8-second pauses into 4.1-second pauses.

Moving the same whole-file `fdatasync` to another thread was not sufficient. The monolithic data and
owner files remained mutable, so their sync chased concurrent buffered writes and produced 5.9-6.5
second outliers. An immutable metadata snapshot is useful only after its payload ordering fence is
also bounded.

## Decision

Keep one shared extent pool and one total mutation order, but separate payload durability from
allocator/index durability:

1. Each aggregated ExtentStore batch writes its payload and slot-owner records and completes one payload
   durability fence before returning from publication. This is write-on-insert at the 128 MiB
   logical batch boundary, not one sync per cache entry.
2. After the payload fence, ExtentStore advances its published epoch and returns to the Foyer flush
   worker. There is no per-insert strict mode; only `sync` and close wait for the corresponding
   durable metadata epoch.
3. The threshold and periodic triggers feed one coalescing coordinator. Under the mutation lock it
   captures the allocator image and detaches the exact index delta, rotates writers to a fresh delta,
   and releases the lock.
4. The coordinator persists the immutable allocator image and index delta outside the mutation
   lock. It never synchronizes the still-mutable payload file. A journal-generation rollover captures
   a full exact-map image; ordinary checkpoints copy only the detached delta.
5. Reclaim waits for an in-flight epoch before reusing an extent generation. It retains the existing
   synchronous reclaim transaction and fences any earlier pieces of the current ExtentStore batch before
   persisting eviction or promotion.
6. `sync`, close, and graceful shutdown wait for the latest published epoch. A coordinator failure is
   retained, detached changes are merged behind any newer per-key update, and later mutations and
   close observe the failure.

The existing 256 MiB `checkpoint_size`, one-second `checkpoint_interval`, 128 MiB
`write_batch_size`, and 256 MiB `submit_queue_size` remain the balanced static defaults and runtime
tuning boundary. A later concurrent-I/O evaluation changed only the automatic physical
`write_run_size` service quantum from 8 MiB to 1 MiB; it does not add a fence or alter the epoch
protocol. No epoch-specific configuration was added.

## Correctness invariants

- A durable index may reference only payload/owner bytes that completed their publication fence and
  an allocator image committed no later than that index generation.
- Recovery may omit a non-durable tail, but it must never return bytes for another key, range, or
  extent generation.
- A detached delta is immutable. Later writes use a fresh delta, and abort merge keeps the record with
  the greater per-key sequence.
- A checkpoint request cannot be lost: a newer requested epoch remains pending after the current
  epoch completes.
- Reclaim cannot reuse a generation while an older epoch may reference it.
- Close succeeds only after its target epoch is durable. A background failure is permanent for the
  current cache instance: existing reads remain available, later mutations fail internally, and
  close returns the retained error.

## Evaluation

The final A/B used the saved range-native binary and the accepted epoch binary on the same two-core
`i8g.large`, with four access clients, buffered I/O, a 4/16/64/256/1,024 KiB schedule, 300 GiB of
payload, 240 GiB of capacity, 4,593 mixed read groups, and a complete 1,175,843-entry scan.

| Measurement | Synchronous checkpoint | Epoch checkpoint | Change |
| --- | ---: | ---: | ---: |
| Submit + drain throughput | 354.8 MiB/s | 354.4 MiB/s | -0.1% |
| Background batch p50 / p99 | 80.50 / 765.81 ms | 352.36 / 397.02 ms | p50 +338%; p99 -48.2% |
| Enqueue-to-publication p50 / p99 | 583.88 / 1,024.94 ms | 733.69 / 774.49 ms | p50 +25.7%; p99 -24.4% |
| Point-read p50 / p99 | 0.973 / 8.815 ms | 0.971 / 8.779 ms | Flat |
| Mixed-group p50 / p99 | 63.85 / 400.11 ms | 72.62 / 331.26 ms | p50 +13.7%; p99 -17.2% |
| Reopen | 323.67 ms | 317.17 ms | -2.0% |
| Full validated scan | 480.43 s | 480.81 s | +0.1% |
| High / normal / low byte retention | 100% / 100% / 55.1% | 100% / 100% / 55.0% | Flat |
| Reclaimed extents | 1,533 | 1,533 | Flat |
| Total engine writes | 341,488 MiB | 341,469 MiB | Flat |
| Peak RSS | 509,996 KiB | 520,548 KiB | +2.1% |
| Incorrect hits / index rejection | 0 / 0 | 0 / 0 | Pass |

The p99 acceptance targets passed without a throughput, recovery, reclaim, retention, write, or
point-read regression. The consequence is deliberate and visible: every aggregated payload batch
now pays its physical durability time, converting a bursty 0.8-1.0 second tail into a stable roughly
0.35-second batch service time. This improves p99 while increasing p50. Production histograms report
both batch service and oldest-entry publication latency so a ScopeDB canary can judge that trade-off;
the runtime batch and queue controls remain available without adding another mode.

A later 8 MiB-service 50 GiB sweep doubled access concurrency from four (`core x 2`) to eight
clients without changing storage settings. Throughput remained 355.7-357.2 MiB/s, batch p99 stayed
395.5-395.6 ms, publication p99 stayed 771.2-771.5 ms, reclaim remained 302 extents, and total
engine writes remained 57,094.6 MiB. Mixed-group p99 improved from 195.2 to 57.6 ms; point-read p99
rose from 7.46 to 14.64 ms with doubled I/O occupancy. Foreground put p99 moved from 37 microseconds
to 291 milliseconds while p95 stayed below 2 microseconds, identifying bounded write-queue
backpressure rather than mutation-lock or checkpoint collapse. The production prefetch path uses
waiting admission, while query-miss fill uses non-blocking submission. The decision therefore keeps
the balanced static queue and exposes a priority-labeled write-queue-wait histogram for runtime
tuning instead of increasing the queue or sharding the engine.

A matched pre-service-quantum Foyer/Extent run at four clients also separated read hits from misses.
Foyer's logical point-read p99 was 7.024 ms but its hit-conditioned p99 was 7.436 ms because 27% of
requests were near-zero-cost misses. Extent's logical and hit-conditioned p99 were 7.477 and 7.491
ms at a 97.3% byte hit rate. Recovered-hit p99 is therefore effectively equal, rather than the
logical distribution implying an Extent regression. Under concurrent writes, however, Extent's
hit-conditioned operation p99 was 9.52 ms versus Foyer's 2.51 ms; fixed 256-operation group p99 was
203.88 versus 215.09 ms. Extent delivered 357.2 versus 388.0 MiB/s raw ingress, reopened in 38.76
versus 94.54 ms, retained 100% of high and normal bytes instead of Foyer's uniform 73.1%, and
completed with no incorrect hit or index rejection. These trade-offs require a production trace;
the engine-local result does not authorize a default switch.

The same run split hit latency by actual entry size. Recovered Extent p99 was lower than Foyer for
4/16/64 KiB (1.325/1.379/1.504 versus 1.509/1.492/1.627 ms), 18% higher at 256 KiB (4.210 versus
3.573 ms), and equal at 1 MiB (8.625 versus 8.640 ms). Under concurrent writes, Extent/Foyer p99 was
5.923/1.561 ms at 4 KiB and 17.546/4.321 ms at 1 MiB. The overlap cost therefore spans all sizes and
grows with payload I/O; it is not explained by index locking or the 64 KiB slot alone.

### Physical write service quantum follow-up

`strace` on the broad-size workload attributed 71.8% of engine write-syscall time to `pwrite64` and
28.2% to `fdatasync`; the write tail was therefore not only the required payload fence. On the same
two-core i8g NVMe host, a matched 50 GiB, four-client (`core x 2`) A/B changed only the maximum
physical write run:

| Measurement | 1 MiB run | 8 MiB run |
| --- | ---: | ---: |
| Submit + drain throughput | 355.4 MiB/s | 357.2 MiB/s |
| Recovered-hit p99 | 7.557 ms | 7.533 ms |
| Mixed hit / group p99 | 4.70 / 143.79 ms | 9.29 / 209.47 ms |
| 4 KiB / 1 MiB mixed-hit p99 | 2.055 / 7.631 ms | 5.923 / 17.546 ms |
| Batch / publication p99 | 395.65 / 770.49 ms | 395.37 / 771.56 ms |
| Data write runs | 58,027 | 8,055 |

Process write bytes were equal. Although 1 MiB produced 7.2 times as many data calls, repeated
1/2/4/8 MiB 8 GiB controls put 1 MiB on the latency/CPU Pareto frontier: mixed-hit p99 was
3.32/3.32/4.07/6.67 ms and total CPU time was 10.43/10.71/10.86/12.02 seconds. The smaller service
quantum releases the buffered device queue between writes without changing the 128 MiB batch or its
single fence. A 300 GiB/100 GiB run then completed two post-capacity turnovers, 3,146 reclaims, a
336 ms reopen, and a full 1.18-million-key validation scan without an incorrect hit or index
rejection. This evidence accepts 1 MiB as the balanced static value while retaining the existing
runtime override for device-specific tuning; it does not justify another scheduler mode or knob.

A fresh current-binary 50 GiB/37.5 GiB series then exercised the accepted 1 MiB value three times
with four clients, buffered I/O, the broad entry-size schedule, concurrent mixed reads, and
Foyer/Extent, Extent/Foyer, then Foyer/Extent isolated order. Median Extent/Foyer raw ingress was
356.3/390.7 MiB/s. Recovered-hit p99/p99.9 was effectively equal at 7.580/8.910 versus
7.357/8.848 ms. During writes, Extent/Foyer mixed-hit p99 was 3.26/2.56 ms and p99.9 was
18.08/8.32 ms, but median worst observed hit was 23.79/376.06 ms and mixed-group p99/max was
41.55/43.68 versus 213.42/388.51 ms. Extent therefore reproducibly trades a higher common overlap
tail for a much lower extreme-stall bound; it does not dominate Foyer on latency. Across the series,
Extent/Foyer throughput ranged only 354.2-356.3/390.7-391.0 MiB/s, mixed-hit p99.9
18.02-18.24/7.76-8.78 ms, and group max 43.30-43.74/333.90-432.95 ms. Extent retained 97.3%
versus 73.1% of weighted bytes, median reopen was 40.83 versus 102.65 ms, and median peak RSS was
141,548 versus 411,620 KiB. This rejects fixed engine order as the explanation, strengthens the
production-trace gate, and does not justify a new scheduler or knob.

A direct-I/O 1/2/4/8 MiB sweep kept 1 and 8 MiB throughput flat at 381.9 and 381.1 MiB/s but exposed
the expected tradeoff. The 1 MiB run had 395.61/770.52 ms batch/publication p99 versus
205.06/325.20 ms at 8 MiB, while mixed hit/group p99 improved from 53.17/303.72 to
3.11/57.30 ms. Total CPU was 11.16 versus 11.83 seconds. Two and four MiB did not dominate either
endpoint. The automatic 1 MiB value intentionally favors latency-sensitive ScopeDB reads and the
default buffered mode; a direct-I/O deployment with a measured publication SLO can reuse the
existing 8 MiB override. A mode-dependent hidden default is not justified.

With the new automatic value, an 8 GiB buffered control doubled clients from `core x 2` to
`core x 4`: throughput moved from 368.2 to 377.0 MiB/s, batch/publication p99 remained
396/770 versus 395/770 ms, and mixed-hit p99 moved from 2.87 to 3.15 ms. Foreground put p99 reached
one 313 ms queue cycle while p95 remained 2 microseconds. This repeats the larger 8 MiB-run
concurrency finding: the shared engine order remains stable, and producer backpressure belongs at
the bounded adapter queue rather than in a larger static queue or a capacity-stranding shard.

The new default also passed the two state-transition cases that a latency-only A/B does not cover.
First, a core-times-two mixed workload was killed with `SIGKILL` after 45 of 50 GiB was submitted,
without drain or close. An independent process reopened the 40 GiB cache in 39.58 ms and validated
all 195,976 workload keys; the non-durable tail was missing and no hit contained incorrect bytes.
Second, a retained 100 GiB cache started at generation 1 and 90.5% journal utilization, then accepted
enough new keys and physical data writes to finish at generation 3 and 20.5%. The two full-map
compactions kept batch/publication max at 154/134 ms. Reopen was 64.54 ms and 64.14 ms in two
processes; both 2.35-million-key scans had identical retention and no incorrect hit, index rejection,
or pending change. Recovered-hit p99 was 8.87/8.97 ms, although the independent run contained one
172 ms maximum versus 11 ms in the first sample. An explicit cold full-key-space replay returned
8.83 ms p99 and 10.05 ms max; three additional cold subsets stayed at 10.09-10.27 ms max. The outlier
is not reproducible evidence for a read scheduler. The benchmark now reports p99.9 between p99 and
max for foreground admission and reads, hit/miss and entry-size cohorts, mixed reads, batch service,
and publication. These runs directly verify crash-tail semantics and both sides of the recovery
sawtooth while leaving trace-correlated higher-percentile query latency for the canary.

A no-reclaim Extent control used 50 GiB payload and 60 GiB capacity. It sustained 365.4 MiB/s versus
357.2 MiB/s with pressure, but mixed-hit p99 worsened from 9.29 to 15.68 ms and the 1 MiB bucket from
17.55 to 46.16 ms even though batch p99 fell from 395 to 355 ms. Reclaim is not causing the read
tail. Its priority eviction reduces the effective working set and improves locality, so weakening
reclaim would trade cache value and latency for only 2.3% raw ingress.

A 600 GiB/240 GiB follow-up kept the then-accepted 8 MiB run and four-client (`core x 2`) access
while running 1.5 complete turnovers after capacity was reached. It sustained 352.2 MiB/s,
reclaimed 6,629 extents, wrote 1.086 physical bytes per logical payload byte, and completed an
independent full recovery scan without an incorrect hit. Recovered hit p99 was 8.87 ms; mixed-write
hit p99 was 25.12 ms. High/normal/low byte retention was 100%/88.4%/0% because high plus normal input
alone equaled raw cache capacity before slot-tail overhead. This confirms stable priority ordering
and reclaim progress, but reinforces write/read interference as the remaining engine-local latency
risk.

The instrumented independent reopen took 776 ms with 63,243 of 77,715 journal pages committed,
3,737,413 records, and 860,328 live entries. The earlier independent run took 1.015 seconds. Replay
latency is therefore the expected capacity-bounded journal sawtooth rather than a payload scan.
Production gauges expose journal generation, pages and capacity, records, and live entries; no new
compaction configuration is added unless a canary startup SLO shows material end-to-end value.

An independent process reopened the 240 GiB cache in 313.35 ms, used 229 MiB peak RSS, and completed
a second 479.58-second full scan with identical retention and no incorrect hit. A separate workload
was killed with `SIGKILL` after 45 GiB of a 50 GiB run, without drain or close; dirty recovery opened
in 35.22 ms and validated all surviving hits in 62.90 seconds. The non-durable tail became misses,
not wrong bytes. All 69 Linux engine tests and 37 cache tests passed, including deterministic
snapshot rotation, concurrent publication, failure retention, generation-pinned
reclaim, journal abort merge, and every existing checkpoint/reclaim crash boundary.

A real ScopeDB release replay then tested whether engine-local cache value reached the statement
boundary. On the same two-core i8g, four clients issued 12,288 validated range statements over an
8 GiB Celty table with a 512 MiB disk cache. Extent/Foyer delivered 23.367/24.290 QPS, statement
p99 278.60/265.91 ms, p99.9 332.71/305.77 ms, and max 359.44/366.75 ms. Extent sent 17.7% fewer
bytes from MinIO, but wrote 65.5% more local bytes and used 7.5% more peak RSS. The epoch design is
therefore mature enough for a canary, but it does not remove the end-to-end p99.9 and resource
trade-offs required for a default switch.

The final ScopeDB binary was then killed after four `core x 2` range statements had entered the
server concurrently. It ran no cache close or shutdown path. Reopening the same 512 MiB Extent
directory took 0.974 ms and service health returned in 70.6 ms. A recovered four-client replay
validated all 1,024 statements and 16 GiB of logical results at 24.149 QPS; p99/max was
273.37/305.10 ms. No cache, checkpoint, corruption, or index-rejection error was logged. This
promotes process-abort recovery from an engine-only result to a service-boundary result, while a
physical host power-loss test remains an explicit canary gate.

## Rejected alternatives

- Threshold enlargement moved rather than removed the synchronous work and produced 4.1-second
  pauses at 2 GiB.
- Thread handoff, request coalescing alone, and extra sync locks produced 2.6-6.0 second outliers or
  left the original durable critical section unchanged.
- Capturing immutable allocator/index state while synchronizing the monolithic payload file in the
  background produced 6.52-second publication p99 because whole-file sync chased new buffered writes.
- Moving the payload fence from every engine batch into the immutable checkpoint epoch improved a
  matched 2 GiB pilot from 385.6 to 431.2 MiB/s and reduced batch p50 from 350 to 44 ms, but reclaim
  then waited behind the in-flight epoch: batch p99 rose from 458 ms to 1.42 s, publication p99 from
  807 ms to 1.78 s, and mixed-hit p99 from 6.11 to 7.07 ms. Avoiding that wait requires quarantined
  generations plus spare-extent headroom. That added GC state is rejected until a production trace
  demonstrates enough end-to-end value to justify its correctness and capacity cost.
- Reading each range-native logical entry in one large buffered syscall reduced physical read calls
  by 78% and improved recovered-hit p99 by 11% in the 50 GiB workload. It was rejected after the
  concurrent-write runs: mixed-group p99 median regressed from 207 to 223 ms and 1 MiB mixed-hit p99
  rose from 17.5 to about 22.6 ms. The 64 KiB balanced read run avoids that device head-of-line
  blocking, while `read_run_size` remains a runtime control.
- A global Extent read-operation semaphore reused the existing cache I/O depth to prevent concurrent
  `mget` calls from multiplying blocking submissions. A recovered 1,024-query ScopeDB trial kept
  throughput at 23.82 QPS, but p99.9 stayed flat near 331 ms and peak process threads remained 118
  versus 128 before the change. Tokio retained short-lived blocking workers after their permits were
  released, so the semaphore did not achieve its resource goal. A dedicated reader pool would add
  lifecycle and scheduling complexity without a demonstrated latency or RSS benefit; the change was
  removed.
- Fixed-capacity engine sharding would strand capacity under skew and did not address file-level
  durability ordering.
- Raw Linux `sync_file_range` was rejected: it weakens the portable durability contract and does not
  provide the required device-cache ordering by itself.
