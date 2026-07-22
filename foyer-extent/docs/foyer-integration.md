# Foyer integration design

## Purpose

Extent is implemented as a Foyer disk engine, not as a replacement for Foyer's hybrid cache.
BlockEngine and ExtentEngine sit behind the same `EngineConfig` and `Engine` boundary so the memory
tier, admission policy, request coalescing, and lifecycle are shared. This keeps Extent focused on
the disk layout, durable index, recovery, and priority-aware reclaim.

The logical API is defined in [Cache contract](cache-contract.md). The disk-side state machine is
defined in [ExtentStore design](extent-store.md).

## Ownership boundary

| Component | Owns |
| --- | --- |
| Foyer `HybridCache` | Memory tier, in-flight lookup coalescing, pending-write keeper, S3FIFO policy, memory-first lookup, promotion, and shared statistics. |
| Public `Cache` facade | Entry-oriented API, builder validation, lifecycle delegation, and access to the read-only ExtentEngine handle. |
| `ExtentEngine` | Foyer engine adaptation, hard-bounded submission queue, load admission, ordered write worker, periodic checkpoint requests, recovery policy, and Foyer metrics. |
| `ExtentStore` | Ordered disk publication, checkpoint frontier, EntryIndex coordination, and reclaim orchestration. |
| `ExtentPool` / `EntryIndex` | Physical payload placement and durable digest-to-location indexing respectively. |

These are concrete ownership boundaries rather than interchangeable backend traits. Extent does not
add another memory cache, externally visible writer, read coalescer, or application-level range
layer around Foyer.

## Write path

Foyer invokes ExtentEngine through a non-blocking enqueue contract. The engine reserves both one
queue entry and its encoded-byte charge before accepting a command. That reservation remains owned
by the command for its complete queued and in-flight lifetime, preventing accounting gaps during
worker handoff.

Every put and delete reservation is bounded by the same entry and byte limits. Low- and
normal-priority puts are progressively shed before the hard byte bound, with earlier shedding while
storage reads are active. High-priority puts may use the full configured queue budget. Shedding
prevents unbounded I/O debt and does not become a per-put result. A rejected or invalid put is simply
dropped; it does not append a compensating delete that could turn overload into unbounded control
debt. Explicit delete remains a best-effort hint and may also be dropped at the hard bound.

One worker owns write order and groups commands into physical store batches. Before publication it
keeps only the final command for each complete key, writes all surviving puts in one store batch,
and applies final deletes together. This removes superseded payload writes without weakening the
hard queue bound. Because a rejected replacement does not invalidate an older disk value, mutable
callers must include freshness in the key or validate it after lookup, as required by the cache
contract.

The worker uses a larger idle batch and a smaller read-busy batch so already admitted writes make
progress without monopolizing the device. Immediate draining is the default. An optional
microbatch window can wait for sparse arrivals to join the same batch; the deadline is measured
from the first command's enqueue time, so an already-backlogged command receives no extra delay.
This can amortize page-aligned data writes; durability is grouped independently by checkpoints.
Completion is reported to Foyer's
pending-write keeper with the command generation;
completion of an older same-key write cannot erase a newer pending value.

The first write-worker or checkpoint failure is sticky. Later submissions are shed, existing reads
remain available, and wait/close report the retained causal error.

## Read path

Foyer checks and coalesces its memory tier before invoking the disk engine. ExtentEngine then:

1. validates and reconstructs the complete Entry key;
2. probes EntryIndex memory state without table I/O;
3. returns a definitive miss immediately when no in-memory record or SST range can contain the
   digest;
4. acquires a non-waiting index permit only for an unknown SST-backed result;
5. resolves the durable index without consuming payload capacity; and
6. acquires a separate non-waiting payload permit only after a location is known.

Each stage has the configured hard, non-waiting concurrency bound. Saturation returns
`Load::Throttled`, allowing the upper cache or caller to fall back instead of creating an unbounded
reader queue. A durable miss storm can consume index permits but cannot consume the permits reserved
for known payload hits. SST-backed hits and misses remain indistinguishable until the index lookup
completes.

Index state can resolve a location, a definitive miss, or an unknown SST-backed result. Only the
unknown case performs durable-index lookup. Full database, block-cache, and WAL statistics are not
collected on this foreground path; lightweight cumulative table-I/O counters are reconciled after
the operation.

## Payload I/O admission

ExtentPool supplies a cooperative synchronous scheduler below the engine queue and above positional
payload syscalls. One logical Entry read holds a lock-free read permit across all of its bounded
physical runs and never waits behind writes.

Payload write runs and checkpoint data syncs first wait for a read-quiescent point. The wait is
bounded by the configured read-priority duration, 2 ms by default;
after that bound a write proceeds even while reads remain active. Existing write concurrency is
still the hard cap, so sustained reads cannot starve publication, reclaim, sync, or close. A zero
duration bypasses this policy and its accounting.

Admission does not own buffers or emulate an asynchronous I/O queue. With the default write
concurrency of one, the caller reuses one aligned buffer and executes `pwrite` inline. Configurations
above one use a persistent bounded worker pool rather than creating and joining OS threads for each
batch. Reads and `fdatasync` remain on the admitted caller. Allocator-state
and FixedRecordLSM operations remain outside the payload scheduler because their operation classes
and durability requirements differ.

## Lifecycle

Fresh creation and strict recovery are explicit modes. Recovery reconstructs ExtentStore before the
engine accepts requests and records the result in shared metrics.

Graceful close follows this order:

1. reject new submissions;
2. allow at most the atomic batch already executing to finish;
3. discard and count the unstarted queue tail;
4. publish one final checkpoint for the completed prefix; and
5. close the underlying Foyer engine exactly once.

This bounds shutdown without exposing a partially published Entry. `wait` and close also wait for
already scheduled FixedRecordLSM maintenance so a late flush or compaction failure cannot be hidden
by an earlier WAL acknowledgement.

Online `HybridCache::clear()` is unsupported. Correct reset requires a new cache incarnation rather
than an O(live entries) tombstone pass. Close and reopen with non-recovery mode removes only
the current Extent-owned paths beneath the configured root; unrelated caller files are never
recursively deleted.

## Compatibility boundary

Extent depends on the Foyer engine API exposing complete keys and the public types needed to
implement `Engine`. A compile-time `DISK_ENGINE_API_VERSION` assertion rejects an incompatible
Foyer revision. The workspace-pinned Foyer fork and `foyer-extent` are therefore one distribution
unit.

BlockEngine behavior is not changed by Extent. The same HybridCache configuration is used for
cross-engine validation, with the selected `EngineConfig` as the principal variable. Benchmark-only
reference engines and environment parsing remain outside production modules.

## Observability

`ExtentEngineHandle` is a read-only view over the same state used by the engine. It exposes queue
ownership, asynchronous outcomes, publication and durable frontiers, physical-record occupancy and
the indexed-cardinality upper bound, physical
I/O, per-file sync counts, separate index WAL/SST/manifest writes and syncs, index flush/compaction
bytes, lazy stale-location checks/discards, immutable layout planning, checkpoint-wait and
generation-invalidation reclaim timing, scheduler waits, and the first background failure. It is
not a second control plane or a write receipt.

The shared Foyer registry exports the corresponding counters, gauges, and latency histograms.
Queue gauges change at reservation ownership boundaries, checkpoint gauges are refreshed after
batches and periodic requests, and cumulative EntryIndex I/O is reconciled exactly once under one
accounting lock.

## Rejected shapes

- Reimplementing the memory tier or hybrid coordination would duplicate Foyer and obscure disk
  engine comparisons.
- A second externally visible async writer would split ordering and queue ownership.
- Fixed-capacity engine sharding would strand disk capacity under skew without addressing shared
  file durability.
- A dedicated payload-I/O executor added a worker hop and buffer ownership without a demonstrated
  latency benefit.
- One global read semaphore lets negative index work starve known payload hits. The two-stage gates
  isolate payload capacity; deeper scheduling of FixedRecordLSM data/filter I/O still requires a
  lower operation hook.
- Static-dispatch-only integration would prevent BlockEngine and ExtentEngine from sharing the same
  runtime builder and rollback boundary.

## Validation boundary

Engine correctness and performance are evaluated through Foyer's public HybridCache and Engine
interfaces before application-specific integration. Both engines receive the same memory policy,
workload, concurrency, key/value representation, and lifecycle. Buffered and direct I/O are
reported as separate configurations. The reproducible procedure and acceptance metrics live in
[Foyer engine benchmark](foyer-engine-benchmark.md), while dated measurements remain in validation
reports rather than this design document.
