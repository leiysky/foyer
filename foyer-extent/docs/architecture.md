# Architecture design

## Purpose

Extent is a standalone, non-authoritative blob cache for local SSDs. It combines Foyer's hybrid
cache coordination with a disk engine designed for bounded recovery, priority-aware residency, and
large append-oriented payload publication.

The architecture separates logical cache identity, Foyer integration, durable indexing, and
physical placement. Those boundaries allow Extent to change its disk representation without
duplicating the memory tier or leaking physical concepts into the public API.

## Design goals

- Return only complete, exactly keyed Entries; stale or corrupt physical state becomes a miss or a
  reported recovery error, never a wrong hit.
- Keep recovery independent of payload size and avoid rebuilding one in-memory record per live
  Entry.
- Preserve high- and normal-priority working sets under capacity pressure while keeping unused
  capacity borrowable.
- Bound foreground queues, read admission, write concurrency, checkpoint work, and reclaim
  amplification.
- Publish payload in large append-oriented runs while retaining point lookup through one exact
  EntryIndex location.
- Keep BlockEngine and ExtentEngine comparable and selectable behind the same Foyer HybridCache
  boundary.
- Make overload, background failure, physical I/O, recovery, and reclaim observable without adding
  per-put acknowledgements.

## Dependency layers

Dependencies point downward. Lower layers do not know about Foyer or the public cache facade.

```text
Cache API
  -> Foyer integration
    -> ExtentStore
      -> ExtentPool + EntryIndex
        -> files + FixedRecordLSM
```

1. **Cache API** — `Cache`, `CacheBuilder`, `Entry`, and `CachePriority` define the best-effort blob
   cache contract.
2. **Foyer integration** — `ExtentEngineConfig` and the internal engine adapt that contract to
   Foyer. This layer owns the put-bounded submission queue, ordered write worker, load admission,
   lifecycle translation, periodic checkpoint requests, and Foyer metrics.
3. **ExtentStore** — owns ordered disk publication, checkpoint frontiers, EntryIndex coordination,
   and invocation of the concrete Reclaimer.
4. **Persistence** — `ExtentPool` owns payload, Entry-directory, and allocator files. `EntryIndex`
   owns digest-to-location lookup through FixedRecordLSM. Neither component knows about public API
   semantics or application key structure.

These are concrete module boundaries rather than interchangeable backend traits. A new abstraction
is justified only when it owns an invariant or allows an accepted implementation to be replaced
without exposing its details upward.

## Core design choices

### Separate identity from placement

One logical Entry has one EntryIndex location and one Stored Entry allocation. The index does not
choose physical placement, and the allocator does not interpret keys. This avoids coupling hash
distribution to disk layout and lets reclaim operate on sequential cache extents.

Complete keys remain in Stored Entries while the fixed-width index uses digests. Exact-key
verification keeps collisions inside the miss boundary.

### Reclaim cache extents, not Entries

A cache extent is the append, seal, generation, and reclaim unit. Stored Entries are packed into
byte ranges inside one cache extent; Entry allocations and I/O frames are not independently
reclaimed. Priority capacity floors are logical protection rather than fixed partitions, so unused
capacity can be borrowed and later repaid.

### Separate publication from metadata checkpointing

An aggregated store batch fences payload and Entry-directory bytes before advancing its published
epoch. Allocator and index state are captured immutably and persisted outside the mutation lock.
This removes metadata checkpoint I/O from the foreground critical section while retaining one total
mutation order and generation-safe reclaim.

### Keep the durable index narrow

FixedRecordLSM implements only the fixed-record point-index features Extent needs. Its recovery
opens manifests, fence summaries, and a bounded WAL tail; it does not scan payload data or rebuild a
full live map. The index capacity value is a soft planning target, while usage accounting remains
exact.

### Keep resource pressure best effort

Foyer puts are fire-and-forget, put reservations are bounded, reads do not wait for storage
capacity, and low-value work may be shed. Ordered deletes may temporarily exceed the put budget to
hide an older value after a rejected update; queue gauges make this non-blocking control debt
visible. This weak admission contract is paired with a strong integrity contract: every hit must
pass location, generation, value-content-digest, and complete-key checks.

## Cross-layer invariants

- A durable EntryIndex location references only payload and allocator state fenced no later than the
  corresponding index checkpoint.
- One cache-extent generation cannot be reused while an older captured epoch may still reference
  it.
- A queue reservation has exactly one owner from admission through completion or discard.
- Completion of an older same-key command cannot remove a newer pending value.
- The first background write or checkpoint error is sticky and visible to later mutation and close.
- Cumulative physical-I/O counters are reconciled exactly once; observability must not introduce
  directory scans or full-stat collection into foreground reads.
- Test-only fault injection and benchmark configuration do not enter production modules.

## Compatibility policy

The disk format is versioned as one layout: payload representation, Entry directory, allocator
state, and FixedRecordLSM compatibility move together. An incompatible change advances
`EXTENT_FORMAT_VERSION`; Extent rejects the old cache and may recreate it because the authoritative
copy remains outside the cache. There is no selectable legacy format or in-place migration path.

The Foyer engine boundary is separately guarded by a compile-time API version assertion. Extent and
the workspace-pinned Foyer fork are upgraded together.

## Design document map

- [Cache contract](cache-contract.md) — logical Entry identity, ownership, best-effort semantics,
  and integrity.
- [Foyer integration](foyer-integration.md) — engine boundary, queues, read/write paths, lifecycle,
  and observability.
- [ExtentStore design](extent-store.md) — V5 physical layout, checkpoint, reclaim, and failure
  model.
- [EntryIndex design](entry-index.md) — overlays, FixedRecordLSM, recovery, accounting, and rejected
  index shapes.
- [Foyer engine benchmark](foyer-engine-benchmark.md) — reproducible validation procedure.
- [Storage validation closeout](validation-closeout-2026-07-20.md) — dated evidence and remaining
  production gates; it is not a source of current design truth.

`CONTEXT.md` is the canonical glossary for these documents. Design documents explain behavior and
rationale; the glossary only defines stable domain terms.

## Deliberate non-goals

- Replacing Foyer's memory cache or hybrid coordination.
- Acting as an authoritative KV database, object store, or application range store.
- General transactions, iteration, range queries, or strong invalidation.
- Runtime selection among legacy layouts, durable index backends, or reclaim implementations.
- Recovery by payload scan or all-key rebuild.
- Hidden device-specific defaults or benchmark-only production modes.
