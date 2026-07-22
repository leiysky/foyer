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
- Bound foreground queues, read admission, write concurrency, and checkpoint work; keep normal
  reclaim independent of victim cardinality.
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
        -> files + IndexDB
```

1. **Cache API** — `Cache`, `CacheBuilder`, `Entry`, and `CachePriority` define the best-effort blob
   cache contract.
2. **Foyer integration** — `ExtentEngineConfig` and the internal engine adapt that contract to
   Foyer. This layer owns the put-bounded submission queue, ordered write worker, load admission,
   lifecycle translation, periodic checkpoint requests, and Foyer metrics.
3. **ExtentStore** — owns ordered disk publication, checkpoint frontiers, EntryIndex coordination,
   and invocation of the concrete Reclaimer.
4. **Persistence** — `ExtentPool` owns the payload and allocator files. `EntryIndex` owns
   digest-to-location lookup through IndexDB.
   Neither component knows about public API semantics or application key structure.

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

Reclaim persists one generation increment and never scans the victim, probes per-key liveness, or
copies payload. The generation and used-range fast path is one atomic word per cache extent, checked
before and after payload I/O. Caller priority is the physical hot/warm/cold classification, avoiding
read-frequency tracking and promotion write amplification.

### Separate volatile publication from durability

An aggregated store batch writes complete payload ranges and publishes locations to the volatile
overlay without issuing `fdatasync`. It then advances the published epoch. A checkpoint holds the
mutation order long enough to synchronize all dirty payload once and capture matching allocator
and index images; allocator and index metadata persistence continues after the lock is released.
This groups sparse-write durability, retains one total mutation order, and deliberately permits
recovery to discard the uncheckpointed tail without a per-Entry ownership sidecar.

### Keep the durable index narrow

IndexDB implements only the fixed-record point-index features Extent needs. Its recovery
opens manifests, fence summaries, and a bounded WAL tail; it does not scan payload data or rebuild a
full live map. The index capacity value is a soft planning target, while usage accounting remains
exact. Checkpoint capture tombstones stale overlay locations; older SST debt is discarded only when
an existing non-trivial compaction is already rewriting it. Garbage collection never forces an SST
rewrite or adds reclaim I/O.

### Keep resource pressure best effort

Foyer puts are fire-and-forget, every put/delete reservation is bounded, reads do not wait for
storage capacity, and low-value work may be shed. A rejected put does not manufacture a delete:
callers encode freshness in the key, and explicit delete remains a best-effort hint subject to the
same hard queue limits. This weak admission contract is paired with a strong integrity contract:
every hit must pass location, generation, value-content-digest, and complete-key checks.

## Cross-layer invariants

- A durable EntryIndex location references only payload and allocator state fenced no later than
  the corresponding index checkpoint; stale captured locations become tombstones.
- One cache-extent generation cannot be reused while an older captured epoch may still reference
  it.
- A reclaimed location is rejected from the lock-free liveness table before payload I/O; a racing
  reuse is rejected again after I/O.
- A queue reservation has exactly one owner from admission through completion or discard.
- Completion of an older same-key command cannot remove a newer pending value.
- The first background write or checkpoint error is sticky and visible to later mutation and close.
- Cumulative physical-I/O counters are reconciled exactly once; observability must not introduce
  filesystem scans or full-stat collection into foreground reads.
- Test-only fault injection and benchmark configuration do not enter production modules.

## Compatibility policy

The disk format is versioned as one layout: payload representation, allocator state, and
IndexDB compatibility move together. Format 1 may be replaced in place before its first
production freeze because development cache images are expendable. After that freeze, an
incompatible change advances `EXTENT_FORMAT_VERSION`; Extent rejects the old cache and may recreate
it because the authoritative copy remains outside the cache. The family magic and format number
must both match exactly. There is no compatibility decoder or in-place migration path.

The Foyer engine boundary is separately guarded by a compile-time API version assertion. Extent and
the workspace-pinned Foyer fork are upgraded together.

## Design document map

- [Cache contract](cache-contract.md) — logical Entry identity, ownership, best-effort semantics,
  and integrity.
- [Foyer integration](foyer-integration.md) — engine boundary, queues, read/write paths, lifecycle,
  and observability.
- [ExtentStore design](extent-store.md) — stable format 1 physical layout, checkpoint, reclaim, and failure
  model.
- [EntryIndex design](entry-index.md) — overlays, IndexDB, recovery, accounting, and rejected
  index shapes.
- [Foyer engine benchmark](foyer-engine-benchmark.md) — reproducible validation procedure.

## Deliberate non-goals

- Replacing Foyer's memory cache or hybrid coordination.
- Acting as an authoritative KV database, object store, or application range store.
- General transactions, iteration, range queries, or strong invalidation.
- Runtime selection among persisted layouts, durable index backends, or reclaim implementations.
- Recovery by payload scan or all-key rebuild.
- Hidden device-specific defaults or benchmark-only production modes.
