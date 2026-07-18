# Architecture

Extent has four dependency layers. Dependencies point downward; lower layers do not know about
Foyer or the public cache facade.

1. **Cache API** — `Cache`, `CacheBuilder`, `Entry`, and `CachePriority` define the best-effort blob
   cache contract.
2. **Foyer integration** — `ExtentEngineConfig` adapts that contract to Foyer's `Engine` boundary.
   A bounded submission queue owns backpressure reservations, one write worker owns ordered batch
   publication and periodic checkpoint requests, and the recovery-policy and statistics modules
   translate Foyer-specific behavior.
3. **Segment engine** — `SegmentEngine` owns ordered entry publication and coordinates the
   checkpoint frontier. Its concrete `Reclaimer` owns allocation pressure, priority-aware victim
   selection, generation-reuse fencing, and bounded hot-entry promotion.
4. **Persistence** — `SegmentStore` owns physical payload, owner, and allocator files;
   `SegmentIndex` owns digest-to-location lookup through FixedRecordLSM. FixedRecordLSM remains a
   separate fixed-record storage crate and has no cache or segment knowledge.

The boundaries are concrete module boundaries rather than interchangeable backend traits. There
is one accepted segment index and one physical layout. A new abstraction is justified only when it
owns an invariant or allows an accepted implementation to be replaced without exposing its
details upward.

Within the Foyer layer, a command retains its queue reservation for its entire queued/in-flight
lifetime. Within the segment layer, reclaim is invoked only while the engine mutation lock is held.
Those ownership rules are module invariants, not conventions repeated at call sites.

The pending-write keeper assigns a generation to every submission. Completion of an older
same-key write removes only its own generation and cannot erase a newer pending value. The first
background write or checkpoint failure is sticky: later submissions are shed, existing reads stay
available, and close reports the causal error. A periodic checkpoint request bounds recovery lag
when traffic never reaches the mutation-count threshold.

The Foyer integration exports queue ownership, async-write outcomes, checkpoint frontiers, and
pipeline health through the shared metrics registry. The runtime handle is a debugging and
benchmark view over the same domain state, not a separate accounting path. Storage-usage polling
does no directory walk: SegmentStore captures its fixed allocation once and FixedRecordLSM exposes
the disk budget it already maintains for admission and compaction.

Test-only injection points exercise no-space, short-write, sync, and flush-worker panic behavior.
They are compiled out of production code. A frozen full-engine V3 image is decoded and advanced by
the current engine, while subprocess crash tests cover publication boundaries that cannot be
represented by returned I/O errors.

Benchmark and reference-engine code belongs outside production modules. It may use public
observability surfaces, but production code must not depend on benchmark configuration or an
optional reference database.
