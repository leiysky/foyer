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
4. **Persistence** — `SegmentStore` owns physical payload, owner, and allocator files. Its
   cooperative I/O scheduler admits payload reads, publication writes, and payload syncs without
   owning buffers or executing work on another thread. `SegmentIndex` owns digest-to-location
   lookup through FixedRecordLSM. FixedRecordLSM remains a separate fixed-record storage crate and
   has no cache or segment knowledge.

The boundaries are concrete module boundaries rather than interchangeable backend traits. There
is one accepted segment index and one physical layout. A new abstraction is justified only when it
owns an invariant or allows an accepted implementation to be replaced without exposing its
details upward.

Within the Foyer layer, a command retains its queue reservation for its entire queued/in-flight
lifetime. Within the segment layer, reclaim is invoked only while the engine mutation lock is held.
Those ownership rules are module invariants, not conventions repeated at call sites.

I/O admission is intentionally below Foyer and above positional file calls. A logical blob read
holds one lock-free read permit across all of its bounded physical runs. A write run waits for a
read-quiescent point for at most the configured read-priority duration, then proceeds; reads never
queue behind an admitted write. The default synchronous implementation keeps syscall execution and
buffer ownership on the caller. It is not an emulated submission/completion queue and does not
pre-commit Extent to io_uring.

The pending-write keeper assigns a generation to every submission. Completion of an older
same-key write removes only its own generation and cannot erase a newer pending value. The first
background write or checkpoint failure is sticky: later submissions are shed, existing reads stay
available, and close reports the causal error. A periodic checkpoint request bounds recovery lag
when traffic never reaches the mutation-count threshold.

The Foyer integration exports queue ownership, async-write outcomes, checkpoint frontiers, and
pipeline health through the shared metrics registry. The runtime handle is a debugging and
benchmark view over the same domain state, not a separate accounting path. The public cache facade
retains and exposes that handle, shared Foyer statistics, and storage usage after consuming the
engine config. Physical-I/O accounting includes payload and FixedRecordLSM index work. A payload
read is counted even when generation or key validation turns it into a cache miss; cumulative index
counters are reconciled under one accounting lock so concurrent readers report every delta once.
Storage-usage polling does no directory walk: SegmentStore captures its fixed allocation once and
FixedRecordLSM exposes the disk budget it already maintains for admission and compaction.

Test-only injection points exercise no-space, short-write, sync, and flush-worker panic behavior.
They are compiled out of production code. A frozen full-engine V3 image is decoded and advanced by
the current engine, while subprocess crash tests cover publication boundaries that cannot be
represented by returned I/O errors.

Benchmark and reference-engine code belongs outside production modules. It may use public
observability surfaces, but production code must not depend on benchmark configuration or an
optional reference database.
