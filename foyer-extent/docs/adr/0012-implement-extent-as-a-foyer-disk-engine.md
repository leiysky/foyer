---
status: accepted
---

# Implement Extent as a Foyer disk engine

Extent will integrate as a sibling of Foyer's BlockEngine behind Foyer's `EngineConfig` and
`Engine` boundary. Foyer remains responsible for the memory cache, in-flight lookup coalescing,
pending-write keeper, foreground-asynchronous submission, shared metrics, and hybrid-cache
lifecycle. ExtentEngine remains responsible for the bounded disk flush queue, batching,
SegmentStore, FixedRecordLSM, recovery, placement, checkpointing, I/O throttling at the engine
boundary, and priority-aware reclaim.

## Why

The structural problems under investigation are in the disk layout and durable index, not in
Foyer's memory cache or hybrid coordination. Reimplementing the upper layer would duplicate mature
concurrency and policy machinery, expand the correctness surface, and confound benchmarks with
unrelated implementation differences.

Using the same Foyer upper layer makes BlockEngine and ExtentEngine directly comparable. Recovery,
write amplification, disk-hit latency, throughput, memory overhead, and load shedding can be
attributed to the disk engine selected by the same builder.

## Required Foyer boundary

Foyer 0.22.3 exposes engine injection through its hybrid builder, but an external implementation is
not yet usable without a small compatibility patch: `PieceRef` appears in the public `Engine`
trait but is not exported, and `load`/`delete` receive only a 64-bit hash. Extent needs the complete
key to derive its segment-index digest and verify precise identity.

The compatibility patch will preserve dynamic engine dispatch:

- publicly export `PieceRef` and `Populated`, both required to implement the public `Engine` trait;
- pass an owned `K` together with the existing hash to `Engine::load` and `Engine::delete`;
- after an explicit HybridCache close, skip the redundant asynchronous close in `Drop` so an
  external engine is not retained across an immediate reopen;
- leave BlockEngine behavior unchanged; and
- keep the patch isolated and suitable for upstream submission.

This is intentionally narrower than foyer-rs/foyer#1287, which also identifies precise lookup and
external engine implementation as missing capabilities but changes the engine to static dispatch.

## Boundary

The independent `extent` project owns ExtentEngine, SegmentEngine, SegmentStore, and
FixedRecordLSM. It may expose a small Entry-oriented facade over the configured Foyer HybridCache,
but it will not implement another memory cache, externally visible writer, or read coalescer.
ExtentEngine has exactly one internal, byte-bounded flush queue because Foyer's `Engine::enqueue`
contract is non-blocking and delegates disk scheduling to the selected engine.

A future ScopeDB adapter may select Foyer BlockEngine or ExtentEngine at configuration time. It
must not wrap either selection in another memory tier or asynchronous writer. ScopeDB integration
is deferred while the engine is validated directly at the shared Foyer boundary.

## Consequences

- Extent depends on Foyer's engine integration contract and must test compatibility when upgrading
  Foyer.
- A minimal local Foyer patch is required until equivalent upstream support is released; a broad
  long-lived fork is not accepted.
- Foyer's 64-bit hash may still be used for sharding and fast routing, while Extent's full digest is
  derived from the complete key for durable indexing.
- Cache priority remains part of Extent's logical Entry. Any mismatch with Foyer's memory hints is
  adapted at the boundary rather than weakening the disk-engine priority model.
- Success means replacing Foyer BlockEngine for the target workload, not replacing the whole Foyer
  library.
