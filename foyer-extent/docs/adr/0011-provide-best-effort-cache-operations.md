---
status: accepted
---

# Provide best-effort cache operations

Extent exposes best-effort `get`, `put`, and `delete` operations rather than coherent KV-store
operations. Predictable resource use and latency take precedence over linearizable visibility,
guaranteed mutation, or total ordering.

## Contract

- Foreground put is fire-and-forget. It does not wait for admission, queue capacity, payload I/O,
  index publication, or checkpoint completion, and there is no per-put waiting variant.
- A put may be throttled, declined, or dropped when the bounded submission path is saturated
  instead of creating unbounded queueing or I/O debt. Throttling never sleeps the foreground
  caller; it suppresses background persistence for that entry.
- Delete is an eviction hint, not an invalidation barrier; returning from delete does not guarantee
  that every tier or concurrent reader has stopped exposing an older entry.
- Get has no total-order consistency guarantee with concurrent puts and deletes. It may observe an
  older or newer complete version.
- Process or machine failure may lose recent cache mutations, irrespective of whether their
  in-process result was observed.

The weak consistency contract does not weaken integrity. A hit must contain a complete,
checksummed entry whose full stored key exactly equals the requested key. Extent may return a miss
or an older complete entry, but never a torn entry, a digest-collision value, or bytes belonging to
another key.

## Caller responsibility

Delete must not be required for application correctness. A caller that caches mutable content must
put a version, generation, content identity, or equivalent freshness discriminator into the key,
or validate freshness outside Extent. ScopeDB's immutable data-object model satisfies this boundary.

## Consequences

- Cache work queues and I/O concurrency remain bounded; overload is visible as load shedding or
  throttling rather than unbounded tail latency.
- Internal admission results describe best-effort handling, not transactional commit or durable
  media completion. Queue pressure and engine work are aggregate observations, never per-put
  acknowledgements.
- Public put returns `()` and exposes neither a completion future nor an admission result.
- Foyer performs bounded memory-tier work, non-blocking disk submission, memory-first get, disk
  fallback, and promotion. ExtentEngine supplies the disk behavior only. ScopeDB does not wrap the
  configured hybrid cache with another memory tier or write queue.
- Per-operation physical details and cross-tier publication order remain internal.
- A future strong invalidation API, if ever required, must be a separately named barrier with an
  explicit cost; `delete` will not silently acquire those semantics.
