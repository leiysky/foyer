# Cache contract design

## Purpose

Extent is a non-authoritative blob cache, not a coherent key-value database. Its public contract
prioritizes bounded resource use, complete-entry integrity, and cheap fallback to the authoritative
source. It deliberately does not promise that every offered mutation is admitted, persisted, or
immediately visible.

This document defines the logical contract above Foyer and the disk layout. The Foyer boundary is
described in [Foyer integration](foyer-integration.md); physical persistence is described in
[ExtentStore design](extent-store.md).

## Data model

An `Entry` contains:

- one complete, non-empty opaque key;
- one immutable, non-empty opaque blob value; and
- one caller-assigned cache priority.

The key alone defines identity. Extent does not parse object names, byte ranges, versions, or
application records. Callers encode every freshness discriminator required by their application in
the key or validate freshness after lookup.

Public keys are variable-length byte strings with a static maximum of 1 KiB. The disk index uses a
fixed 24-byte BLAKE3 digest, while the Stored Entry contains the complete original key. Every disk
hit compares that complete key before returning the value. A digest collision may cause eviction or
a miss, but it cannot produce a hit for another key.

## Ownership and lifetimes

Point operations borrow key bytes from the caller. Extent creates the owned key representation
required by Foyer only when the request crosses that boundary.

A successful lookup returns a complete `Entry` whose key and value use immutable `Bytes` handles.
The result owns its lifetime independently of cache locks, pages, and engine buffers, and callers
may retain, clone, or slice it without copying the underlying contents. The same type is returned
for memory-tier and disk-tier hits.

`Entry` is a logical cache value. It is never reused as an index record, physical allocation, or
persistence acknowledgement.

## Operation semantics

| Operation | Contract |
| --- | --- |
| `get` | Returns one complete matching Entry or a miss. It may observe an older or newer complete version during concurrent mutation. |
| `put` | Offers an Entry to the bounded cache pipeline and returns without waiting for disk admission, publication, or checkpoint durability. |
| `delete` | Provides an eviction hint. It is not an invalidation barrier and does not order concurrent readers. |
| `wait` / close | Drains the work selected by the lifecycle policy and reports retained background failure; it does not retroactively acknowledge individual puts. |

Invalid input is rejected when constructing or adapting an Entry. Runtime throttling, queue
pressure, cache absence, and storage failure are expressed as misses or aggregate observations at
the best-effort public boundary rather than as transactional outcomes.

Within one ordered engine batch, the last successfully admitted occurrence of a key is
authoritative for that batch. Repeating the same key, value, and priority is logically idempotent
and may avoid physical work. There is no linearizable visibility guarantee across asynchronous
puts, gets, and deletes.

## Integrity boundary

Weak freshness and admission semantics do not weaken hit integrity. A returned disk hit must pass
all of these checks:

1. the indexed location is within the configured layout;
2. the cache-extent generation matches before payload I/O;
3. the Stored Entry header, length, and value-content digest are valid;
4. the complete stored key equals the requested key; and
5. the generation still matches after payload I/O.

Failure of any identity or generation check is a miss. Structural corruption is reported to the
engine recovery policy; it is never converted into unverified bytes.

A process or machine failure may discard a recently published non-durable tail. Recovery may
therefore return an older complete Entry or a miss, but never a torn Entry or bytes owned by another
key or cache-extent generation.

## Priority and admission

Cache priority is the caller's explicit temperature and retention class. It affects queue shedding,
physical extent placement, protected capacity floors, and victim selection. Foreground reads do not
maintain a second frequency estimate, and reclaim never copies a hit merely to preserve it. Callers
that know data is hot use high priority; cold or speculative data uses low priority.

Admission is internal. A put can be queued, shed, dropped during shutdown, rejected by cache
policy, or fail in the background without changing the method's return type. These outcomes are
exported through metrics and the read-only engine handle so operators can observe cache value
without turning fire-and-forget puts into write receipts.

## Caller responsibilities

- The authoritative copy must remain available outside Extent.
- A miss, shed put, or lost recent mutation must be safe.
- Mutable content must include a version, generation, content identity, or equivalent freshness
  discriminator in its key unless the caller validates freshness independently.
- `delete` must not be required for application correctness.
- Callers must not interpret cache publication, queue acceptance, or storage counters as a durable
  mutation acknowledgement.

## Deliberate non-goals

- Linearizable reads or mutations.
- Per-put admission or durability receipts.
- A strong invalidation operation hidden behind `delete`.
- Transactions, compare-and-swap, iteration, range queries, or application-level range assembly.
- Exposing the internal digest or physical location in the public API.

A future strong invalidation or durability barrier would require a separately named API with an
explicit cost and consistency contract; the existing operations will not silently acquire those
semantics.
