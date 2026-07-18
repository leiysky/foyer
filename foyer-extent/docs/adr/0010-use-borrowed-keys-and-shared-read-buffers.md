---
status: accepted
---

# Use borrowed lookups and shared cache entries

Extent's public point operations accept blob keys as borrowed byte slices. A successful read
returns the complete `Entry`; a miss returns no entry. An `Entry` owns `bytes::Bytes` handles
for its key and value together with its cache priority. Returned entries own their lifetime
independently of the engine, are immutable, and may be cloned or sliced without copying key or blob
contents.

## Why

Callers should not have to transfer ownership of their encoded cache keys. The public facade
therefore borrows a byte slice, validates it, and creates the owned `Bytes` needed by Foyer's async
lookup boundary. This is one bounded key copy; it is not retained by the caller. Conversely,
returning a borrowed value slice would couple the buffer lifetime to an internal page, lock, or
engine borrow and would make memory-cache and disk-read implementations observably different.

`Entry` gives both paths one stable value type. A memory-cache hit can clone its existing entry; a
disk read can transfer owned buffers into `Bytes`. Callers do not need to copy merely to retain,
slice, inspect priority, or fan out a cache hit.

## Consequences

- `BlobKey` remains the domain name for the complete key but need not be a public Rust struct.
- `Entry { key: Bytes, value: Bytes, priority: CachePriority }` is the core public data type for
  insertion and hits; it is not reused as a physical disk or index record.
- Point lookup has the shape `async get(&[u8]) -> Option<Entry>`. Invalid input, throttling, storage
  failure, and absence are all misses at this best-effort boundary.
- A returned `Entry` contains shared immutable buffers, not borrows into mutable engine storage.
- Key limits are checked at the engine boundary; internal index digests remain unobservable.
- Complete key equality alone defines entry identity. A successfully published replacement updates
  its value and priority as one index location; the public cache does not promise linearizable
  visibility between asynchronous put, get, and delete calls.
- Repeating the same key, value, and priority is logically idempotent and may avoid physical I/O.
  Within one ordered batch, the last successfully admitted occurrence of a key is authoritative.
- Put consumes an `Entry` and returns `()`. Whether it was admitted, rejected, throttled, queued,
  dropped, or failed is internal statistics rather than a public result. Static input validity is
  established when constructing the entry.
- The public Extent cache is a Foyer HybridCache configured with ExtentEngine. SegmentEngine is its
  internal disk tier rather than the primary public API; Extent does not reimplement Foyer's upper
  hybrid layer.
- Public get is asynchronous so a memory miss can read the disk tier. Put and delete are
  synchronous, non-waiting submissions.
- Memory and disk tiers exchange the same logical entry fields through `EngineValue`; the public
  facade reconstructs `Entry` with cheap `Bytes` clones.
