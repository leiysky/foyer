---
status: accepted
---

# Index variable-length blob keys by digest

Extent exposes blob keys as opaque byte strings with a static maximum size of 1 KiB. The segment
index and owner records retain a fixed 24-byte BLAKE3 digest rather than embedding the variable
key. Each stored blob carries its original key once, in a versioned envelope immediately before
the value. Every cache hit verifies that original key before returning the value.

## Boundary

The public cache contract borrows opaque byte slices containing 1 through 1,024 bytes. Key
interpretation, ownership, and serialization belong to the caller. Extent owns only the size
check, digest derivation, storage, and exact-match verification. `BlobKey` is a domain term, not a
required owned public Rust type.

FixedRecordLSM maps `KeyDigest -> SegmentLocation`; it does not see the public key or the blob
envelope. Segment owner records also identify allocations by `KeyDigest`, keeping their record
size fixed. SegmentStore writes and validates the complete key alongside the value.

## Why this representation

Putting variable-length keys directly in FixedRecordLSM would remove its fixed-record format,
increase compaction and recovery complexity, and multiply key storage across the index, owner
slots, and levels. Requiring callers to provide a fixed digest would instead leak an internal
storage choice into the cache API and make exact identity dependent on caller behavior.

A digest-indexed envelope preserves both useful properties: callers get a normal bounded blob-key
API, while the durable high-churn index remains fixed-width. Storing the original key once adds a
small sequential payload cost without adding random metadata I/O. The key is read in the same data
operation as the value, so a hot metadata page does not require another lookup.

## Collision semantics

A digest collision may cause one colliding cache entry to replace, evict, or mask another. This is
acceptable for a non-authoritative cache: it is equivalent to a cache miss. It must never become a
wrong hit. Exact envelope-key comparison is therefore mandatory on every read, including batched
reads and reclaim promotion.

BLAKE3 truncated to 192 bits makes accidental or adversarial collisions negligible without
changing the existing 24-byte FixedRecordLSM key width. Correctness still relies on the full-key
comparison, not on that probability.

## Consequences

- Public cache operations borrow keys as byte slices and do not allocate a key wrapper for lookup.
- A stored allocation contains a fixed envelope header, the original key, and the value; allocation
  accounting uses their combined length while cache byte statistics continue to report value
  bytes.
- The owner record stores both stored length and logical value length, so reclaim does not need to
  read payloads merely to report logical-byte statistics.
- The segment format version and ScopeDB cache incarnation must change; existing cache files are
  disposable and are not migrated.
- Raising the 1 KiB maximum is a static-format/API decision, not a runtime tuning knob.
