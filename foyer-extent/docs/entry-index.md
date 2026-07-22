# EntryIndex design

## Purpose

EntryIndex is ExtentStore's exact mapping from a fixed key digest to one physical EntryLocation. It
provides crash-safe point lookup and atomic mutation batches without owning payload placement,
cache priority policy, range assembly, or the public cache API.

The accepted durable implementation is the workspace-private, Rust-native `foyer-index-db` crate.
There is no runtime index selector. RocksDB remains available only behind a benchmark feature as an
industrial comparison and fallback design.

## Representation boundary

EntryIndex maps:

```text
24-byte KeyDigest -> 32-byte EntryLocation
```

The digest is a 192-bit BLAKE3 value derived internally from the complete variable-length Entry
key. The complete key is stored once with the payload and is compared on every hit, so index
collisions can cause only replacement or a miss, never a wrong value.

`EntryLocation` identifies one complete Stored Entry by byte offset, encoded length, an 88-bit
value-content digest, priority, and cache-extent generation. Its 32-byte encoding has an independent
CRC. IndexDB treats the value as opaque; EntryIndex decodes it for foreground validation and
the liveness compaction filter. The index also stores one opaque `u64`
application state used by ExtentStore for a durable indexed-cardinality upper bound. Generation
invalidation can make locations stale without mutating the index, so this count is telemetry only;
it is neither an admission limit nor an exact live-Entry count.

## Volatile overlays

EntryIndex has three logical lookup layers:

1. an active overlay containing mutations published since the latest checkpoint capture;
2. one immutable frozen overlay being persisted; and
3. the durable IndexDB base.

Overlays contain exact puts and tombstones. Checkpoint capture rotates the active overlay into the
frozen position while holding the ExtentStore mutation order, then releases that order before
durable I/O. Later writes enter a new active overlay.

Lookup checks active, frozen, and durable state in that order. A durable lookup records the base
revision, performs possible table I/O, and then rechecks the overlays. If checkpoint retirement
changed the base/overlay relationship during I/O, lookup retries. This prevents an LSM miss or old
location from hiding a concurrent overlay mutation without invalidating readers for unrelated
keys.

An I/O-free probe returns one of three states:

- a location found in an overlay or memtable;
- a definitive miss represented by a tombstone or by absence from every in-memory value and SST
  key range; or
- unknown, meaning an SST may contain the digest.

ExtentEngine uses this probe before entering its blocking pool. Bloom filters and data blocks are
consulted only for the unknown case.

## Durable engine

IndexDB is deliberately narrower than a general-purpose database. It supports fixed keys and
values, point lookup, atomic put/delete batches, a checksummed WAL, immutable SSTs, Bloom filters, a
bounded block cache, partitioned leveled compaction, alternating checksummed manifests, and one
opaque application state.

It does not expose variable record sizes, public iteration, transactions, snapshots, column
families, merge operators, TTL, compression selection, custom comparison, or pluggable compaction
policy. These omitted surfaces are part of the maintenance boundary: Extent owns a fixed-record
point index, not a reusable RocksDB replacement.

## Checkpoint publication

EntryIndex durability follows the ExtentStore publication order:

```text
data-file sync
  -> allocator state
  -> synced IndexDB mutation batch + indexed-cardinality upper bound
  -> frozen-overlay retirement
```

A crash before the LSM batch may leave unreachable physical bytes but cannot expose a location
whose allocator generation is not durable. A crash after the batch recovers only previously fenced
payload and allocator state.

Before building the durable batch, EntryIndex checks every captured location against the
lock-free extent-liveness table. A location already invalidated by reclaim is written as a
tombstone, not as stale metadata; omission would be unsafe because an older value may exist in a
lower level. If checkpoint persistence fails, the frozen overlay is merged behind newer active
mutations using per-key sequence order. The failure remains sticky for the current store instance;
later mutations and close observe it.

## Recovery

Open validates the newest usable manifest, referenced SST metadata, and a bounded WAL tail. It does
not rebuild one in-memory hash entry per live cache Entry and does not scan payload files. Fence
summaries, filters, and data pages are demand-loaded.

Incomplete final WAL frames are ignored. A checksum failure inside the durable WAL prefix, a bad
referenced SST structure, or an invalid manifest is corruption. Orphan temporary/table files are
removed only after the live version is established. Obsolete WAL generations are removed after
their records are covered; obsolete empty WALs from repeated recovery are also cleaned so
observation cost and directory size cannot grow per reopen.

The indexed-cardinality upper bound is recovered from application state rather than an all-key
scan. ExtentPool's bounded allocator state is the authoritative liveness source: every location is
checked against its extent generation before payload I/O. Recovery therefore does not need to
enumerate stale index keys.

## Generation garbage collection

Whole-extent reclaim deliberately does not install per-key tombstones. Doing so would turn one
allocator transition into index lookups, WAL writes, and an index checkpoint. Stale locations are
harmless because ExtentPool rejects their generation before payload I/O.

Captured overlay debt is removed at the next ordinary checkpoint by the liveness check described
above. Older SST debt is removed only as a side effect of work IndexDB already has to
perform. During a non-trivial compaction, the newest value for each key is decoded and checked
against the same table. An invalid location is emitted as a tombstone so an older lower-level value
cannot reappear; bottom-level compaction may omit the tombstone. A trivial move never rewrites an
SST just to run this filter. Cumulative check and discard counters expose cleanup progress without
creating a separate garbage-collection job or recovery scan.

## Read cache and accounting

The runtime index-cache limit defaults to 1 GiB and is lazy rather than eagerly allocated. One
eighth is a bounded table-local pinned-Bloom budget; the remainder is a shared evictable page cache.
Bloom pages become lock-free after their first validated load. The internal split, Bloom shape,
block format, level topology, and compaction style are fixed policy rather than runtime knobs.

IndexDB exposes two observation levels:

- lightweight cumulative table-read counters for foreground reconciliation; and
- a full explicit snapshot covering database, WAL, cache, and maintenance state.

Foreground get, write, and sync paths use only the lightweight O(1) counters. Full snapshots may
inspect WAL files and cache state and therefore belong only to explicit metrics or benchmark
collection.

## Capacity target

The cache layout derives an EntryIndex capacity target from the planned Entry count implied by the
4 KiB planning charge. The absolute cardinality bound remains the packed payload format. The target
plans space for a steady-state index, atomic compaction output, and a bounded WAL/L0 tail. WAL,
manifest, flush, and compaction reservations are all accounted against the same cumulative usage.

The target is intentionally soft. Existing usage and new reservations may exceed it; overcommit is
reported as pressure but never by itself rejects a cache write, forces a flush retry, or marks the
store unhealthy. The host filesystem is the real allocation boundary. Reservations still include
transient input/output overlap and release bytes only after obsolete files are unlinked and the
directory update is synchronized, so the reported usage remains exact.

The mutation path likewise never issues an SST point lookup merely to classify cardinality. It
uses the active overlay, memtables, and in-memory SST ranges. A possible SST match is conservatively
charged again on insert, while delete installs a tombstone without decrementing an uncertain
charge. This can only overestimate indexed cardinality; it removes read-before-write I/O from a
telemetry value and cannot change lookup correctness.

This separates planning from correctness: an estimate cannot turn temporary LSM amplification into
a cache-health failure. Unique-key churn can temporarily raise both disk usage and the indexed
cardinality upper bound until ordinary leveled compaction reaches those records; neither value is
used to reject payload publication.

## Corruption and miss policy

EntryIndex reports structural corruption precisely to ExtentStore. A key-digest lookup that returns
a location is still provisional until ExtentPool validates generation, the value-content digest,
and the complete stored key. A stale or colliding location therefore becomes a miss at the cache
boundary.

This division keeps index validation narrow while preserving the public invariant that every hit
belongs to the requested complete key.

## Why IndexDB

The selected design was compared with the following alternatives:

- **Candidate/COW index** coupled logical and physical placement, lost capacity under collisions,
  and produced random metadata publication.
- **Dual-generation full journal** made writes sequential but rebuilt the full in-memory map during
  recovery; recovery CPU and RSS scaled with live Entry count.
- **Paged base plus bounded delta** removed full-map recovery but rewrote a large immutable base and
  maintained a custom two-level compaction design. Its prototype and backend were removed.
- **RocksDB** provided stronger industrial maturity, lower mixed-write amplification in some index
  tests, and remains the reference. Its C++ build/FFI cost, broader feature surface, memory overhead,
  and integrated workload results did not justify making it the production dependency.
- **Tag plus owner pointer** appeared compact until exact-key validation required another owner
  lookup, coupling index correctness to payload placement and losing the apparent advantage.
- **Selectable backends** would preserve unvalidated code paths and multiply recovery testing. A
  materially better implementation must replace IndexDB behind the narrow boundary rather
  than become another permanent mode.

IndexDB is accepted because it bounds recovery by metadata and WAL tail, remains exact under
high churn, integrates without a C++ runtime, and keeps its responsibilities narrow. It does not
claim to dominate RocksDB on every index-only latency, space, or amplification metric.

## Deliberate non-goals

- Range lookup, range iteration, or application key interpretation.
- Payload ownership, reclaim selection, or priority policy.
- A public general-purpose storage engine API.
- Runtime selection of compaction algorithms or index backends.
- A hard cache-health boundary derived from estimated index capacity.
