# Extent

Extent is a standalone non-authoritative blob cache for local SSDs. This language separates its
logical cache contract from its physical allocation model.

## Language

**Extent**:
The proper name of this project. It is not the name of a logical value, physical allocation, or
storage unit.
_Avoid_: Extent key, cache extent, allocation extent

**Extent cache**:
The public hybrid cache formed by configuring Foyer's shared hybrid-cache layer with ExtentEngine
as its disk engine, exposed behind one best-effort Entry API.
_Avoid_: SegmentEngine, disk store, ScopeDB cache adapter

**Blob cache**:
A non-authoritative cache with a key-value-shaped interface that maps opaque blob keys to cache
entries. An entry may be absent or evicted, but a returned hit must match its key exactly.
_Avoid_: KV store, range store, extent store

**Blob key**:
The complete, non-empty, bounded variable-length opaque byte identity under which one cache blob
is stored and looked up. Equality is exact byte equality.
_Avoid_: Object range, range key, extent key, digest key

**Cache blob**:
The immutable, bounded variable-length byte value carried by a cache entry.
_Avoid_: Cache extent, cache chunk, mutable buffer

**Cache entry**:
The core logical cache object combining one complete blob key, one cache blob, and its cache
priority. A hit returns the complete entry as independently owned shared immutable buffers whose
handles can be cloned without copying key or blob bytes. It is not a physical index, owner, or
allocation record. Its key alone defines identity; a later accepted entry with the same key
publishes blob and priority through one index location. The best-effort API does not promise which
complete version a concurrent read observes.
_Avoid_: Index entry, owner entry, disk entry

**Allocation slot**:
The fixed-size physical allocation quantum used to store a cache blob; one cache blob may occupy
multiple contiguous slots.
_Avoid_: Extent, chunk

**Cache segment**:
A fixed-size, physically contiguous group of allocation slots that shares one reuse generation and
is reclaimed as a unit. A cache blob is wholly contained in one cache segment, while one segment
may contain many blobs.
_Avoid_: Cache blob, object range, shard

**SegmentEngine**:
The internal disk tier that indexes, places, recovers, and reclaims cache entries in cache
segments. It is not the public hybrid-cache API.
_Avoid_: Extent cache, memory cache, public engine

**Foyer disk engine**:
Foyer's pluggable disk-tier contract beneath HybridCache. BlockEngine and ExtentEngine are peer
implementations that may use different physical I/O infrastructure.
_Avoid_: Device, I/O backend, hybrid cache

**ExtentEngine**:
The Foyer disk-engine integration backed by SegmentEngine and FixedRecordLSM. It replaces Foyer's
BlockEngine, not Foyer's memory cache or hybrid-cache coordination. Its bounded flush queue is the
single disk-engine submission queue required by Foyer's non-blocking `Engine::enqueue` contract;
it is not a second public writer.
_Avoid_: Extent cache, Foyer replacement, BlockEngine fork

**Foyer engine comparison**:
An engine-level experiment in which BlockEngine and ExtentEngine are installed beneath the same
Foyer HybridCache type, policy, memory configuration, key/value model, workload, and concurrency.
ScopeDB is not part of this comparison; engine-specific physical I/O configuration is reported as
an explicit variable.
_Avoid_: ScopeDB benchmark, end-to-end query benchmark, facade comparison

**Cache priority**:
A caller-assigned protection class expressing a cache blob's relative business importance.
_Avoid_: Temperature, hotness

**Cache temperature**:
A runtime estimate of a cache blob's observed reuse, independent of its cache priority.
_Avoid_: Priority

**Priority capacity floor**:
The minimum cache-segment capacity protected for one cache priority while unused capacity remains
borrowable by more active priorities. It is a lower bound, not a physical partition or upper quota.
_Avoid_: Priority partition, pinned capacity, priority limit

**Admission**:
The internal classification of how Extent handled an offered cache entry. Queue pressure and
engine work may be observed in aggregate, but admission is never returned as a per-put result.
_Avoid_: Public put result, inserted, updated, write error

**Publication**:
The internal point at which one complete cache entry becomes visible to later cache reads. It is
not a foreground acknowledgement or a promise that the entry belongs to the recovery frontier.
_Avoid_: Commit, durable put, successful write

**Recovery frontier**:
The newest internally published cache state that Extent can reconstruct after process or machine
failure. It may lag publication because recent best-effort cache mutations are disposable; the
mutation threshold and periodic checkpoint request bound that lag during a healthy engine lifetime.
_Avoid_: Transaction commit, consistency point

**Segment index**:
The internal mapping from a blob-key digest to one physical cache-segment location. It is not the
public blob cache and must never turn a digest collision into a wrong cache hit.
_Avoid_: KV store, public index, blob-key map

**Reclaimer**:
The internal SegmentEngine component that resolves allocation pressure by selecting a cache
segment, fencing its generation reuse against the recovery frontier, evicting entries, and
optionally promoting a bounded hot subset. It is not a background compactor or a selectable
eviction policy.
_Avoid_: LSM compactor, garbage collector, eviction backend

**Best-effort cache operation**:
An operation that preserves entry integrity but may shed work, expose an older complete entry, or
have no lasting effect. Foreground put never waits for admission, queue capacity, I/O, or
checkpoint completion. It is not a durability, invalidation, or total-order consistency boundary.
_Avoid_: Transaction, committed write, invalidation barrier
