# Extent

Extent is a standalone, non-authoritative blob cache for local SSDs. Its language keeps the public
cache contract, Foyer integration, logical disk store, and physical extent pool distinct.

## Language

### Public cache contract

**Extent**:
The proper name of this project, derived from its cache-extent storage model. Use **cache extent**,
not bare “extent”, for one physical reclaim unit.
_Avoid_: Extent key, Extent entry, Extent value

**Cache** (`Cache`):
The public hybrid facade combining Foyer's memory tier and ExtentEngine's disk tier behind the
best-effort Entry API.
_Avoid_: Store, engine, disk cache

**Blob cache**:
A non-authoritative cache with a key-value-shaped interface for opaque byte values. The source of
truth remains outside Extent, and a miss or shed operation falls back to that source.
_Avoid_: KV database, range store, object store

**Entry** (`Entry`):
The core logical cache object: one entry key, one blob value, and one cache priority. Its key alone
defines identity, and successful `get` returns the complete Entry through cheap shared byte handles.
_Avoid_: Blob, index record, owner record

**Entry key** (`EntryKey` internally):
The complete, non-empty, bounded variable-length opaque byte identity of one Entry.
_Avoid_: Blob key, range key, extent key, digest key

**Blob**:
The immutable, non-empty, bounded variable-length byte value carried by an Entry. “Blob” names only
the value, never the key-plus-value object or a physical allocation.
_Avoid_: Entry, stored entry, cache extent

**Stored entry**:
The complete encoded representation of one Entry in the disk tier, including its exact key so a
digest collision can never produce a wrong hit.
_Avoid_: Blob, index record, extent

### Foyer integration

**Foyer disk engine**:
Foyer's pluggable disk-tier contract beneath `HybridCache`; BlockEngine and ExtentEngine are peer
implementations.
_Avoid_: Cache, store, device, I/O backend

**ExtentEngine** (`ExtentEngineConfig`, internal `ExtentEngine`):
The Foyer disk-engine adapter. It owns Foyer-facing submission, load, delete, wait, close, recovery
policy, and metrics, and delegates disk state to ExtentStore.
_Avoid_: Cache, ExtentStore, Foyer replacement

**ExtentEngineHandle**:
A read-only runtime observation surface for an attached ExtentEngine. It does not acknowledge
best-effort puts or provide a second control plane.
_Avoid_: Store handle, admin API, write receipt

### Logical disk store

**ExtentStore**:
The internal disk-side key-to-Entry core. It owns ordered publication, EntryIndex coordination,
checkpoint frontiers, and reclaim orchestration over one ExtentPool.
_Avoid_: Engine, Cache, ExtentPool

**EntryIndex**:
The internal mapping from an entry-key digest to one EntryLocation. It must verify the complete key
from the Stored Entry before returning a hit.
_Avoid_: Public KV interface, blob-key map, payload store

**Reclaimer**:
The ExtentStore component that resolves allocation pressure by choosing a cache extent, fencing
generation reuse, evicting entries, and optionally promoting a bounded hot subset.
_Avoid_: LSM compactor, generic garbage collector, background eviction service

### Physical storage

**ExtentPool**:
The bounded physical collection that owns cache extents, allocation slots, payload/owner files, and
their lifecycle state.
_Avoid_: ExtentStore, engine, device

**Cache extent**:
A fixed-size append-oriented group of allocation slots with one active priority and one generation.
It contains many Stored Entries and is sealed, reclaimed, and reused as one unit.
_Avoid_: Segment, entry extent, priority partition

**Allocation slot**:
The fixed-size physical allocation quantum. One Stored Entry may occupy multiple contiguous slots
inside exactly one cache extent.
_Avoid_: Block, chunk, extent

**Entry allocation**:
The contiguous allocation-slot span occupied by one Stored Entry. It is not independently reclaimed.
_Avoid_: Extent allocation, block chain, reclaim unit

**Entry location** (`EntryLocation`):
The physical reference to one Stored Entry: first slot, encoded length, checksum, priority, and
extent generation.
_Avoid_: Extent location, blob address, index entry

**Slot owner** (`SlotOwner`):
The fixed record repeated for each occupied allocation slot so reclaim can identify candidate
entries without rebuilding variable-length metadata.
_Avoid_: Entry, index record, ownership service

**Extent generation**:
The reuse epoch of one cache extent. It fences EntryLocations left behind by an older physical
incarnation.
_Avoid_: Service generation, cache incarnation, entry version

### Policy and durability

**Cache priority**:
A caller-assigned protection class expressing an Entry's relative business importance.
_Avoid_: Temperature, hotness

**Cache temperature**:
A volatile estimate of observed Entry reuse, independent of cache priority.
_Avoid_: Priority

**Priority capacity floor**:
The minimum cache-extent capacity protected for one priority while unused capacity remains
borrowable by more active priorities.
_Avoid_: Priority partition, pinned capacity, priority limit

**Admission**:
The internal classification of how Extent handled an offered Entry. It is never returned as a
per-put result.
_Avoid_: Public put result, committed write, write receipt

**Publication**:
The internal point at which one complete Entry becomes visible to later reads. It is not a
foreground acknowledgement or durability promise.
_Avoid_: Commit, durable put, successful write

**Recovery frontier**:
The newest internally published cache state that Extent can reconstruct after process or machine
failure.
_Avoid_: Transaction commit, consistency point

**Best-effort cache operation**:
An operation that preserves Entry integrity but may shed work, expose an older complete Entry, or
have no lasting effect.
_Avoid_: Transaction, acknowledged mutation, invalidation barrier
