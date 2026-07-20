# ExtentStore design

## Model

`ExtentStore` stores one complete Entry per variable-length `EntryKey`. Its physical hierarchy is:

```text
Stored Entry
  -> contiguous allocation slots
  -> cache extent (allocation and reclaim unit)
  -> preallocated data/owner files
```

Range parsing and application-level assembly belong above this store. One Entry always has one
`EntryLocation` and one durable index record, regardless of how many slots hold its bytes.

## Static layout

The store directory contains:

| Path | Role |
| --- | --- |
| `data` | Preallocated payload slots |
| `owners` | Fixed owner record per physical slot |
| `state` | Two alternating checksummed allocator-state copies |
| `index-lsm/` | FixedRecordLSM WAL, manifests, and SSTs |

A slot-owner record binds a slot to the key digest, extent generation, value length, checksum, and
priority. Multi-slot Stored Entries repeat enough ownership information to reclaim them without
consulting a variable-length metadata heap. Exact key validation remains in the Stored Entry.

The capacity calculation includes all four components. One extent is excluded from usable
capacity as reclaim headroom, and fewer than five physical extents are rejected. The index
reservation is derived from the maximum usable entry count and covers three bounded regions: one
steady-state index copy, one atomic compaction output copy, and one WAL/L0 write tail. WAL append,
manifest replacement, flush output, and compaction output reserve bytes from the same hard budget.
Obsolete bytes are released only after unlink and directory sync, so runtime compaction cannot
silently exceed the cache size limit.

## Lookup

The index lookup order is active overlay, frozen checkpoint overlay, then durable LSM. A lookup that
enters the LSM records the durable-base revision, rechecks both overlays after I/O, and retries if a
frozen overlay retired meanwhile. This closes the miss race without invalidating readers for every
unrelated active mutation.

After a single-Entry index lookup, `ExtentPool` validates the extent generation, reads contiguous
slots in runs bounded by `read_run_size`, checksum-checks the Stored Entry, and rechecks the generation.
This path does not read owner metadata, so a hot index lookup does not add an owner-file I/O. Owner
records exist for reclaim and recovery accounting, not foreground lookup. Any stale, torn, or
mismatched location is a miss/error boundary, never an unverified hit. There is deliberately no
second batch-read implementation beside Foyer's point-load interface.

## I/O admission

`ExtentPool` owns a cooperative synchronous I/O scheduler for the payload data plane. One read
permit spans all physical runs of an Entry payload; acquisition is an atomic lock-free fast path and never
waits for a write. Data and owner write runs plus their publication syncs use write permits. A write
first looks for a read-quiescent point, but the read-priority interval is bounded (2 ms by default),
so continuously arriving reads cannot starve cache publication or reclaim. Existing write
concurrency remains the hard cap. A zero interval bypasses admission and accounting entirely.

The scheduler does not own buffers, spawn I/O workers, reorder durability steps, or alter the disk
format. The admitted caller executes the positional syscall directly. Allocator-state persistence
and FixedRecordLSM remain outside this policy: allocator writes are serialized recovery-critical
transitions, while an index lookup may be satisfied by its memory overlay or block cache without a
physical I/O. Extending admission into FixedRecordLSM requires evidence of metadata-I/O contention,
not a scheduler call around a high-level lookup.

`IoSchedulerStats` reports enabled policy, current readers/writers, scheduled write operations,
read-priority waits, write-limit waits, and cumulative/maximum write admission delay. These are
runtime tuning signals rather than acknowledged-write semantics.

## Insert and checkpoint

An insert validates the Entry, allocates contiguous slots, writes payload and slot owners, installs the
location in the active overlay, and synchronizes the payload publication fence. It then advances a
logical epoch. Once `checkpoint_changes` is reached, or the periodic cache worker requests one, the
coordinator captures immutable allocator and index images under the mutation lock and releases it.

Durable checkpoint order is:

```text
payload + owners sync
  -> alternating allocator-state copy
  -> synced FixedRecordLSM batch + live-entry application state
  -> frozen-overlay retirement
```

Publication returns to the Foyer flush worker after the payload fence; metadata checkpointing has
no per-insert strict mode. Mutation-count and periodic triggers feed the same coalescing checkpoint
worker, which advances the durable frontier while later mutations may continue. A checkpoint error
becomes sticky and subsequent mutations fail rather than continuing with an unknown durability
state. Graceful `sync` and close wait for the latest epoch and already-scheduled FixedRecordLSM
maintenance, so a late flush or compaction failure cannot be hidden by an earlier WAL durability
acknowledgement.

## Reclaim

Allocation is append-oriented within the current extent. Under pressure, priority capacity floors
first select a reclaimable class, then age selects a victim extent. The default protects 10% of
usable extents for high-priority data and 70% for normal-priority data; these are logical lower
bounds, rounded up to reclaim-unit granularity, rather than preallocated partitions. Empty
protection and all unreserved capacity remain borrowable. Low priority has no floor and can recycle
only low-priority extents.

Normal pressure reclaims low data first, then high occupancy above the high floor, then normal
data. High pressure reclaims low data first, then normal occupancy above the normal floor, then
high data. Thus stale historical high-priority data cannot starve normal demand, and high writes
cannot consume normal's protected working set. Floor percentages are runtime policy and may change
across reopen; extent ownership remains part of the durable allocator state.

Within the selected class, valuable entries may be promoted into the reclaim target; others are
removed from the index. Promotion is considered only for same-priority reclaim and is capped at the
hottest one eighth of the source extent, bounding promotion-only write amplification at one
seventh. Extent generation changes fence all stale locations.

The concrete `Reclaimer` owns that complete transition. `ExtentStore` invokes it only from the
ordered publication path while holding the mutation lock; it is deliberately not an independent
background service or a pluggable policy interface.

Reclaim is serialized with metadata persistence. Before a source generation can be reused, the
store performs the required inline checkpoint and publishes removals/promotions. This is the
exception to background checkpointing because generation reuse cannot race an older captured
epoch.

Priority and temperature remain distinct:

- priority is supplied by the caller (`high`, `normal`, `low`);
- temperature is the volatile TinyLFU-style reuse estimate;
- capacity floors protect minimum physical residency without persisting temperature;
- promotion requires the configured threshold for the entry's priority.

## FixedRecordLSM boundary

The durable index supports exactly 24-byte keys and 32-byte locations, atomic put/delete batches,
point lookup, WAL recovery, and an opaque `u64` application state. It has no range API, public
iterator, transaction model, column family, compression selector, or pluggable compaction policy.

Its fixed policy uses immutable SSTs, Bloom filters, a bounded block cache, partitioned leveled
compaction, and alternating manifests. Open reads fence summaries and a bounded WAL tail; detailed
metadata and data blocks are demand-loaded. One eighth of the runtime cache budget is reserved for
lazy, table-local Bloom pages, which become lock-free after their first validated read. The
remaining shared budget serves evictable data pages and any Bloom pages that cannot enter the
pinned tier. ExtentStore supplies no range or payload-layout knowledge to the LSM.

## Failure model

- A frozen V3 fixture covers the complete payload, owner, allocator, manifest, and WAL recovery
  path. The current store must read it, append a new entry, and reopen both entries. Any intentional
  format break therefore requires an explicit compatibility decision rather than an encoder and
  decoder changing unnoticed together.
- Incomplete final WAL frames are ignored; corruption inside the durable prefix is an error.
- The newest invalid allocator or manifest copy falls back to the older valid copy.
- New SSTs are synced before a manifest can reference them.
- Obsolete SST/WAL files are unlinked only after the new manifest is durable.
- A crash before index publication may leak slots; a crash after it recovers only previously
  durable payload/generation state.
- Since this is an expendable cache, an integrator may recreate an invalid top-level store; the
  store itself still reports the corruption precisely.

## Deliberate non-goals

- No selectable legacy layout or index implementation.
- No payload scan or all-key rebuild during recovery.
- No range parsing or cross-Entry assembly in the store.
- No general-purpose Rust RocksDB clone.
- No tuning switch for block format, Bloom shape, level topology, or compaction style.
