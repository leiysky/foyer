# ExtentStore design

## Model

`ExtentStore` stores one complete Entry per variable-length `EntryKey`. Its physical hierarchy is:

```text
Stored Entry
  -> contiguous byte allocation
  -> page-aligned publication frame
  -> cache extent (append and reclaim unit)
  -> preallocated data/directory files
```

Range parsing and application-level assembly belong above this store. One Entry always has one
`EntryLocation` and one durable index record. Multiple small Stored Entries may share one I/O frame,
but no Stored Entry crosses a cache-extent boundary.

## Static layout

The store directory contains:

| Path | Role |
| --- | --- |
| `data` | Preallocated packed Stored Entry bytes |
| `directory` | One fixed owner record per Entry allocation |
| `state` | Two alternating checksummed allocator-state copies |
| `index-lsm/` | FixedRecordLSM WAL, manifests, and SSTs |

An Entry-directory record binds one allocation's byte offset to the key digest, extent generation,
value length, checksum, sequence, and priority. Reclaim enumerates this compact sidecar instead of
reading the entire payload extent. Exact key validation remains in the Stored Entry.

The capacity calculation includes all four components. One extent is excluded from usable
capacity as reclaim headroom, and fewer than five physical extents are rejected. Allocation is
bounded by both packed payload bytes and directory entries. The 4 KiB minimum Entry charge derives
the maximum Entry count without rounding physical allocations. The index capacity target is derived
from that count and plans for three regions: one steady-state index copy, one atomic compaction
output copy, and one WAL/L0 write tail. WAL append, manifest replacement, flush output, and
compaction output remain fully accounted, but this target is soft: transient or sustained
overcommit is reported as pressure and never by itself rejects a cache write. Obsolete bytes are
released only after unlink and directory sync, preserving exact usage without turning an estimate
into a cache-health boundary.

## Lookup

The index lookup order is active overlay, frozen checkpoint overlay, then durable LSM. A memory
probe can return a known location or a definitive range miss without entering the blocking I/O
pool; an unknown result continues through the durable LSM. A lookup that enters the LSM records the
durable-base revision, rechecks both overlays after I/O, and retries if a frozen overlay retired
meanwhile. This closes the miss race without invalidating readers for every unrelated active
mutation.

After a single-Entry index lookup, `ExtentPool` validates the extent generation, reads the indexed
byte range in runs bounded by `read_run_size`, checksum-checks the Stored Entry, and rechecks the
generation. Direct I/O expands the read to the covering 4 KiB frame span; buffered I/O reads only
the logical bytes. This path does not read Entry-directory metadata, so a hot index lookup does not
add a sidecar I/O. Directory records exist for reclaim and tail recovery, not foreground lookup. Any stale, torn, or
mismatched location is a miss/error boundary, never an unverified hit. There is deliberately no
second batch-read implementation beside Foyer's point-load interface.

## I/O admission

`ExtentPool` owns a cooperative synchronous I/O scheduler for the payload data plane. One read
permit spans all physical runs of an Entry payload; acquisition is an atomic lock-free fast path and never
waits for a write. Data and Entry-directory write runs plus their publication syncs use write permits. A write
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

An insert validates the Entry and reserves an exact byte range plus one directory position. Adjacent
same-extent allocations in a store batch are packed into page-aligned write frames; only the final
frame is padded. The store writes payload and Entry-directory records, advances the extent cursor to
the frame boundary, installs locations in the active overlay, and synchronizes the payload
publication fence. It then advances a logical epoch. Once `checkpoint_bytes` is reached, or the
periodic cache worker requests one, the coordinator captures immutable allocator and index images
under the mutation lock and releases it.

Durable checkpoint order is:

```text
payload + Entry directory sync
  -> alternating allocator-state copy
  -> synced FixedRecordLSM batch + live-entry application state
  -> frozen-overlay retirement
```

Publication returns to the Foyer flush worker after the payload fence; metadata checkpointing has
no per-insert strict mode. Published-byte and periodic triggers feed the same coalescing checkpoint
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
hottest one eighth of the source extent using `max(stored length, Entry charge)`, bounding both
payload and directory pressure and keeping promotion-only write amplification near one seventh.
Extent generation changes fence all stale locations.

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

- A frozen V3 fixture covers the former payload, owner, allocator, manifest, and WAL layout. V4 must
  reject it and the explicit recreate path must remove legacy owned files before creating the new
  directory layout. Current-format tests separately cover append, reopen, active-tail recovery,
  reclaim, and process abort.
- Incomplete final WAL frames are ignored; corruption inside the durable prefix is an error.
- The newest invalid allocator or manifest copy falls back to the older valid copy.
- New SSTs are synced before a manifest can reference them.
- Obsolete SST/WAL files are unlinked only after the new manifest is durable.
- A crash before index publication may leak byte allocations or directory positions; a crash after it recovers only previously
  durable payload/generation state.
- Since this is an expendable cache, an integrator may recreate an invalid top-level store; the
  store itself still reports the corruption precisely.

## Deliberate non-goals

- No selectable legacy layout or index implementation.
- No payload scan or all-key rebuild during recovery.
- No range parsing or cross-Entry assembly in the store.
- No general-purpose Rust RocksDB clone.
- No tuning switch for block format, Bloom shape, level topology, or compaction style.
