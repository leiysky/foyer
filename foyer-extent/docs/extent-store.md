# ExtentStore design

## Scope

`ExtentStore` is the ordered disk-side core beneath ExtentEngine. It owns Entry publication,
checkpoint frontiers, EntryIndex coordination, and reclaim orchestration over one ExtentPool. It
does not define the public best-effort API, own Foyer's memory tier, or interpret application keys.

The logical integrity contract is defined in [Cache contract](cache-contract.md), the engine and
queue boundary in [Foyer integration](foyer-integration.md), and the durable index in
[EntryIndex design](entry-index.md).

The store is designed around five constraints:

- one complete Entry has one location and never crosses a cache-extent boundary;
- allocation and publication are append-oriented, while reclaim operates on whole cache extents;
- recovery reads bounded metadata rather than scanning payload or rebuilding every live key;
- priority protection is borrowable and reclaim-unit aware rather than a fixed partition; and
- a crash may lose a recent tail but cannot produce a wrong or torn hit.

## Model

`ExtentStore` stores one complete Entry per variable-length `EntryKey`. Its physical hierarchy is:

```text
Stored Entry
  -> contiguous byte allocation
  -> page-aligned publication frame
  -> cache extent (append and reclaim unit)
  -> preallocated data file plus sparse Entry directory
```

Range parsing and application-level assembly belong above this store. One Entry always has one
`EntryLocation` and one durable index record. Multiple small Stored Entries may share one I/O frame,
but no Stored Entry crosses a cache-extent boundary.

## Static layout

The store directory contains:

| Path | Role |
| --- | --- |
| `data` | Preallocated packed Stored Entry bytes |
| `directory` | Sparse address space with one fixed record per Entry allocation |
| `state` | Two alternating checksummed allocator-state copies |
| `index-lsm/` | FixedRecordLSM WAL, manifests, and SSTs |

An Entry-directory record binds one allocation's byte offset to the key digest, extent generation,
value length, 88-bit value-content digest, sequence, and priority. The record itself has a CRC.
Reclaim enumerates this compact sidecar instead of reading the entire payload extent. Exact key
validation remains in the Stored Entry.

The hard layout calculation includes the preallocated data file, a directory budget based on the
4 KiB Entry planning charge, and both allocator-state copies. One extent is excluded from usable
payload capacity as reclaim headroom, and fewer than five physical extents are rejected. The
EntryIndex capacity target is deliberately excluded from this hard calculation: it plans one
steady-state index copy, one atomic compaction output copy, and one WAL/L0 write tail, but remains a
soft target.

Physical allocation is bounded by packed payload bytes, not by the planning charge. The directory
therefore uses a sparse logical address space large enough for the theoretical maximum number of
valid Stored Entries (header plus a one-byte key and one-byte value). Small Entries may exceed the
planned cardinality without being rejected; only the directory blocks actually written consume
space. Creation establishes the complete logical address space with one sentinel positional write
at its final byte rather than `ftruncate`; this preserves holes on filesystems that otherwise
allocate an extended range eagerly. Directory and EntryIndex usage can consequently exceed their
planning targets, and that overcommit is reported as pressure rather than converted into a
cache-health boundary. The host
filesystem remains the real allocation limit. Index reservations include transient overlap and
release obsolete bytes only after unlink and directory sync, preserving exact usage accounting.

## Lookup

The index lookup order is active overlay, frozen checkpoint overlay, then durable LSM. A memory
probe can return a known location or a definitive range miss without entering the blocking I/O
pool; an unknown result continues through the durable LSM. A lookup that enters the LSM records the
durable-base revision, rechecks both overlays after I/O, and retries if a frozen overlay retired
meanwhile. This closes the miss race without invalidating readers for every unrelated active
mutation.

After a single-Entry index lookup, `ExtentPool` validates the extent generation, reads the indexed
byte range in runs bounded by `read_run_size`, validates the Stored Entry header and seeded 88-bit
XXH3 value-content digest, compares the complete key, and rechecks the generation. The 2 MiB
default keeps values through 1 MiB, including the Stored Entry metadata and alignment fragments, in
one run. Direct I/O expands the read to the covering 4 KiB frame span; buffered I/O reads only the
logical bytes. Frame counters describe pages covered, not separate I/O calls. This path does not
read Entry-directory metadata, so a hot index lookup does not add a sidecar I/O.
Directory records exist for reclaim and tail recovery, not foreground lookup. Any stale, torn, or
mismatched location is a miss/error boundary, never an unverified hit. There is deliberately no
second batch-read implementation beside Foyer's point-load interface.

Reclaim and tail recovery read directory records in bounded 64 KiB runs and decode them in memory.
The run bound prevents a high-cardinality extent from allocating an unbounded buffer while avoiding
the former one-`pread`-per-Entry syscall pattern.

Run limits are runtime syscall-batching controls, not persistent-layout boundaries. A point read
allocates only the covering range for that Entry, capped per syscall; setting a 2 MiB maximum does
not make every read 2 MiB. The write path similarly coalesces adjacent allocations from one batch,
but retains a 1 MiB default maximum so a large payload write does not monopolize the synchronous
I/O scheduler. Both limits must be positive page multiples and can change across reopen without a
format migration.

## I/O admission

`ExtentPool` owns the physical half of the cooperative synchronous I/O scheduler described in
[Foyer integration](foyer-integration.md). One read permit spans all physical runs of an Entry
payload; acquisition is an atomic lock-free fast path and never waits for a write. Data and
Entry-directory write runs plus their publication syncs use write permits. A write first looks for
a read-quiescent point, but the read-priority interval is bounded (2 ms by default), so continuously
arriving reads cannot starve cache publication or reclaim. Existing write concurrency remains the
hard cap. A zero interval bypasses admission and accounting entirely.

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

The value-content digest is computed once on submission and stored in both the directory record and
index location. A repeated key, encoded length, digest, and priority is idempotent without reading
the old payload. The previous V4 CRC32 shortcut could suppress an update for an easily constructed
collision; V5 introduced an 88-bit seeded XXH3 identity, retained by V6, while keeping the same
32-byte location and 64-byte directory record sizes. The complete key is still compared on every
returned hit.

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

The checkpoint protocol maintains these invariants:

- a durable index location references only payload and allocator state from the same or an earlier
  publication frontier;
- a captured allocator/index image is immutable while it is persisted;
- a newer requested epoch remains pending when an older checkpoint completes;
- abort merge preserves the newest per-key overlay mutation;
- reclaim cannot reuse a cache-extent generation while an older captured epoch may reference it;
  and
- close succeeds only after its target epoch and scheduled index maintenance are durable.

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

## EntryIndex boundary

The durable index supports fixed 24-byte digests and 32-byte locations, atomic put/delete batches,
point lookup, WAL recovery, and one opaque `u64` application state. ExtentStore supplies no range or
payload-layout knowledge to it. Overlay concurrency, FixedRecordLSM policy, recovery, cache
accounting, and rejected index implementations are specified in
[EntryIndex design](entry-index.md).

## Failure model

- A frozen V3 fixture covers the former payload, owner, allocator, manifest, and WAL layout. V6 must
  reject it and the explicit recreate path must remove legacy owned files before creating the new
  directory layout. Current-format tests separately cover append, reopen, active-tail recovery,
  reclaim, and process abort.
- Incomplete final WAL frames are ignored; corruption inside the durable prefix is an error.
- The newest invalid allocator or manifest copy falls back to the older valid copy.
- New SSTs are synced before a manifest can reference them.
- Obsolete SST/WAL files are unlinked only after the new manifest is durable.
- A crash before index publication may leak byte allocations or directory positions; a crash after
  it recovers only previously durable payload/generation state.
- Since this is an expendable cache, an integrator may recreate an invalid top-level store; the
  store itself still reports the corruption precisely.

## Design rationale and rejected alternatives

- **Fixed allocation slots** simplified alignment but imposed severe tail padding on small Entries.
  The packed layout introduced in V5 and retained by V6 uses page alignment only for physical I/O
  frames.
- **Cross-extent Entry descriptors** would reduce boundary waste but make reads, reclaim, and crash
  recovery span multiple generations. Extent seals the current cache extent instead.
- **Payload scanning during reclaim or recovery** would remove the Entry directory at the cost of a
  full payload read. The compact sidecar keeps these paths bounded without entering foreground
  lookup.
- **One directory record per page or slot** duplicates Entry identity and inflates index cardinality.
  The packed layout stores one directory record and one EntryIndex location per complete Entry.
- **Fixed priority partitions** strand capacity when one class is idle. Borrowable floors preserve
  minimum residency while allowing repayment under later demand.
- **An independent background reclaimer** would race the total mutation order and generation
  checkpointing. Reclaim remains an ordered ExtentStore transition.
- **A hard EntryIndex capacity limit** turns transient LSM amplification into a cache-health
  failure. The layout target is soft while usage remains exactly accounted.
- **The former 64 KiB read-run maximum** came from the fixed-slot layout and split a common 64 KiB
  value once its header, key, and direct-I/O alignment were included. A 2 MiB maximum makes the
  bounded 1 MiB value workload one payload call per hit while preserving an explicit upper bound.
- **Larger 4 MiB and 8 MiB write runs** reduce syscall count but did not materially improve
  throughput in the bounded large-value validation and raised peak RSS. Writes remain capped at
  1 MiB unless a production device demonstrates a different throughput/latency tradeoff.
- **Synchronizing mutable payload files in the background checkpoint** lets the sync chase later
  writes. Each physical batch pays its bounded payload fence before immutable metadata capture.

## Deliberate non-goals

- No selectable legacy layout or index implementation.
- No payload scan or all-key rebuild during recovery.
- No range parsing or cross-Entry assembly in the store.
- No general-purpose Rust RocksDB clone.
- No tuning switch for block format, Bloom shape, level topology, or compaction style.
