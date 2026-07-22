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
  -> preallocated data file
```

Range parsing and application-level assembly belong above this store. One Entry always has one
`EntryLocation` and one durable index record. Multiple small Stored Entries may share one I/O frame,
but no Stored Entry crosses a cache-extent boundary.

## Static layout

The store directory contains:

| Path | Role |
| --- | --- |
| `data` | Preallocated packed Stored Entry bytes |
| `directory` | Inert sparse placeholder retained by stable Format 1 |
| `state` | Two alternating checksummed allocator-state copies |
| `index-lsm/` | FixedRecordLSM WAL, manifests, and SSTs |

Format 1 originally reserved a 64-byte Entry-directory record shape. The stable layout and logical
file size are retained so existing Format 1 stores reopen without migration, but the runtime writes
no records and performs no directory reads or syncs. Exact key and value validation live in the
Stored Entry and `EntryLocation`; recovery intentionally discards allocations newer than the last
allocator/index checkpoint.

The hard layout calculation includes the preallocated data file, a directory budget based on the
4 KiB Entry planning charge, and both allocator-state copies. One extent is excluded from usable
payload capacity as reclaim headroom, and fewer than five physical extents are rejected. The
EntryIndex capacity target is deliberately excluded from this hard calculation: it plans one
steady-state index copy, one atomic compaction output copy, and one WAL/L0 write tail, but remains a
soft target.

Physical allocation is bounded by packed payload bytes, not by the planning charge. The directory
keeps a sparse logical address space large enough for the theoretical maximum number of valid
Stored Entries (header plus a one-byte key and one-byte value), but only its sentinel sizing block
is allocated. Small Entries may exceed planned cardinality without being rejected. EntryIndex usage
can also exceed its planning target, and that overcommit is reported as pressure rather than
converted into a cache-health boundary. The host filesystem remains the real allocation limit.
Index reservations include transient overlap and release obsolete bytes only after unlink and
directory sync, preserving exact usage accounting.

## Lookup

The index lookup order is active overlay, frozen checkpoint overlay, then durable LSM. A memory
probe can return a known location or a definitive range miss without entering the blocking I/O
pool; an unknown result continues through the durable LSM. A lookup that enters the LSM records the
durable-base revision, rechecks both overlays after I/O, and retries if a frozen overlay retired
meanwhile. This closes the miss race without invalidating readers for every unrelated active
mutation.

After a single-Entry index lookup, `ExtentPool` validates the extent generation and used range from
one lock-free atomic liveness word, reads the indexed byte range in runs bounded by `read_run_size`,
validates the Stored Entry header and seeded 88-bit
XXH3 value-content digest, compares the complete key, and rechecks the generation. The 2 MiB
default keeps values through 1 MiB, including the Stored Entry metadata and alignment fragments, in
one run. Direct I/O expands the read to the covering 4 KiB frame span; buffered I/O reads only the
logical bytes. Frame counters describe pages covered, not separate I/O calls. This path does not
read Entry-directory metadata, so a hot index lookup does not add a sidecar I/O. Any stale, torn,
or mismatched location is a miss/error boundary, never an unverified hit. There is deliberately no
second batch-read implementation beside Foyer's point-load interface.

Recovery loads one of two bounded allocator copies plus FixedRecordLSM metadata and its bounded WAL
tail. It never scans payload or directory records. Allocations and overlay mutations newer than the
last complete checkpoint are discarded as ordinary cache loss. Reclaim is a metadata-only
generation transition and contributes no directory, index, or payload reads.

Run limits are runtime syscall-batching controls, not persistent-layout boundaries. A point read
allocates only the covering range for that Entry, capped per syscall; setting a 2 MiB maximum does
not make every read 2 MiB. The write path similarly coalesces adjacent allocations from one batch,
but retains a 1 MiB default maximum so a large payload write does not monopolize the synchronous
I/O scheduler. Both limits must be positive page multiples and can change across reopen without a
format migration.

## I/O admission

`ExtentPool` owns the physical half of the cooperative synchronous I/O scheduler described in
[Foyer integration](foyer-integration.md). One read permit spans all physical runs of an Entry
payload; acquisition is an atomic lock-free fast path and never waits for a write. Data write runs
and checkpoint payload syncs use write permits. A write first looks for a read-quiescent point, but
the read-priority interval is bounded (2 ms by default), so continuously
arriving reads cannot starve cache publication or reclaim. Existing write concurrency remains the
hard cap. A zero interval bypasses admission and accounting entirely.

The scheduler does not own buffers, reorder durability steps, or alter the disk format. Default
single-concurrency writes execute inline with one reusable aligned buffer; explicitly parallel
writes use an ExtentPool-owned persistent bounded worker pool. Allocator-state persistence
and FixedRecordLSM remain outside this policy: allocator writes are serialized recovery-critical
transitions, while an index lookup may be satisfied by its memory overlay or block cache without a
physical I/O. Extending admission into FixedRecordLSM requires evidence of metadata-I/O contention,
not a scheduler call around a high-level lookup.

`IoSchedulerStats` reports enabled policy, current readers/writers, scheduled write operations,
read-priority waits, write-limit waits, and cumulative/maximum write admission delay. These are
runtime tuning signals rather than acknowledged-write semantics.

## Insert and checkpoint

An insert validates the Entry and reserves an exact byte range plus one Format 1 entry ordinal.
Adjacent same-extent allocations in a store batch are packed into page-aligned write frames; only
the final frame is padded. The store writes payload, advances the extent cursor to the frame
boundary, installs locations in the active overlay, and advances a logical published epoch without
issuing `fdatasync`. Once `checkpoint_bytes` is reached, or the periodic cache worker requests one,
the coordinator holds the mutation lock while synchronizing all dirty payload once and capturing
immutable allocator and index images, then releases the lock before metadata persistence.

The value-content digest is computed once on submission and stored in the Stored Entry and index
location. A repeated key, encoded length, digest, and priority is idempotent without reading
the old payload only when the index location is memory-resident and its allocator generation and
range are still live. An SST-only or stale location is conservatively rewritten. The development-V4
CRC32 shortcut could suppress an update for an easily
constructed collision; development V5 introduced an 88-bit seeded XXH3 identity, retained by
stable format 1. The complete key is still compared on every returned hit.

Durable checkpoint order is:

```text
data-file sync
  -> alternating allocator-state copy
  -> synced FixedRecordLSM batch + indexed-cardinality upper bound
  -> frozen-overlay retirement
```

Volatile publication returns to the Foyer flush worker after data writes; metadata checkpointing
has no per-insert strict mode. Published-byte and periodic triggers feed the same coalescing
checkpoint worker. Data sync and image capture serialize briefly with mutations, while allocator
and index persistence proceed after the lock is released and later mutations may continue. A
checkpoint error becomes sticky and subsequent mutations fail rather than continuing with an
unknown durability state. Graceful `sync` and close wait for the latest epoch and already-scheduled FixedRecordLSM
maintenance, so a late flush or compaction failure cannot be hidden by an earlier WAL durability
acknowledgement.

The checkpoint protocol maintains these invariants:

- a durable index location references only payload and allocator state from the same or an earlier
  publication frontier;
- a captured allocator/index image is immutable while it is persisted;
- a newer requested epoch remains pending when an older checkpoint completes;
- abort merge preserves the newest per-key overlay mutation;
- a captured location that is already generation-stale becomes a tombstone, not durable stale
  metadata;
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

Normal pressure reclaims low data first. While normal occupancy is below its protected floor, it
then repays high occupancy borrowed beyond the high floor; once normal's floor is satisfied, it
reclaims normal's own oldest extent instead of evicting explicitly hotter data to grow into shared
capacity. High pressure reclaims low data first, then normal occupancy above the normal floor, then
high data. Thus stale historical high-priority data cannot starve normal's protected demand, while
normal churn cannot erase a useful high-priority working set merely because high has grown beyond
its minimum. Floor percentages are runtime policy and may change across reopen; extent ownership
remains part of the durable allocator state.

Once a victim is selected, reclaim waits only for an already captured metadata checkpoint to
finish, increments the victim generation, marks the extent free, and persists the alternating
allocator-state copy before the bytes can be reused. It does not synchronize unrelated dirty
payload. This is the complete normal reclaim transaction. It performs no directory scan, per-key
index lookup, tombstone batch, payload
read, payload copy, or index checkpoint. The allocator write and sync are required before the same
physical bytes can be overwritten; the pre-I/O generation check makes every old location a miss.

The concrete `Reclaimer` owns that complete transition. `ExtentStore` invokes it only from the
ordered publication path while holding the mutation lock; it is deliberately not an independent
background service or a pluggable policy interface.

Reclaim is serialized with metadata persistence without forcing new metadata work. The store holds
the mutation order while it waits for an older captured epoch, so the background coordinator cannot
capture another allocator image before generation invalidation is durable. The wait has a separate
latency metric and normally resolves immediately.

Priority is the explicit temperature and retention class supplied by the caller (`high`, `normal`,
or `low`). Entries of one class are appended to class-owned extents, and victim selection works at
that same physical granularity. ExtentStore does not infer hotness from foreground reads and does
not turn hits into reclaim writes.

Old index locations are intentionally left in place on the reclaim path. Their generation is
checked at two later boundaries. Checkpoint persistence converts a stale captured overlay location
into a tombstone immediately. Older durable SST locations are checked only while an existing
non-trivial compaction is already rewriting those records; an invalid newest location becomes a
tombstone, and a bottom-level compaction can drop it. Trivial moves remain zero-copy and do not
rewrite a file merely for garbage collection. The persisted index cardinality is consequently an
upper bound, never an admission limit or a correctness input.

### Format 1 reclaim scaling boundary

Generation invalidation is O(1) logical work and O(1) sync operations, but Format 1 encodes the
allocator as two full copies. Each reclaimed extent therefore rewrites one `state_copy_size` image,
whose bytes grow linearly with configured extent count. This is small at the current target
capacities but becomes meaningful at multi-terabyte scale. A constant-byte generation journal or
batched generation transaction changes recovery metadata and belongs to Format 2; it is not hidden
behind a runtime switch in Format 1.

## EntryIndex boundary

The durable index supports fixed 24-byte digests and 32-byte locations, atomic put/delete batches,
point lookup, WAL recovery, and one opaque `u64` application state. ExtentStore supplies no range or
payload-layout knowledge to it. Overlay concurrency, FixedRecordLSM policy, recovery, cache
accounting, and rejected index implementations are specified in
[EntryIndex design](entry-index.md).

## Failure model

- A frozen development-V3 fixture covers the former payload, owner, allocator, manifest, and WAL
  layout. Stable format 1 must reject it and the explicit recreate path must remove legacy owned
  files before creating the new directory layout. The stable family magic also rejects development
  formats that used the same numeric version. Current-format tests separately cover append, reopen,
  checkpoint-tail discard, reclaim, and process abort.
- Incomplete final WAL frames are ignored; corruption inside the durable prefix is an error.
- The newest invalid allocator or manifest copy falls back to the older valid copy.
- New SSTs are synced before a manifest can reference them.
- Obsolete SST/WAL files are unlinked only after the new manifest is durable.
- A crash before index publication discards the uncheckpointed byte/allocation tail; a crash after
  it recovers only payload and allocator state fenced by that checkpoint.
- Since this is an expendable cache, an integrator may recreate an invalid top-level store; the
  store itself still reports the corruption precisely.

## Design rationale and rejected alternatives

- **Fixed allocation slots** simplified alignment but imposed severe tail padding on small Entries.
  The packed layout prototyped in development V5 and retained by stable format 1 uses page alignment
  only for physical I/O frames.
- **Cross-extent Entry descriptors** would reduce boundary waste but make reads, reclaim, and crash
  recovery span multiple generations. Extent seals the current cache extent instead.
- **Payload or directory scanning during reclaim** would make eviction cost proportional to victim
  cardinality and pollute the read path. Generation invalidation keeps reclaim independent of
  victim cardinality.
- **Operational Entry-directory records** duplicate Entry identity and add a write, sync, and
  recovery-read stream solely to rescue an expendable uncheckpointed tail. Format 1 retains the
  sparse file shape for compatibility but performs no runtime directory I/O.
- **Fixed priority partitions** strand capacity when one class is idle. Borrowable floors preserve
  minimum residency while allowing repayment under later demand.
- **An independent background reclaimer** would race the total mutation order and generation
  checkpointing. Reclaim remains an ordered ExtentStore transition.
- **Access-frequency promotion during reclaim** converts cache hits into tracking overhead and
  eviction into payload rewrite amplification. Callers classify hot and cold data explicitly;
  Extent preserves that class in physical placement and evicts whole extents.
- **A hard EntryIndex capacity limit** turns transient LSM amplification into a cache-health
  failure. The layout target is soft while usage remains exactly accounted.
- **The former 64 KiB read-run maximum** came from the fixed-slot layout and split a common 64 KiB
  value once its header, key, and direct-I/O alignment were included. A 2 MiB maximum makes the
  bounded 1 MiB value workload one payload call per hit while preserving an explicit upper bound.
- **Larger 4 MiB and 8 MiB write runs** reduce syscall count but did not materially improve
  throughput in the bounded large-value validation and raised peak RSS. Writes remain capped at
  1 MiB unless a production device demonstrates a different throughput/latency tradeoff.
- **Synchronizing every physical batch** turned sparse puts into nearly one `fdatasync` each and
  doubled the fence with the directory file. Checkpoints now hold mutation order across one grouped
  data sync and immutable capture; the tradeoff is a periodic write-latency pause instead of
  per-batch durability I/O.

## Deliberate non-goals

- No selectable legacy layout or index implementation.
- No payload scan or all-key rebuild during recovery.
- No range parsing or cross-Entry assembly in the store.
- No general-purpose Rust RocksDB clone.
- No tuning switch for block format, Bloom shape, level topology, or compaction style.
