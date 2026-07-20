# FixedRecordLSM

`foyer-fixed-lsm` is the specialized durable index for Extent's `ExtentStore`. It is deliberately
not a general-purpose key/value database. Its performance and operational reference is RocksDB under
Extent's exact 24-byte key, 32-byte location, high-churn point-lookup workload.

## Contract

- Keys are exactly 24 bytes and values are exactly 32 bytes.
- The only mutations are put and delete; a write batch becomes visible atomically.
- A write batch may atomically carry one opaque `u64` application state. ExtentStore uses it for
  the exact durable live-entry count, so recovery never scans keys to rebuild cardinality.
- Reads are exact point lookups. There is no public iterator, range API, snapshot, transaction,
  column family, merge operator, TTL, or compression policy.
- Buffered and WAL-synced writes are supported. WAL-free writes exist only for a bulk load that is
  explicitly flushed before publication.
- One process owns a database directory. Readers are concurrent; writers are serialized.

## Static format

An internal record is 64 bytes: key (24), value (32), and a sequence/tombstone word (8). SST data
blocks are 8 KiB and contain a 64-byte header plus at most 127 records. Every block has a CRC-32C.
The table appends one 24-byte last-key fence and one fixed 224-byte Bloom filter per data block.
Fences are packed 338 per checksummed 8 KiB page; only one last-key fence per fence page is loaded
during open. Thirty-six filters are packed into a checksummed 8 KiB metadata page. Detailed fence
pages are loaded and CRC-validated once into per-page `OnceLock` slots, so open does not read them
and hot fence lookup has no shared cache lock. Bloom pages and data blocks use the bounded sharded
cache, with Bloom metadata protected from one-pass data scans. A checksummed 4 KiB footer closes the
table and records its tombstone count without requiring an open-time scan.
Fourteen bits per key is the static balance: at 100M entries it costs about 52 MiB more than 10 bits
while reducing the measured multi-run false-positive path that occupied point-read p99.

The WAL is a sequence of checksummed batch frames. Recovery ignores an incomplete final frame and
replays only records newer than the manifest's flushed sequence. A checksummed manifest snapshot is
alternated between two files and names the live SST set. SST creation and compaction follow:

1. write and sync new SST files;
2. write, sync, atomically rename, and directory-sync the next manifest copy;
3. publish the immutable in-process version;
4. unlink obsolete SSTs and WAL state.

A crash can therefore leave orphan files or duplicate old WAL records, but cannot make the manifest
reference an unsynced table. Open validates referenced tables, ignores already-flushed WAL records,
and removes only unreferenced temporary/orphan files after the live version is established.

The ExtentStore adapter supplies a hard disk budget derived from its static layout. WAL frames,
temporary manifest copies, flush SSTs, and all compaction outputs reserve space before I/O.
Reservations include transient input/output overlap and are released only after obsolete files are
unlinked and the directory is synced. Capacity exhaustion fails the write or maintenance pipeline
before the configured cache budget is crossed.

## Compaction policy

The single policy is partitioned leveled compaction. L0 accepts overlapping flush outputs. Level is
Manifest state rather than an SST property, so a file with no overlap in the next level is promoted
by a metadata-only trivial move. Otherwise selected L0 files and all overlaps in the active base
level are streaming-merged. Populated levels below L0 are key-range partitioned; one file plus all
overlaps in the next level bounds each later compaction. The picker minimizes overlap bytes per
source byte. Output files become eligible to close at populated deeper-level fences after reaching
one quarter of the target table size, and are always capped at one write-buffer worth of records. This
bounds grandparent overlap without creating one tiny SST per deeper partition. Tombstones are
retained in every upper level. Compaction into the
bottom level includes every older overlapping record, so it drops tombstones there. A non-overlap
file remains a metadata-only move unless its footer reports tombstones, in which case its
bottom-level transition rewrites it once to purge them. Runtime
tuning is limited to write-buffer size and block-cache budget; the on-disk block/filter format and
level policy are static.

The active base level is derived from total SST bytes. The bottom level targets the current data set
and each preceding populated level targets one tenth of the next. The first base target may range
from one tenth of the four-write-buffer base through the full base, matching the purpose of dynamic
level sizing: L0 enters near the bottom without first merging an unnecessarily large level. Smaller
levels remain empty. This is one static dynamic-level policy, not a user-selectable compaction mode.

## ExtentStore integration

The ExtentStore adapter keeps an active mutation overlay and at most one immutable frozen overlay in
front of `FixedLsm`. Reads check active, frozen, and durable state in that order. Retiring a
persisted frozen overlay increments a base revision; a durable lookup rechecks the overlays and
revision after I/O so a concurrent checkpoint cannot expose a stale base result. Ordinary active
mutations are caught by the overlay recheck without invalidating unrelated reads.

A checkpoint rotates the active overlay while holding ExtentStore's mutation lock, then persists
payload, allocator state, and the frozen index batch in that order. The index batch uses a synced
WAL frame and carries the captured live-entry count as application state. Retiring the frozen
overlay is only an in-memory step after that frame succeeds. A crash may leak allocator space, but
cannot publish an index location whose payload and allocator generation are not already durable.

ExtentPool remains an ordinary physical payload store. It neither exposes range semantics to the index
nor derives its payload layout from an index file. FixedRecordLSM is the sole EntryIndex backend;
RocksDB is retained only as an isolated benchmark reference.

## Measured evidence

The matched 100M-entry tests used RocksDB 10.4.2 as the industrial reference, buffered I/O, a
512 MiB metadata-cache budget, and four clients on the two-core i8g.large instance-store SSD.

| Workload | FixedRecordLSM | RocksDB | FixedRecordLSM result |
| --- | ---: | ---: | ---: |
| Uniform 50/50 mixed throughput | 0.272 Mops/s | 0.228 Mops/s | +19% |
| 90%/1% hot-set mixed throughput | 0.586 Mops/s | 0.438 Mops/s | +34% |
| Uniform read p99 | 153.6 us | 119.5 us | 28.5% worse |
| Hot-set read p99 | 93.8 us | 129.2 us | 27% better |
| Replacement-churn publication | 4.824 M mutations/s | 0.941 M mutations/s | 5.1x |
| Replacement-churn total time | 11.12 s | 25.02 s | 2.25x |
| Replacement-churn bytes/mutation | 605.8 B | 496.8 B | 22% worse |
| Final index size after churn | 7.18 GB | 6.66 GB | 7.8% worse |
| Replacement-churn peak RSS | 412 MiB | 204 MiB | 2.0x |

The custom engine's dirty-tail recovery was 1.054 s after reading 269 MB. Its clean recovery after
the churn state was 21.4 ms and 1.19 MB. These numbers establish a candidate, not RocksDB-equivalent
correctness maturity. In particular, the uniform read p99, index-only write amplification, and
replacement-churn RSS are explicit tradeoffs rather than wins.

At the integrated ScopeDB layer, a 300 GiB buffered-I/O run stored 7,602,593 blobs spanning
4-128 KiB entries with four concurrent clients. Reopen after the write was 60.7 ms; after dropping
the Linux page cache it was 71.6 ms and read only 90 KiB of index metadata. The full post-reopen
content and identity scan retained all entries. Payload submission sustained 359.5 MiB/s, recovered
reads had 2.532/2.977 ms p99/p99.9, and peak process RSS was 1.19 GiB. This proves recovery is not an
O(live entries) rebuild at this scale; it does not prove long-term production durability.

## Acceptance gate

At 100M live entries and at least `core * 2` clients, the implementation is retained only if it:

- stays within 25% of RocksDB's long-run replacement-churn bytes per mutation after reaching the
  bottom level; the integrated cache must stay within 5% total device writes because payload I/O
  should dominate this index-only tradeoff;
- has no more than a 10% regression in uniform and hot-set mixed throughput;
- opens a clean index in at most 100 ms and a bounded dirty tail in at most 1.8 s on the i8g.large
  test host, without scanning live keys;
- uses at most 7.6 GB for the 100M-entry index after compaction; and
- passes the integrated 300 GiB ScopeDB-cache workload with complete content validation, competitive
  Foyer payload throughput, and bounded p99/p99.9 latency;
- enforces the cache's physical disk budget including transient compaction space, and passes
  process-crash, kill-loop, corruption, and long-running churn tests.

Failure means deleting this experiment and using RocksDB behind the same EntryIndex adapter.
Passing keeps it as a canary candidate; production replacement still requires accepting the much
larger correctness and maintenance burden of owning a storage engine.
