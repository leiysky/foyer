---
status: superseded
---

# Use a dual-generation journal for the extent cache index

This persistence baseline was superseded by ADR 0005 after the 100-million-entry replay limit was
measured. Its exactness, sequential mutation pages, and dual-generation commit protocol remain part
of the paged base-and-delta design.

The extent cache will keep an exact in-memory index and persist index mutations as checksummed,
sequential journal pages. When a generation exhausts its fixed slack, the live index is compacted
sequentially into the inactive half of the preallocated index file and committed by its
superblock. This replaces candidate-bucket capacity loss and random 4 KiB COW publication while
keeping recovery independent of data-file scanning; recovery replays only the highest valid,
capacity-bounded journal generation.

The balanced static layout reserves 25% journal slack. Existing write-batch, metadata-flush, and
checkpoint runtime settings remain the tuning boundary; no new tuning knob is added until a
sensitivity test shows a material workload-dependent benefit. The journal replaces the COW index
only if it reaches data capacity without index loss, reduces index write calls by at least an order
of magnitude, preserves priority retention, and either improves sustained end-to-end throughput by
at least 10% or removes the admission-loss failure without a material throughput or recovery
regression. Otherwise the prototype is deleted rather than retained as a second production path.

The trade-off is explicit: reopen must rebuild the exact in-memory index by sequentially replaying
at most one journal generation, so it uses memory proportional to live cache entries and gives up
the current millisecond lazy-open path. A 300 GiB acceptance run must keep this replay below one
second on the target NVMe host before the decision becomes accepted.

The first 300 GiB/240 GiB run passed its complete 1,175,843-entry content scan with zero index
replacement/rejection. It sustained 353.9 MiB/s versus 329.4 MiB/s for the earlier COW run and
reduced process-accounted writes from 359.1 GiB to about 334.5 GiB. Index publication was 1.17 GiB
in 5,448 calls. However, reopen was 1.024 seconds, peak RSS was about 1.0 GiB, and mixed-read
p50/p99 rose from 41.96/243.40 ms to 56.70/412.99 ms in the single-run comparison. The decision
therefore remained proposed at that stage: exactness and the write path passed, but recovery and
concurrent latency still needed the core-times-two benchmark. Range-native records were expected to
remove the 4.6 physical-map entries per logical entry before this ADR could become accepted.

The range-native, core-times-two acceptance rerun closed those gates. With four clients on the
two-core i8g host, the journal sustained 354.8 MiB/s, reopened the 240 GiB cache in 323.67 ms, used
509,996 KiB peak RSS, and reduced index writes to 109.8 MiB. An independent recover-only process
reopened the same cache in 312.27 ms and completed a second full content scan. Both scans found zero
incorrect hits, high and normal byte retention remained 100%, and index replacement/rejection
remained zero. Point-read p99 was 8.815 ms and mixed-group p99 was 400.11 ms, compared with 8.853
and 473.38 ms for the saved part-based journal binary. This accepts the dual-generation journal as
the exact persistent index. Checkpoint publication latency remains a separate concern recorded in
ADR 0004; it does not require returning to candidate placement or COW index pages.

A later 100-million-blob scale test found the boundary of that decision. The test generated a valid
compacted journal directly because 100 million 16 KiB payload slots would require about 1.6 TB,
which does not fit the 436 GiB development device. It therefore isolates the index work that
dominates `LogIndex::open`, rather than claiming a full data-file capacity run. On the same two-core
i8g.large host, the fully allocated dual-region index was 16,253,984,768 bytes and its active journal
was 6,501,588,992 bytes. Three page-cache-cold recoveries took 38.265, 37.039, and 36.762 seconds;
each physically read 6,501,593,088 bytes, rebuilt a map with capacity 117,440,512, used about 7.13
GiB RSS, and passed sampled key/location validation. A standalone buffered read of the active
journal took 7.33 seconds at 887 MB/s. Preloading the journal reduced recovery only to 34.54 seconds
and memory pressure caused 3.40 GB to be read again while the map grew.

The journal remains the accepted exact and crash-safe persistence baseline, but a single-threaded
full-map replay is not accepted as the final 100-million-entry recovery architecture. At that scale,
CRC/decode/hash insertion and the monolithic map dominate the sequential I/O. Further recovery work
must first align the startup SLO and key representation, then compare an independently recoverable
sharded base plus bounded delta against a persistent or lazy index. Increasing the I/O run or adding
another static tuning knob is not sufficient evidence for changing this result.
