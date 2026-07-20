---
status: accepted
---

# Admit payload I/O cooperatively

Extent schedules synchronous payload I/O with caller-executed admission rather than an internal
executor. A foreground Entry-payload read acquires a lock-free permit across all of its positional reads and
never waits behind writes. Data/slot-owner write runs and payload syncs wait for a read-quiescent point
for at most 2 ms by default, then proceed even while reads remain active. Existing write concurrency
is still the hard write cap. A zero duration bypasses the scheduler and its accounting.

## Why

Foyer puts are fire-and-forget and may be shed, while a cache miss must return quickly enough for
the caller to fall back to source storage. Queue admission and smaller read-busy batches limit work
above ExtentPool, but cannot control when already admitted physical writes compete with a payload
read. ExtentPool is the lowest point that still knows whether an operation is a foreground Entry
read or cache-publication write.

The scheduler owns only admission. The calling thread retains its buffer and executes the syscall,
so positional I/O remains compatible with buffered I/O and Linux direct I/O. There is no additional
buffer copy, worker hop, async runtime dependency, on-disk change, or io_uring-shaped public API.
This leaves a future native async backend possible without treating synchronous calls as fake SQEs.

One permit covers a logical Entry read rather than one `pread`. Large entries span many bounded
physical runs; syscall-level permits would create avoidable atomic traffic and let writes enter
between adjacent runs of the same foreground request.

Allocator-state and FixedRecordLSM operations are not admitted by this policy. Allocator updates are
serialized durability transitions. Index reads are frequently served by overlays or cached pages,
so wrapping a high-level lookup would delay writes even when no metadata I/O occurred. FixedRecordLSM
needs a physical-operation hook before metadata scheduling would be honest.

## Evidence and limits

An executor prototype with owned requests, channels, and dedicated workers was rejected. On the
i8g.large NVMe development host it added a second scheduling hop and raised warm buffered hit p99
from roughly 0.35 ms to 0.85 ms.

The cooperative design was tested through the same Foyer engine benchmark at `2 * cores`. In a
capacity-contained 96 MiB read set plus 32/64 MiB write wave, both direct and buffered runs retained
100% of the original keys:

- direct I/O: bypass and 2 ms policy both produced about 8.52 ms mixed hit p99; 73 writes reached
  the priority bound and accumulated 149.9 ms of admission delay;
- buffered I/O: mixed hit p99 was 837 us with bypass and 849 us with the policy; 44 writes waited,
  accumulating 11.3 ms, because most found natural read-quiescent gaps early.

These results validate bounded behavior and show no material read regression, but do not prove a
throughput or latency win on this virtualized SSD. Eviction-pressure runs sometimes reduced mixed
hit p99, but changed retention and accepted-write counts and are not causal evidence. The 2 ms
default is therefore a conservative read-start QoS budget, not a benchmark claim. Runtime tuning,
including full bypass, remains explicit and scheduler statistics expose whether a production
workload actually exercises the policy.

## Consequences

- Reads remain non-queueing at both Foyer admission and physical payload admission.
- Write completion may be delayed by at most one read-priority interval per admitted write run,
  plus time waiting for the configured write-concurrency limit.
- Sustained reads cannot starve publication, reclaim, sync, or graceful shutdown.
- io_uring support, if added later, must preserve these operation classes and durability ordering;
  it does not require changing the current public engine contract.
