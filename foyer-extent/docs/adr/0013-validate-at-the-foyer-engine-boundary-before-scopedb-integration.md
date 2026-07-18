---
status: accepted
---

# Validate at the Foyer engine boundary before ScopeDB integration

ExtentEngine correctness and performance will be evaluated through Foyer's public HybridCache and
Engine interfaces before ScopeDB receives a runtime integration. ScopeDB has no dependency,
configuration, metrics, writer, or benchmark changes during this phase.

## Why

The question is whether ExtentEngine is a better disk engine than BlockEngine for the target blob
cache workload. Testing through ScopeDB adds application admission, I/O semaphores, metrics,
scheduling, key formatting, and lifecycle code as uncontrolled variables. It also creates a second
integration surface before the engine contract is stable.

The engine comparison therefore constructs the same `HybridCache<Bytes, EngineValue>` with the
same memory capacity, shard count, S3FIFO policy, write-on-insertion policy, key/value encoding,
workload, and concurrency. The selected `EngineConfig` is the principal variable. Physical I/O
options that necessarily differ are reported explicitly.

## Required evidence

- Both engines run the same replacement, recovery, exact-key, priority, and deletion contract.
- Concurrent access is at least twice the detected CPU core count.
- The report includes foreground put latency, end-to-end drain throughput, recovery time, disk-hit
  latency, hit ratio by priority, physical I/O, write amplification, and memory footprint.
- Scale workloads include wide entry and key sizes, 300 GiB-class payloads, and a separate
  100-million-entry recovery case.
- Buffered I/O and direct I/O are separate runs rather than a hidden configuration difference.

## Consequences

- ScopeDB remains buildable and reviewable with no Extent changes while the engine is under test.
- The benchmark can identify engine-level regressions without attributing application-layer noise
  to either disk layout.
- ScopeDB integration is a later, small adapter change only after the evidence justifies selecting
  ExtentEngine.
