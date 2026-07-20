---
status: accepted
---

# Enforce a transient disk budget for EntryIndex

ExtentStore reserves a hard index disk budget inside the configured cache capacity, and
FixedRecordLSM must reserve from it before writing WAL frames, temporary manifests, flush SSTs, or
compaction output. The static reservation represents three bounded copies of the maximum fixed
record state: steady SSTs, atomic compaction output, and the WAL/L0 write tail. Obsolete bytes are
released only after unlink and directory sync.

Checking only the final SST size was rejected because leveled compaction temporarily holds input
and output together and could exceed ScopeDB's cache limit. Reserving twice the entire cache was
also rejected because payload data does not need duplication. A streaming partial-publication
compactor could reduce headroom later, but would add recovery states and is unnecessary while the
index remains a small fraction of the cache.

This reduces usable payload capacity in exchange for a locally enforceable upper bound. Capacity
exhaustion fails the write or maintenance pipeline before crossing the limit. Because the derived
layout changed, Extent cache incarnation V5 clears V4 data instead of opening it under a different
budget interpretation.
