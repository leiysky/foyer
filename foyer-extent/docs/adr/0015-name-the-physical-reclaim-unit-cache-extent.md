# Name the physical reclaim unit cache extent

The former segment terminology hid the reason for the Extent name and created two competing engine
concepts. We name the append, seal, generation, and reclaim unit a **cache extent**, reserve
**Engine** for Foyer's disk-engine boundary, call the disk key-to-entry core **ExtentStore**, and
call its physical extent owner **ExtentPool**. A per-entry contiguous slot span is an **entry
allocation**, because it is not independently reclaimed. The complete logical key/value/priority
object is an **Entry**; “blob” names only its value, and the disk encoding is a **Stored Entry**.

The Rust module and type names follow those boundaries without legacy aliases. This is a naming and
API decision only: persisted V3 paths, magic values, record encodings, and compatibility remain
unchanged, so the format version does not advance.
