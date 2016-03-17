## Branch byte_pool
 * Rewrite pool to be a traditional malloc/free impl over a non-segmented set of bytes. Smallest allowed chunk is a PAGE_SIZE.
 * Use Arc semantics to alloc/free memory. Arcs are backed by an entry in the refs_page, so that they're persistent.
  * Follow std Arc pattern which stores the counters next to the data as ArcInner. Use relative pointers for the outer.
 * This lets us switch to a slice-based or pointer-based API.


## Limitations
 * Max number of concurrent transactions: `usize::max_value()`
 * Only 1 write transaction at a time
 * Max Key/Value size: `1,040,400` bytes
   * sizeof(EntryLocation) is 16 bytes
   * sizeof(EntryHeader) is 16 bytes
   * Max number of subkeys in an alias is `(4096-16)/sizeof(EntryLocation) = 255`
   * Each alias points to a page. The content of a page is `4096-sizeof(EntryHeader)` bytes long
   * TODO: If this becomes an issue, I can add recursive aliasing
 * Current strategy is to never remove nodes. or join under-full nodes. This may need to be revisited since it will cause fragmentation under workloads such as sorted insertion.

## Clean Up
 * Move all node_functions that currently take an entry_loc to be object-oriented instead
 * Replace unsafe ptr copy with slice_clone_from_slice()
 * Add ARC semantics to EntryLocation
 * Clean up special casing for get_iter
 * Change constructor for Pool to consume a vec
 * FIX PAGE DEFINITIONS FOR 32 BIT SYSTEMS
