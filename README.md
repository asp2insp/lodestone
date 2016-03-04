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
