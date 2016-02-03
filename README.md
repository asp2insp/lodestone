## Limitations
 * Max number of concurrent transactions: `usize::max_value()`
 * Only 1 write transaction at a time
 * Max Key/Value size: `1,040,400` bytes
   * sizeof(EntryLocation) is 16 bytes
   * sizeof(EntryHeader) is 16 bytes
   * Max number of subkeys in an alias is `(4096-16)/sizeof(EntryLocation) = 255`
   * Each alias points to a page. The content of a page is `4096-sizeof(EntryHeader)` bytes long
   * TODO: If this becomes an issue, I can add recursive aliasing
