## Limitations
 * Max number of concurrent transactions: `usize::max_value()`
 * Only 1 write transaction at a time
 * Max Key/Value size: `2,791,426` bytes
   * sizeof(EntryLocation) is 6 bytes
   * sizeof(EntryHeader) is 3 bytes
   * Max number of subkeys in an alias is `(4096-3)/sizeof(EntryLocation) = 682`
   * Each alias points to a page. The content of a page is `4096-sizeof(EntryHeader)` bytes long
