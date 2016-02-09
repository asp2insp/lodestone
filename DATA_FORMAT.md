# WAY OUT OF DATE

Currently, we have nodes, we have entries, and aliased entries.
Every chunk of memory contains a flag MemType as the first byte.
This byte is what's used to tag the memory. Memory is stored in pages.
The page size matches the memory cache's page size (Currently 4K).
Each page either contains data (in the form of ByteStringEntries) or
tree structure (in the form of NodeHeaders).

##Node
* NodeHeader
* Node metadata

##NodeHeader
* enumerated node type u8
* transaction id usize
* data offset start usize
* data offset end usize

##Node Metadata (Internal or Root)
* keys  [BSL; B]
* children [BSL; B]

##Node Metadata (Leaf)
* keys  [BSL; B]
* values [BSL; B]


##Byte String Location
* Arc<Page> usize
* offset usize

##Byte String Entry Alias
* enumerated entry type u8
* num segments
* segments {num segments}
    * BSL

##Byte String Entry
* enumerated entry type u8
* size usize
