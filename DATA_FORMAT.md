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
