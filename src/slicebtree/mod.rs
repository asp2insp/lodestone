/// 2-headed, Copy-on-Write B+Tree map
/// Supports MVCC up to 2 revisions
/// Lives entirely within the slice that is given to it.
/// Keys and Values are byte slices. There is no max
/// size for keys or values.

use std::mem;
use self::NodeType::*;
use allocator::*;


pub const N: usize = 2;
pub const B: usize = 100;

#[repr(u8)]
enum NodeType {
    Meta,
    Root,
    Internal,
    Leaf,
}

/// Maps arbitrary [u8] to [u8].
/// One value per key
pub struct BTree<'a> {
    num_heads: usize,
    b: usize,
    buffer: &'a [u8],
    current_root: &'a NodeHeader,
}

/// Public API
impl <'a> BTree<'a> {
    pub fn new() {

    }

    pub fn open() {

    }
}

/// Internal Functions
impl <'a> BTree<'a> {

}

/// The structure of a tree is a series of Nodes.
/// Each node is made up of at least 1 page.
/// The first page is interpreted as a NodeHeader + data
/// subsequent pages are interpreted as data based on the
/// NodeType defined by the header.
#[repr(C)]
struct NodeHeader {
    node_type: NodeType,
    tx_id: u64,
    keys: [EntryLocation; B],
    children: [EntryLocation; B],
}

struct EntryLocation {
    offset: usize,
}

// ##Node
// * NodeHeader
// * Node metadata
//
// ##NodeHeader
// * enumerated node type u8
// * transaction id usize
// * data offset start usize
// * data offset end usize
//
// ##Node Metadata (Internal or Root)
// * keys  [BSL; B]
// * children [BSL; B]
//
// ##Node Metadata (Leaf)
// * keys  [BSL; B]
// * values [BSL; B]
//
//
// ##Byte String Location
// * Arc<Page> usize
// * offset usize
//
// ##Byte String Entry Alias
// * enumerated entry type u8
// * num segments
// * segments {num segments}
//     * BSL
//
// ##Byte String Entry
// * enumerated entry type u8
// * size usize
