/// 2-headed, Copy-on-Write B+Tree map
/// Supports MVCC up to 2 revisions
/// Lives entirely within the slice that is given to it.
/// Keys and Values are byte slices.

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
    page_pool: Pool,
    current_root: &'a NodeHeader,
    roots: Vec<&'a NodeHeader>,
}

/// Public API
impl <'a> BTree<'a> {
    pub fn new(buf: &'a mut [u8]) -> Result<BTree, &'static str> {
        let mut page_pool = Pool::new(buf);
        let mut roots = vec![];
        for _ in 0..N {
            let p = try!(page_pool.alloc());
            roots.push(NodeHeader::new_in_page(p));
        }

        Err("Not implemented yet")
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
    tx_id: usize,
    keys: [EntryLocation; B],
    children: [EntryLocation; B],
}

impl NodeHeader {
    fn new_in_page(p: &mut Page) -> &NodeHeader {
        p.transmute_page::<NodeHeader>()
    }
}

#[repr(C)]
struct EntryLocation {
    page_index: usize,
    offset: usize,
}

#[repr(u8)]
enum EntryType {
    Alias,
    Entry,
    Deleted,
}

#[repr(C)]
struct ByteStringEntryAlias {
    entry_type: EntryType,
    num_segments: usize,
    // sizeof(EntryLocation) * num_segments
}

#[repr(C)]
struct ByteStringEntry {
    entry_type: EntryType,
    data_size: usize,
    // data_size bytes of data
}
