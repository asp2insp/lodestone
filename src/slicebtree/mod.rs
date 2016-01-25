/// N-headed, Copy-on-Write B+Tree map
/// Supports MVCC up to N revisions
/// Lives entirely within the slice that is given to it.
/// Keys and Values are byte slices. There is no max
/// size for keys or values.

use std::mem;
use self::NodeType::*;
use allocator::*;

pub struct Options {
    num_heads: usize,
    b: usize,
}

pub fn defaults() -> Options {
    Options {
        num_heads: 2,
        b: 100,
    }
}

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
#[repr(C, packed)]
struct NodeHeader {
    node_type: NodeType,
    tx_id: u64,
    data_offset_start: u64,
    data_offset_end: u64,
}


/// Each page is 64Kb
type Page = [u8; 0x10000];
