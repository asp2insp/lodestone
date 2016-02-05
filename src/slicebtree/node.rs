use std::mem;
use allocator::*;

use super::*;
use super::entry_location::*;
use super::byte_string::*;


/// The Node exposes a mutable API. Immutability/Append only
/// is left to the wrapping tree implementation

#[derive(Clone, PartialEq, Debug)]
#[repr(u8)]
pub enum NodeType {
    Meta = 0x5,
    Root,
    Internal,
    Leaf,
}

/// The structure of a tree is a series of Nodes.
/// Each node is made up of at least 1 page.
/// The first page is interpreted as a NodeHeader
/// Pages pointed to by the header as data based on the
/// NodeType defined by the header.
/// If the NodeType is Root or Internal, the children
/// are interpreted as Nodes. If the NodeType is Leaf,
/// the children are interpreted as the values of the mapping.
#[repr(C)]
pub struct NodeHeader {
    node_type: NodeType,
    tx_id: usize,
    num_keys: usize,
    keys: [EntryLocation; B],
    num_children: usize,
    children: [EntryLocation; B],
}

impl NodeHeader {
    pub fn from_entry<'a>(e: &EntryLocation, pool: &'a Pool) -> &'a mut NodeHeader {
        pool[e.page_index].borrow_mut().transmute_page_mut::<NodeHeader>()
    }

    /// Perform initial setup, such as fixing the keys/children arrays,
    /// setting the tx_id
    pub fn init(&mut self, tx: usize, node_type: NodeType) {
        self.num_keys = 0;
        self.num_children = 0;
        self.node_type = node_type;
        self.tx_id = tx;
    }
}

/// Returns true if this node is now completely removed
pub fn release_node_contents(entry: &EntryLocation, pool: &Pool) {
    let node = NodeHeader::from_entry(entry, pool);
    match node.node_type {
        NodeType::Root | NodeType::Internal => {
            for e in node.children.iter().take(node.num_children) {
                let should_recurse = pool.release(e.page_index);
                // If this node is now dead, we can recursively
                // remove its contents
                if should_recurse {
                    release_node_contents(e, pool);
                }
            }
        },
        NodeType::Leaf => {
            for e in node.children.iter().take(node.num_children) {
                release_byte_string(e, pool);
            }
        },
        _ => {},
    }
    for e in node.keys.iter().take(node.num_keys) {
        release_byte_string(e, pool);
    }
}


#[test]
fn test_invariants() {
    println!("CHECK {:?} < {:?}?", mem::size_of::<NodeHeader>(), mem::size_of::<Page>());
    assert!(mem::size_of::<NodeHeader>() < mem::size_of::<Page>());
}
