/// 2-headed, Copy-on-Write B+Tree map
/// Supports MVCC up to 2 revisions
/// Lives entirely within the slice that is given to it.
/// Keys and Values are byte slices.

use std::mem;
use self::node::*;
use self::entry_location::*;
use self::node::NodeType::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use allocator::*;

pub mod node;
pub mod entry_location;

pub const N: usize = 2;
pub const B: usize = 100;

/// Maps arbitrary [u8] to [u8].
/// One value per key
pub struct BTree {
    page_pool: Pool,
    current_root: AtomicUsize,
    tx_id: AtomicUsize,
    roots: Vec<EntryLocation>,
}

/// Public API
impl BTree {
    pub fn new(buf: &mut [u8]) -> BTree {
        let mut page_pool = Pool::new(buf);
        let mut roots:Vec<EntryLocation> = vec![];

        for _ in 0..N {
            roots.push(EntryLocation {
                page_index: page_pool.alloc().unwrap(),
                offset: 0,
            });
        }

        for root in &roots {
            NodeHeader::from_entry(root, &mut page_pool).init(0, Root);
        }

        BTree {
            page_pool: page_pool,
            roots: roots,
            tx_id: AtomicUsize::new(0),
            current_root: AtomicUsize::new(0),
        }
    }

    pub fn open() {

    }
}

/// Internal Functions
impl BTree {

}
