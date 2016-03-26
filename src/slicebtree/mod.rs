/// 2-headed, Copy-on-Write B+Tree map
/// Supports MVCC up to 2 revisions
/// Lives entirely within the slice that is given to it.
/// Keys and Values are byte slices.
use std::mem;
use self::node::*;
use std::sync::atomic::AtomicUsize;
use allocator::*;

pub mod node;

pub const N: usize = 2;
pub const B: usize = 100;
pub const NOT_FOUND: usize = B+1;

/// Maps arbitrary [u8] to [u8].
/// One value per key
pub struct BTree {
    page_pool: Pool,
    current_root: AtomicUsize,
    tx_id: AtomicUsize,
    // roots: Vec<EntryLocation>,
}

/// Public API
impl BTree {
    pub fn new(buf: &mut [u8]) -> BTree {
        let page_pool = Pool::new(buf);

        BTree {
            page_pool: page_pool,
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

// pub struct Context {
//     tx_id: usize,
//     pool: &Pool,
// }
//
