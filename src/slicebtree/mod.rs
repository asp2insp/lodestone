/// 2-headed, Copy-on-Write B+Tree map
/// Supports MVCC up to 2 revisions
/// Lives entirely within the slice that is given to it.
/// Keys and Values are byte slices.
use std::mem;
use self::node::*;
use self::entry_location::*;
use self::byte_string::*;
use std::sync::atomic::{AtomicUsize};
use allocator::*;

pub mod node;
pub mod byte_string;
pub mod entry_location;

pub const N: usize = 2;
pub const B: usize = 100;
pub const NOT_FOUND: usize = B+1;
lazy_static! {
    pub static ref BSE_HEADER_SIZE: usize = mem::size_of::<ByteStringEntry>();
    pub static ref BSE_CHUNK_SIZE: usize = PAGE_SIZE - *BSE_HEADER_SIZE;
    pub static ref EL_PTR_SIZE: usize = mem::size_of::<EntryLocation>();
    pub static ref MAX_ALIASES_PER_CHUNK: usize = *BSE_CHUNK_SIZE / *EL_PTR_SIZE;
    pub static ref ALIASED_BSE_MAX_SIZE: usize = *MAX_ALIASES_PER_CHUNK * *BSE_CHUNK_SIZE;
}

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
        let page_pool = Pool::new(buf);
        let mut roots:Vec<EntryLocation> = vec![];

        for _ in 0..N {
            roots.push(EntryLocation {
                page_index: page_pool.alloc().unwrap(),
                offset: 0,
            });
        }

        // for root in &roots {
        //     NodeHeader::from_entry(root, &mut page_pool)
        //         .init(root.page_index, 0, MemType::Root);
        // }

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

// pub struct Context {
//     tx_id: usize,
//     pool: &Pool,
// }
//
// pub trait Node {
//     fn insert(key: &[u8], value: &[u8]) {
//
//     }
// }


/// Return the amount of free space left in a given page
fn free_space(page: &Page) -> usize {
    match page.transmute_segment::<MemType>(0) {
        &MemType::Entry | &MemType::Alias | &MemType::Deleted => {
            free_space_entry_page(page)
        },
        &MemType::Meta | &MemType::Root | &MemType::Internal | &MemType::Leaf => {
            free_space_node_page(page)
        }
    }
}
