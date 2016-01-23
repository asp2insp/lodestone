/// N-headed, Copy-on-Write B+Tree map
/// Supports MVCC up to N revisions
/// Lives entirely within the slice that is given to it

use std::mem;
use self::NodeType::*;
use zero::read;

pub const MAGIC_ID: u32 = 0x1D_2D_3D_4D;

pub struct Options {
    num_heads: Option<usize>,
    b: Option<usize>,
}

pub fn Defaults() -> Options {
    Options {
        num_heads: Some(2),
        b: Some(100),
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

#[repr(C)]
struct NodeHeader {
    node_type: NodeType,
    tx_id: u64,
    data_offset_start: u64,
    data_offset_end: u64,
}

#[repr(C)]
struct Metadata {
    magic: u32,
    num_heads: usize,
    b: usize,
}

unsafe impl Pod for Metadata;

/// Public API
impl <'a> BTree<'a> {
    pub fn open() {

    }
}

/// Internal Functions
impl <'a> BTree<'a> {

}

/// Get stats from the metadata page
fn get_metadata<'a>(buffer: &'a [u8]) -> Result<&'a Metadata, String> {
    if buffer.len() < mem::size_of::<Metadata>() {
        return Err("Memory block is too small")
    }
    let meta: &Metadata = read::<Metadata>();
    if meta.magic != MAGIC_ID {
        return Err("This data does not represent a valid SliceBTree")
    }
    Ok(meta)
}
