/// A file block is a view into a memory mapped file.
/// It represents a contiguous sequence of bytes that
/// are interpreted as follows:
/// - [0..4]               block_type: u32
/// - [4..12]              data_size: u64
/// - [12..metadata_size]  metadata_block*
/// - remaining data_size bytes are for data storage
/// The metadata segment is split into 0 or more blocks.
/// Each block is a contiguous sequence of bytes that
/// is interpreted as follows:
/// - [0..4] block_size: u32
/// - [4..8] block_type: u32
/// - remaining block_size bytes are for metadata storage

// pub struct FileBlock<'a> {
//     header: &'a FileBlockHeader,
//
// }
//
// #[repr(C)]
// pub struct FileBlockHeader {
//     metadata_size: u32,
//     data_size: u32,
// }
//
// #[repr(C)]
// pub struct MetadataBlockHeader {
//
// }
