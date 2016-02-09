#[derive(Clone, PartialEq, Debug)]
#[repr(C)]
pub struct EntryLocation {
    pub page_index: usize,
    pub offset: usize,
}

#[derive(Clone, PartialEq, Debug)]
#[repr(u8)]
pub enum MemType {
    Meta = 0xA,
    Root,
    Internal,
    Leaf,
    Alias,
    Entry,
    Deleted,
}
