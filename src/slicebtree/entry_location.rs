#[derive(Clone, PartialEq)]
#[repr(C)]
pub struct EntryLocation {
    pub page_index: usize,
    pub offset: usize,
}

pub const END: EntryLocation = EntryLocation {
    page_index: 0xFFFF_FFFF,
    offset: 0xFFFF_FFFF,
};
