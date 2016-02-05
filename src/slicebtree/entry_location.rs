#[derive(Clone, PartialEq, Debug)]
#[repr(C)]
pub struct EntryLocation {
    pub page_index: usize,
    pub offset: usize,
}
