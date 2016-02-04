use std::mem;

/// Each page is 4096 bytes
pub type Page = [u8; 0x1000];

pub type PageIndex = usize;

pub trait FlexibleMemory {
    fn transmute_page<T>(&self) -> &T;
    fn transmute_segment<T>(&self, offset: u16) -> &T;

    fn transmute_page_mut<T>(&mut self) -> &mut T;
    fn transmute_segment_mut<T>(&mut self, offset: u16) -> &mut T;
}

impl FlexibleMemory for Page {
    fn transmute_page<T>(&self) -> &T {
        let subsection = &self[0];
        unsafe {
            mem::transmute(subsection)
        }
    }

    fn transmute_segment<T>(&self, offset: u16) -> &T {
        let offset = offset as usize;
        let subsection = &self[offset];
        unsafe {
            mem::transmute(subsection)
        }
    }

    fn transmute_page_mut<T>(&mut self) -> &mut T {
        let subsection = &mut self[0];
        unsafe {
            mem::transmute(subsection)
        }
    }

    fn transmute_segment_mut<T>(&mut self, offset: u16) -> &mut T {
        let offset = offset as usize;
        let subsection = &mut self[offset];
        unsafe {
            mem::transmute(subsection)
        }
    }
}

#[test]
fn test_transmute_whole() {
    #[repr(C)]
    struct Composite {
        signed: isize,
        unsigned: usize,
    }

    let mut p = [0u8; 0x1000];
    {
        let c = p.transmute_page_mut::<Composite>();
        c.signed = -17;
        c.unsigned = 56;
    }

    assert_eq!([239, 255, 255, 255, 255, 255, 255, 255][..], p[0..8]);
    assert_eq!([56, 0, 0, 0, 0, 0, 0, 0][..], p[8..16]);
}

#[test]
fn test_transmute_part() {
    #[repr(C)]
    struct Composite {
        signed: isize,
        unsigned: usize,
    }

    let mut p = [0u8; 0x1000];
    {
        let c = p.transmute_segment_mut::<Composite>(64);
        c.signed = -17;
        c.unsigned = 56;
    }

    assert_eq!([239, 255, 255, 255, 255, 255, 255, 255][..], p[64..72]);
    assert_eq!([56, 0, 0, 0, 0, 0, 0, 0][..], p[72..80]);
}
