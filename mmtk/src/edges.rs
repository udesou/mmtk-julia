use atomic::Atomic;
use mmtk::{
    util::{Address, ObjectReference},
    vm::edge_shape::{Edge, SimpleEdge},
};

/// If a VM supports multiple kinds of edges, we can use tagged union to represent all of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum JuliaVMEdge {
    Simple(SimpleEdge),
    Offset(OffsetEdge),
    Masked(MaskedEdge),
}

unsafe impl Send for JuliaVMEdge {}

impl Edge for JuliaVMEdge {
    fn load(&self) -> ObjectReference {
        match self {
            JuliaVMEdge::Simple(e) => e.load(),
            JuliaVMEdge::Offset(e) => e.load(),
            JuliaVMEdge::Masked(e) => e.load(),
        }
    }

    fn store(&self, object: ObjectReference) {
        match self {
            JuliaVMEdge::Simple(e) => e.store(object),
            JuliaVMEdge::Offset(e) => e.store(object),
            JuliaVMEdge::Masked(e) => e.store(object),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OffsetEdge {
    slot_addr: *mut Atomic<Address>,
    offset: usize,
}

unsafe impl Send for OffsetEdge {}

impl OffsetEdge {
    pub fn new_no_offset(address: Address) -> Self {
        Self {
            slot_addr: address.to_mut_ptr(),
            offset: 0,
        }
    }

    pub fn new_with_offset(address: Address, offset: usize) -> Self {
        Self {
            slot_addr: address.to_mut_ptr(),
            offset,
        }
    }

    pub fn slot_address(&self) -> Address {
        Address::from_mut_ptr(self.slot_addr)
    }

    pub fn offset(&self) -> usize {
        self.offset
    }
}

impl Edge for OffsetEdge {
    fn load(&self) -> ObjectReference {
        let middle = unsafe { (*self.slot_addr).load(atomic::Ordering::Relaxed) };
        let begin = middle - self.offset;
        unsafe { begin.to_object_reference() }
    }

    fn store(&self, object: ObjectReference) {
        let begin = object.to_address();
        let middle = begin + self.offset;
        unsafe { (*self.slot_addr).store(middle, atomic::Ordering::Relaxed) }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MaskedEdge {
    slot_addr: *mut Atomic<Address>,
    mask:  usize,
}

unsafe impl Send for MaskedEdge {}

impl MaskedEdge {
    pub fn new(address: Address) -> Self {
        Self {
            slot_addr: address.to_mut_ptr(),
            mask: 3,
        }
    }

    pub fn slot_address(&self) -> Address {
        Address::from_mut_ptr(self.slot_addr)
    }

    pub fn mask(&self) -> usize {
        self.mask
    }
}

impl Edge for MaskedEdge {
    fn load(&self) -> ObjectReference {
        let masked_obj = unsafe { (*self.slot_addr).load(atomic::Ordering::Relaxed) };
        let obj = masked_obj.as_usize() & !self.mask;
        unsafe { Address::from_usize(obj).to_object_reference() }
    }

    //FIXME how to implement this?
    fn store(&self, object: ObjectReference) {
        let masked_obj = unsafe { (*self.slot_addr).load(atomic::Ordering::Relaxed) };
        let tag = masked_obj.as_usize() & self.mask;

        let obj = object.to_address().as_usize() | tag;
        unsafe { (*self.slot_addr).store(Address::from_usize(obj), atomic::Ordering::Relaxed) }
    }
}