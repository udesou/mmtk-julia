use mmtk::util::{Address, ObjectReference};
use mmtk::vm::ObjectModel;
use mmtk::util::copy::*;
use crate::{JuliaVM, UPCALLS, init_boot_image_metadata_info};
use mmtk::util::constants::BYTES_IN_PAGE;
use mmtk::util::metadata::side_metadata::{
    SideMetadataSpec, SideMetadataOffset, SideMetadataContext
};
use mmtk::vm::*;

pub struct VMObjectModel {}

/// Global logging bit metadata spec
/// 1 bit per object
pub(crate) const LOGGING_SIDE_METADATA_SPEC: VMGlobalLogBitSpec =
    VMGlobalLogBitSpec::side_first();

pub(crate) const MARKING_METADATA_SPEC: VMLocalMarkBitSpec =
    VMLocalMarkBitSpec::side_after(LOS_METADATA_SPEC.as_spec());

// pub(crate) const LOCAL_FORWARDING_POINTER_METADATA_SPEC: VMLocalForwardingPointerSpec =
//     VMLocalForwardingPointerSpec::side_after(MARKING_METADATA_SPEC.as_spec());

pub(crate) const LOCAL_PINNING_METADATA_BITS_SPEC: VMLocalPinningBitSpec =
VMLocalPinningBitSpec::side_after(MARKING_METADATA_SPEC.as_spec());


pub(crate) const BI_MARKING_METADATA_SPEC: SideMetadataSpec =
SideMetadataSpec {
    name : "BI_MARK",
    is_global: false,
    offset: SideMetadataOffset::layout_after(LOCAL_PINNING_METADATA_BITS_SPEC.as_spec().extract_side_spec()),
    log_num_of_bits: 0,
    log_bytes_in_region: 3
};

lazy_static! {
    pub static ref BI_METADATA_CONTEXT: SideMetadataContext =
    SideMetadataContext {
        global : vec![],
        local : vec![BI_MARKING_METADATA_SPEC],
    };
}

/// PolicySpecific mark-and-nursery bits metadata spec
/// 2-bits per object
pub(crate) const LOS_METADATA_SPEC: VMLocalLOSMarkNurserySpec =
    VMLocalLOSMarkNurserySpec::side_first();


impl ObjectModel<JuliaVM> for VMObjectModel {
    const GLOBAL_LOG_BIT_SPEC: VMGlobalLogBitSpec = LOGGING_SIDE_METADATA_SPEC;
    const LOCAL_FORWARDING_POINTER_SPEC: VMLocalForwardingPointerSpec = VMLocalForwardingPointerSpec::in_header(-64);
    const LOCAL_FORWARDING_BITS_SPEC: VMLocalForwardingBitsSpec = VMLocalForwardingBitsSpec::in_header(-64);
    const LOCAL_MARK_BIT_SPEC: VMLocalMarkBitSpec = MARKING_METADATA_SPEC;
    const LOCAL_LOS_MARK_NURSERY_SPEC: VMLocalLOSMarkNurserySpec = LOS_METADATA_SPEC;
    const LOCAL_PINNING_BIT_SPEC: VMLocalPinningBitSpec = LOCAL_PINNING_METADATA_BITS_SPEC;

    fn copy(
        from: ObjectReference,
        semantics: CopySemantics,
        copy_context: &mut GCWorkerCopyContext<JuliaVM>,
    ) -> ObjectReference {
        let bytes = Self::get_current_size(from);
        let from_start_ref = unsafe { Self::object_start_ref(from).to_object_reference() };
        let header_offset = from.to_address().as_usize() - from_start_ref.to_address().as_usize();

        let dst =
        if header_offset == 8 {
            // regular object
            copy_context.alloc_copy( from_start_ref, bytes, 16, 8, semantics)
        } else if header_offset == 16 {
            unimplemented!();
            // buffer
            // copy_context.alloc_copy( from_start_ref, bytes, 16, 0, semantics)
        } else {
            unimplemented!()
        };

        let src = Self::object_start_ref(from);
        unsafe { std::ptr::copy_nonoverlapping::<u8>(src.to_ptr(), dst.to_mut_ptr(), bytes); }
        let to_obj = unsafe { (dst + header_offset).to_object_reference() };
        copy_context.post_copy(to_obj, bytes, semantics);

        // use std::fs::OpenOptions;
        // use std::io::Write;

        // let mut file = OpenOptions::new()
        //         .write(true)
        //         .append(true)
        //         .create(true)
        //         .open("/home/eduardo/mmtk-julia/copied_objs.log")
        //         .unwrap();

        // if let Err(e) = writeln!(file, "copied object from {} to {}", from, to_obj) {
        //         eprintln!("Couldn't write to file: {}", e);
        // }

        unsafe {
            ((*UPCALLS).introspect_objects_after_copying)(from.to_address(), to_obj.to_address())
        }

        to_obj
    }

    fn copy_to(_from: ObjectReference, _to: ObjectReference, _region: Address) -> Address {
        unimplemented!()
    }

    fn get_current_size(object: ObjectReference) -> usize {
        let size = if is_object_in_los(&object) {
            unsafe {
                ((*UPCALLS).get_lo_size)(object)
            }
        } else {
            let obj_size = unsafe {
                ((*UPCALLS).get_so_size)(object)
            };
            obj_size
        };

        size
    }

    fn get_size_when_copied(_object: ObjectReference) -> usize {
        unimplemented!()
    }

    fn get_align_when_copied(_object: ObjectReference) -> usize {
        unimplemented!()
    }

    fn get_align_offset_when_copied(_object: ObjectReference) -> isize {
        unimplemented!()
    }

    fn get_reference_when_copied_to(_from: ObjectReference, _to: Address) -> ObjectReference {
        unimplemented!()
    }

    fn get_type_descriptor(_reference: ObjectReference) -> &'static [i8] {
        unimplemented!()
    }

    fn object_start_ref(object: ObjectReference) -> Address {
        let res = if is_object_in_los(&object) {
            object.to_address() - 48
        } else {
            unsafe {
                ((*UPCALLS).get_object_start_ref)(object)
            }
        };
        res
    }

    fn ref_to_address(object: ObjectReference) -> Address {
        object.to_address()
    }

    fn dump_object(_object: ObjectReference) {
        unimplemented!()
    }

    fn is_object_pinned(object: ObjectReference) -> bool {
        // unsafe { LOCAL_PINNING_METADATA_BITS_SPEC.load::<JuliaVM, u8>(object, None) == 1 }
        true
    }
}

pub fn is_object_in_los(object: &ObjectReference) -> bool {
    (*object).to_address().as_usize() > 0x60000000000
}


#[no_mangle]
pub extern "C" fn map_boot_image_metadata(start: Address, end: Address) {
    let start_address_aligned_down = start.align_down(BYTES_IN_PAGE);
    let end_address_aligned_up = end.align_up(BYTES_IN_PAGE);
    unsafe {
        init_boot_image_metadata_info(start_address_aligned_down.as_usize(), end_address_aligned_up.as_usize());
    }
    let res = BI_METADATA_CONTEXT.try_map_metadata_space(start_address_aligned_down, end_address_aligned_up.as_usize() - start_address_aligned_down.as_usize());

    match res {
        Ok(_) => (),
        Err(e) => panic!("Mapping failed with error {}", e)
    }
}