#[cfg(feature = "object_pinning")]
use crate::api::mmtk_pin_object;
use crate::edges::JuliaVMEdge;
use crate::julia_scanning::jl_task_type;
use crate::julia_scanning::mmtk_jl_typeof;
#[cfg(feature = "scan_obj_c")]
use crate::julia_scanning::process_edge;
#[cfg(feature = "scan_obj_c")]
use crate::julia_scanning::process_offset_edge;
use crate::julia_types::mmtk_jl_datatype_layout_t;
use crate::julia_types::mmtk_jl_datatype_t;
use crate::julia_types::mmtk_jl_task_t;
use crate::object_model::BI_MARKING_METADATA_SPEC;
use crate::reference_glue::jl_nothing;
#[cfg(feature = "object_pinning")]
use crate::PINNED_ROOTS;
use crate::RED_ROOTS;
use crate::TASK_ROOTS;
use crate::{ROOTS, SINGLETON, UPCALLS};
use mmtk::memory_manager;
use mmtk::scheduler::*;
use mmtk::util::opaque_pointer::*;
use mmtk::util::Address;
use mmtk::util::ObjectReference;
use mmtk::vm::EdgeVisitor;
use mmtk::vm::RootsWorkFactory;
use mmtk::vm::Scanning;
use mmtk::vm::VMBinding;
use mmtk::Mutator;
use mmtk::MMTK;

use crate::JuliaVM;
use log::info;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::MutexGuard;

pub struct VMScanning {}

impl Scanning<JuliaVM> for VMScanning {
    fn scan_thread_roots(_tls: VMWorkerThread, mut factory: impl RootsWorkFactory<JuliaVMEdge>) {
        let mut roots: MutexGuard<HashSet<Address>> = ROOTS.lock().unwrap();
        info!("{} thread roots", roots.len());

        let mut roots_to_scan = vec![];

        #[cfg(feature = "object_pinning")]
        for obj in roots.iter() {
            let obj_ref = ObjectReference::from_raw_address(*obj);
            let pinned_root = mmtk_pin_object(obj_ref);
            if pinned_root {
                PINNED_ROOTS.write().unwrap().insert(*obj);
            }
        }

        // roots may contain mmtk objects
        for obj in roots.drain() {
            let obj_ref = ObjectReference::from_raw_address(obj);
            roots_to_scan.push(obj_ref);
        }

        factory.create_process_node_roots_work(roots_to_scan, false);
    }

    fn scan_thread_root(
        _tls: VMWorkerThread,
        _mutator: &'static mut Mutator<JuliaVM>,
        _factory: impl RootsWorkFactory<JuliaVMEdge>,
    ) {
        unimplemented!()
    }
    fn scan_vm_specific_roots(
        _tls: VMWorkerThread,
        mut factory: impl RootsWorkFactory<JuliaVMEdge>,
    ) {
        // scan immovable roots
        let mut roots: MutexGuard<HashSet<Address>> = TASK_ROOTS.lock().unwrap();
        info!("{} task roots", roots.len());

        unsafe { process_stack_roots(roots.drain().collect()) };

        let mut red_roots: MutexGuard<HashSet<Address>> = RED_ROOTS.lock().unwrap();
        let mut roots_to_scan = vec![];

        for obj in red_roots.drain() {
            let obj_ref = ObjectReference::from_raw_address(obj);
            roots_to_scan.push(obj_ref);
        }

        factory.create_process_node_roots_work(roots_to_scan, true);
    }

    fn scan_object<EV: EdgeVisitor<JuliaVMEdge>>(
        _tls: VMWorkerThread,
        object: ObjectReference,
        edge_visitor: &mut EV,
    ) {
        process_object(object, edge_visitor);
    }
    fn notify_initial_thread_scan_complete(_partial_scan: bool, _tls: VMWorkerThread) {
        // Specific to JikesRVM - using it to load the work for sweeping malloced arrays
        let sweep_malloced_arrays_work = SweepJuliaSpecific::new();
        memory_manager::add_work_packet(
            &SINGLETON,
            WorkBucketStage::Compact,
            sweep_malloced_arrays_work,
        );
    }
    fn supports_return_barrier() -> bool {
        unimplemented!()
    }

    fn prepare_for_roots_re_scanning() {
        unimplemented!()
    }
}

unsafe fn process_stack_roots(mut roots: Vec<Address>) {
    let mut processed_roots = HashSet::new();

    while !roots.is_empty() {
        let root = roots.pop().unwrap();
        processed_roots.insert(root);

        let root_type = mmtk_jl_typeof(root);
        assert_eq!(root_type, Address::from_ptr(jl_task_type));
        let ta = root.to_ptr::<mmtk_jl_task_t>();

        let next = (*ta).next;
        if next != Address::from_mut_ptr(jl_nothing).to_mut_ptr() {
            let next_addr = Address::from_mut_ptr(next);
            let next_task_type = mmtk_jl_typeof(next_addr);
            assert_eq!(next_task_type, Address::from_ptr(jl_task_type));
            if !processed_roots.contains(&next_addr) {
                roots.push(next_addr);
            }
        }

        let queue = (*ta).queue;
        if queue != Address::from_mut_ptr(jl_nothing).to_mut_ptr() {
            let queue_addr = Address::from_mut_ptr(queue);
            let obj_type_addr = mmtk_jl_typeof(queue_addr);
            let obj_type = obj_type_addr.to_ptr::<mmtk_jl_datatype_t>();
            let layout = (*obj_type).layout;
            let npointers = (*layout).npointers;
            let layout_fields =
                Address::from_ptr(layout) + std::mem::size_of::<mmtk_jl_datatype_layout_t>();
            let fielddesc_size = 2 << (*layout).fielddesc_type();
            let obj8_start_addr = layout_fields + fielddesc_size * (*layout).nfields as usize;
            let obj8_end_addr = obj8_start_addr + npointers as usize;
            for elem_usize in obj8_start_addr.as_usize()..obj8_end_addr.as_usize() {
                let elem = queue_addr.to_mut_ptr::<*mut libc::c_uchar>();
                let index = Address::from_usize(elem_usize).to_mut_ptr::<libc::c_uchar>();
                let slot: *mut libc::c_uchar = &mut *elem.offset(*index as isize)
                    as *mut *mut libc::c_uchar
                    as *mut libc::c_uchar;
                let queued_task = Address::from_mut_ptr(slot).load::<Address>();
                if !processed_roots.contains(&queued_task) {
                    let queued_task_type = mmtk_jl_typeof(queued_task);
                    assert_eq!(queued_task_type, Address::from_ptr(jl_task_type));
                    roots.push(queued_task);
                }
            }
        }

        ((*UPCALLS).collect_stack_roots)(root);
    }
}

pub fn process_object(object: ObjectReference, closure: &mut dyn EdgeVisitor<JuliaVMEdge>) {
    let addr = object.to_raw_address();

    #[cfg(feature = "scan_obj_c")]
    {
        unsafe {
            ((*UPCALLS).scan_julia_obj)(addr, closure, process_edge as _, process_offset_edge as _)
        };
    }

    #[cfg(not(feature = "scan_obj_c"))]
    unsafe {
        crate::julia_scanning::scan_julia_object(addr, closure);
    }
}

#[no_mangle]
pub extern "C" fn object_is_managed_by_mmtk(addr: usize) -> bool {
    memory_manager::is_mapped_address(unsafe { Address::from_usize(addr) })
}

// Sweep Julia specific data structures work
pub struct SweepJuliaSpecific {
    swept: bool,
}

impl SweepJuliaSpecific {
    pub fn new() -> Self {
        Self { swept: false }
    }
}

impl<VM: VMBinding> GCWork<VM> for SweepJuliaSpecific {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        // call sweep malloced arrays from UPCALLS
        unsafe {
            ((*UPCALLS).mmtk_sweep_malloced_array)();
            ((*UPCALLS).mmtk_sweep_stack_pools)();
        }
        self.swept = true;
    }
}

#[no_mangle]
pub extern "C" fn mark_metadata_scanned(addr: Address) {
    BI_MARKING_METADATA_SPEC.store_atomic::<u8>(addr, 1, Ordering::SeqCst);
}

#[no_mangle]
pub extern "C" fn check_metadata_scanned(addr: Address) -> u8 {
    BI_MARKING_METADATA_SPEC.load_atomic::<u8>(addr, Ordering::SeqCst)
}
