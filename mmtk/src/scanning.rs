use mmtk::vm::Scanning;
use mmtk::memory_manager;
use mmtk::Mutator;
use mmtk::vm::EdgeVisitor;
use mmtk::vm::RootsWorkFactory;
use mmtk::util::ObjectReference;
use mmtk::util::opaque_pointer::*;
use mmtk::scheduler::*;
use crate::TASK_ROOTS;
use crate::api::mmtk_pin_object;
use crate::julia_scanning::read_stack;
use crate::julia_types::mmtk_jl_gcframe_t;
use crate::julia_types::mmtk_jl_task_t;
use crate::{SINGLETON, ROOTS, UPCALLS};
use crate::object_model::BI_MARKING_METADATA_SPEC;
use mmtk::util::Address;
use mmtk::MMTK;
use mmtk::vm::VMBinding;
use crate::edges::JuliaVMEdge;
#[cfg(feature = "scan_obj_c")]
use crate::julia_scanning::process_edge;
#[cfg(feature = "scan_obj_c")]
use crate::julia_scanning::process_offset_edge;

use log::info;
use std::sync::MutexGuard;
use std::collections::{HashSet};
use std::sync::atomic::Ordering;
use crate::JuliaVM;

pub struct VMScanning {
}

impl Scanning<JuliaVM> for VMScanning {

    fn scan_vm_immovable_roots(_tls: VMWorkerThread, mut factory: impl RootsWorkFactory<<JuliaVM as VMBinding>::VMEdge>) {
        // scan immovable roots
        let mut roots: MutexGuard<HashSet<Address>> = TASK_ROOTS.lock().unwrap();
        info!("{} task roots", roots.len());

        let mut roots_to_scan = vec![];

        // roots may contain mmtk objects
        for obj in roots.drain() {
            let red_stack_roots = unsafe { collect_stack_roots(obj) };
            for stack_root in red_stack_roots {
                if object_is_managed_by_mmtk(stack_root.to_address().as_usize()) {
                    mmtk_pin_object(stack_root);
                }
                roots_to_scan.push(stack_root);  
            }        
        }

        factory.create_process_node_roots_work(roots_to_scan);
    }

    fn scan_thread_roots(_tls: VMWorkerThread, _factory: impl RootsWorkFactory<JuliaVMEdge>) {
        
    }

    fn scan_thread_root(_tls: VMWorkerThread, _mutator: &'static mut Mutator<JuliaVM>, _factory: impl RootsWorkFactory<JuliaVMEdge>) {
        unimplemented!()
    }
    fn scan_vm_specific_roots(_tls: VMWorkerThread, mut factory: impl RootsWorkFactory<JuliaVMEdge>) {
        let mut roots: MutexGuard<HashSet<Address>> = ROOTS.lock().unwrap();
        info!("{} thread roots", roots.len());

        let mut roots_to_scan = vec![];

        // roots may contain mmtk objects
        for obj in roots.drain() {
            let obj_ref = unsafe {obj.to_object_reference()};
            if object_is_managed_by_mmtk(obj.as_usize()) {
                mmtk_pin_object(obj_ref);
            }
            roots_to_scan.push(obj_ref);          
        }

        factory.create_process_node_roots_work(roots_to_scan);
    }

    fn scan_object<EV: EdgeVisitor<JuliaVMEdge>>(_tls: VMWorkerThread, object: ObjectReference, edge_visitor: &mut EV) {
        process_object(object, edge_visitor);
    }
    fn notify_initial_thread_scan_complete(_partial_scan: bool, _tls: VMWorkerThread) {
        // Specific to JikesRVM - using it to load the work for sweeping malloced arrays
        let sweep_malloced_arrays_work = SweepMallocedArrays::new();
        memory_manager::add_work_packet(&SINGLETON, WorkBucketStage::Compact, sweep_malloced_arrays_work);
    }
    fn supports_return_barrier() -> bool {
        unimplemented!()
    }

    fn prepare_for_roots_re_scanning() { unimplemented!() }
}

unsafe fn collect_stack_roots(addr : Address) -> Vec<ObjectReference> {
    let mut stack_roots = HashSet::new();


    let ta = addr.to_ptr::<mmtk_jl_task_t>();
    let mut s = (*ta).gcstack;
    
    let (offset, lb, ub) = (0, 0, usize::MAX);

    if s != std::ptr::null_mut() {
        let s_nroots_addr = ::std::ptr::addr_of!((*s).nroots);
        let nroots_addr = read_stack(Address::from_ptr(s_nroots_addr), offset, lb, ub);
        let mut nroots = nroots_addr.load::<usize>();
        let mut nr = nroots >> 2;

        loop {
            let rts = Address::from_mut_ptr(s) + 2 * std::mem::size_of::<Address>() as usize;                
            for i in 0..nr {
                if (nroots & 1) != 0 {
                    let slot = read_stack(rts + (i * std::mem::size_of::<Address>()), offset, lb, ub);
                    let real_addr = read_stack(slot.load::<Address>(), offset, lb, ub);
                    let obj : ObjectReference = real_addr.load();
                    stack_roots.insert(obj);
                } else {
                    let slot = read_stack(rts + (i * std::mem::size_of::<Address>()), offset, lb, ub);
                    let obj : ObjectReference = slot.load();
                    stack_roots.insert(obj);
                }
            }

            let new_s = read_stack(Address::from_mut_ptr((*s).prev), offset, lb, ub);
            s = new_s.to_mut_ptr::<mmtk_jl_gcframe_t>();
            if s != std::ptr::null_mut() {
                let s_nroots_addr = ::std::ptr::addr_of!((*s).nroots);
                nroots = read_stack(Address::from_ptr(s_nroots_addr), offset, lb, ub).load();
                nr = nroots >> 2;
                continue;
            }
            break;
        }
    }


    stack_roots.into_iter().collect()
}

pub fn process_object(object: ObjectReference, closure: &mut dyn EdgeVisitor<JuliaVMEdge>) {
    let addr = object.to_address();

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

// Sweep malloced arrays work
pub struct SweepMallocedArrays {
    swept: bool
}

impl SweepMallocedArrays {
    pub fn new() -> Self {
        Self {
            swept: false
        }
    }
}

impl<VM:VMBinding> GCWork<VM> for SweepMallocedArrays {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        // call sweep malloced arrays from UPCALLS
        unsafe {
            ((*UPCALLS).sweep_malloced_array)()
        }
        self.swept = true;
    }
}

#[no_mangle]
pub extern "C" fn mark_metadata_scanned(addr : Address) {
    BI_MARKING_METADATA_SPEC.store_atomic::<u8>(addr, 1, Ordering::SeqCst );
}

#[no_mangle]
pub extern "C" fn check_metadata_scanned(addr : Address) -> u8 {
    BI_MARKING_METADATA_SPEC.load_atomic::<u8>( addr, Ordering::SeqCst )
}
