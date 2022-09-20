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
use crate::julia_scanning::jl_task_type;
use crate::julia_scanning::mmtk_jl_typeof;
use crate::julia_scanning::process_masked_edge;
use crate::julia_scanning::read_stack;
use crate::julia_types::mmtk_jl_datatype_t;
use crate::julia_types::mmtk_jl_gcframe_t;
use crate::julia_types::mmtk_jl_task_t;
use crate::{SINGLETON, ROOTS, UPCALLS};
use crate::object_model::BI_MARKING_METADATA_SPEC;
use mmtk::util::Address;
use mmtk::MMTK;
use mmtk::vm::VMBinding;
use mmtk::util::metadata::side_metadata::{load_atomic, store_atomic};
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
    fn scan_thread_roots(_tls: VMWorkerThread, _factory: impl RootsWorkFactory<JuliaVMEdge>) {
        // Thread roots are collected by Julia before stopping the world
    }

    fn scan_thread_root(_tls: VMWorkerThread, _mutator: &'static mut Mutator<JuliaVM>, _factory: impl RootsWorkFactory<JuliaVMEdge>) {
        unimplemented!()
    }
    fn scan_vm_specific_roots(_tls: VMWorkerThread, mut factory: impl RootsWorkFactory<JuliaVMEdge>) {
        let task_roots: MutexGuard<HashSet<Address>> = TASK_ROOTS.lock().unwrap();
        info!("{} task roots", task_roots.len());

        collect_thread_roots(task_roots);

        let mut roots: MutexGuard<HashSet<Address>> = ROOTS.lock().unwrap();
        info!("{} thread roots", roots.len());

        let mut roots_to_scan = vec![];

        // use std::fs::OpenOptions;
        // use std::io::Write;

        // let mut file = OpenOptions::new()
        //         .write(true)
        //         .append(true)
        //         .create(true)
        //         .open("/home/eduardo/mmtk-julia/root_objs.log")
        //         .unwrap();

        

        // roots may contain mmtk objects
        for obj in roots.drain() {
            // if let Err(e) = writeln!(file, "root = {}", obj) {
            //     eprintln!("Couldn't write to file: {}", e);
            // }
            roots_to_scan.push(unsafe {obj.to_object_reference()} );
            if object_is_managed_by_mmtk(obj.as_usize()) {
                memory_manager::pin_object(&SINGLETON, unsafe { obj.to_object_reference() });  
            }
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

pub fn collect_thread_roots(mut task_objects: MutexGuard<HashSet<Address>>) {
    for obj in task_objects.drain() {
        unsafe {
            collect_thread_roots_from_task(obj);
        }
    }
}

pub unsafe fn collect_thread_roots_from_task(obj : Address) {
    let obj_type_addr = mmtk_jl_typeof(obj);
    let obj_type = obj_type_addr.to_ptr::<mmtk_jl_datatype_t>();

    let obj_addr = obj;
    assert_eq!(obj_type, jl_task_type);
    let ta = obj_addr.to_ptr::<mmtk_jl_task_t>();
    let stkbuf = (*ta).stkbuf;
    let stkbuf_addr = Address::from_mut_ptr(stkbuf);
    let copy_stack = (*ta).copy_stack();

    let mut s = (*ta).gcstack;
    
    let (mut offset, mut lb, mut ub) = (0, 0, usize::MAX);
    // FIXME: the code below is executed COPY_STACKS has been defined in the C Julia implementation - it is on by default
    if !stkbuf_addr.is_zero() && copy_stack != 0 && (*ta).ptls != std::ptr::null_mut() {
        if (*ta).tid < 0 {
            panic!("tid must be positive.")
        }
        let stackbase = ((*UPCALLS).get_stackbase)((*ta).tid);
        ub = stackbase;
        lb = ub - (*ta).copy_stack() as usize;
        offset = Address::from_mut_ptr(stkbuf).as_usize() - lb as usize;
    }
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
                    let internal_obj: ObjectReference = real_addr.load() ;

                    // use std::fs::OpenOptions;
                    // use std::io::Write;

                    // let mut file = OpenOptions::new()
                    //         .write(true)
                    //         .append(true)
                    //         .create(true)
                    //         .open("/home/eduardo/mmtk-julia/stackroot_objs.log")
                    //         .unwrap();
                    
                    // if let Err(e) = writeln!(file, "root = {}", internal_obj) {
                    //     eprintln!("Couldn't write to file: {}", e);
                    // }

                    if object_is_managed_by_mmtk(internal_obj.to_address().as_usize()) {
                        mmtk_pin_object(internal_obj);
                    }
                } else {
                    let slot = read_stack(rts + (i * std::mem::size_of::<Address>()), offset, lb, ub);
                    let internal_obj: ObjectReference = slot.load() ;

                    // use std::fs::OpenOptions;
                    // use std::io::Write;

                    // let mut file = OpenOptions::new()
                    //         .write(true)
                    //         .append(true)
                    //         .create(true)
                    //         .open("/home/eduardo/mmtk-julia/stackroot_objs.log")
                    //         .unwrap();

                    // if let Err(e) = writeln!(file, "root = {}", internal_obj) {
                    //     eprintln!("Couldn't write to file: {}", e);
                    // }
        
                    if object_is_managed_by_mmtk(internal_obj.to_address().as_usize()) {
                        mmtk_pin_object(internal_obj);
                    }
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
}

pub fn process_object(object: ObjectReference, closure: &mut dyn EdgeVisitor<JuliaVMEdge>) {
    let addr = object.to_address();

    #[cfg(feature = "scan_obj_c")]
    {
        unsafe {
            ((*UPCALLS).scan_julia_obj)(addr, closure, process_edge as _, process_offset_edge as _, process_masked_edge as _)
        };
    }

    #[cfg(not(feature = "scan_obj_c"))]
    unsafe {
        crate::julia_scanning::scan_julia_object(addr, closure);
    }
}

#[no_mangle]
pub extern "C" fn object_is_managed_by_mmtk(addr: usize) -> bool {
    let res = addr >= crate::api::starting_heap_address().as_usize()
        && addr <= crate::api::last_heap_address().as_usize();

    res
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
    store_atomic(&BI_MARKING_METADATA_SPEC, addr, 1, Ordering::SeqCst );
}

#[no_mangle]
pub extern "C" fn check_metadata_scanned(addr : Address) -> usize {
    load_atomic(&BI_MARKING_METADATA_SPEC, addr, Ordering::SeqCst )
}
