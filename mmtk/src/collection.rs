// use crate::api::mmtk_immixspace_fragmentation_rate;
use crate::JuliaVM;
use crate::{SINGLETON, UPCALLS};
use log::{info, trace};
use mmtk::util::alloc::AllocationError;
use mmtk::util::heap::GCTriggerPolicy;
use mmtk::util::opaque_pointer::*;
use mmtk::vm::ActivePlan;
use mmtk::vm::{Collection, GCThreadContext};
use mmtk::Mutator;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use crate::{BLOCK_FOR_GC, STW_COND, WORLD_HAS_STOPPED};

static GC_START: AtomicU64 = AtomicU64::new(0);

extern "C" {
    pub static jl_gc_disable_counter: AtomicU32;
}

pub struct VMCollection {}

impl Collection<JuliaVM> for VMCollection {
    fn stop_all_mutators<F>(_tls: VMWorkerThread, mut mutator_visitor: F)
    where
        F: FnMut(&'static mut Mutator<JuliaVM>),
    {
        // Wait for all mutators to stop and all finalizers to run
        while !AtomicBool::load(&WORLD_HAS_STOPPED, Ordering::SeqCst) {
            // Stay here while the world has not stopped
            // FIXME add wait var
        }

        trace!("Stopped the world!");

        // Tell MMTk the stacks are ready.
        {
            for mutator in crate::active_plan::VMActivePlan::mutators() {
                info!("stop_all_mutators: visiting {:?}", mutator.mutator_tls);
                mutator_visitor(mutator);
            }
        }

        // Record the start time of the GC
        let now = unsafe { ((*UPCALLS).jl_hrtime)() };
        trace!("gc_start = {}", now);
        GC_START.store(now, Ordering::Relaxed);
    }

    fn resume_mutators(_tls: VMWorkerThread) {
        // Get the end time of the GC
        let end = unsafe { ((*UPCALLS).jl_hrtime)() };
        trace!("gc_end = {}", end);
        let gc_time = end - GC_START.load(Ordering::Relaxed);
        unsafe {
            ((*UPCALLS).update_gc_stats)(
                gc_time,
                crate::api::mmtk_used_bytes(),
                is_current_gc_nursery(),
            )
        }

        AtomicBool::store(&BLOCK_FOR_GC, false, Ordering::SeqCst);
        AtomicBool::store(&WORLD_HAS_STOPPED, false, Ordering::SeqCst);

        let &(_, ref cvar) = &*STW_COND.clone();
        cvar.notify_all();

        info!(
            "Live bytes = {}, total bytes = {}",
            crate::api::mmtk_used_bytes(),
            crate::api::mmtk_total_bytes()
        );

        // #[cfg(feature = "print_fragmentation_stats")]
        // println!(
        //     "{} Fragmentation rate for immixspace (live_bytes / used_pages_as_bytes) = {}%",
        //     chrono::offset::Local::now().format("%Y-%m-%d %H:%M:%S"),
        //     mmtk_immixspace_fragmentation_rate()
        // );

        trace!("Resuming mutators.");
    }

    fn block_for_gc(_tls: VMMutatorThread) {
        info!("Triggered GC!");

        unsafe { ((*UPCALLS).prepare_to_collect)() };

        info!("Finished blocking mutator for GC!");
    }

    fn spawn_gc_thread(_tls: VMThread, ctx: GCThreadContext<JuliaVM>) {
        // Just drop the join handle. The thread will run until the process quits.
        let _ = std::thread::spawn(move || {
            use mmtk::util::opaque_pointer::*;
            use mmtk::util::Address;
            let worker_tls = VMWorkerThread(VMThread(OpaquePointer::from_address(unsafe {
                Address::from_usize(thread_id::get())
            })));
            match ctx {
                GCThreadContext::Controller(mut c) => {
                    mmtk::memory_manager::start_control_collector(&SINGLETON, worker_tls, &mut c)
                }
                GCThreadContext::Worker(mut w) => {
                    mmtk::memory_manager::start_worker(&SINGLETON, worker_tls, &mut w)
                }
            }
        });
    }

    fn schedule_finalization(_tls: VMWorkerThread) {}

    fn out_of_memory(_tls: VMThread, _err_kind: AllocationError) {
        println!("Out of Memory!");
        unsafe { ((*UPCALLS).jl_throw_out_of_memory_error)() };
    }

    fn vm_live_bytes() -> usize {
        crate::api::JULIA_MALLOC_BYTES.load(Ordering::SeqCst)
    }

    fn is_collection_enabled() -> bool {
        unsafe { AtomicU32::load(&jl_gc_disable_counter, Ordering::SeqCst) <= 0 }
    }

    fn create_gc_trigger() -> Box<dyn GCTriggerPolicy<JuliaVM>> {
        use crate::gc_trigger::*;
        use std::convert::TryInto;
        let total_mem = unsafe { ((*UPCALLS).mmtk_get_total_memory)() }
            .try_into()
            .unwrap();
        let constrained_mem = unsafe { ((*UPCALLS).mmtk_get_constrained_memory)() }
            .try_into()
            .unwrap();
        let size_hint = unsafe { ((*UPCALLS).mmtk_get_heap_size_hint)() }
            .try_into()
            .unwrap();
        Box::new(JuliaGCTrigger::new(total_mem, constrained_mem, size_hint))
    }
}

pub fn is_current_gc_nursery() -> bool {
    match crate::SINGLETON.get_plan().generational() {
        Some(gen) => gen.is_current_gc_nursery(),
        None => false,
    }
}

#[no_mangle]
pub extern "C" fn mmtk_block_thread_for_gc(gc_n_threads: u16) {
    AtomicBool::store(&BLOCK_FOR_GC, true, Ordering::SeqCst);

    let &(ref lock, ref cvar) = &*STW_COND.clone();
    let mut count = lock.lock().unwrap();

    info!("Blocking for GC!");

    debug_assert!(
        gc_n_threads as usize == crate::active_plan::VMActivePlan::number_of_mutators(),
        "gc_nthreads = {} != number_of_mutators = {}",
        gc_n_threads,
        crate::active_plan::VMActivePlan::number_of_mutators()
    );

    AtomicBool::store(&WORLD_HAS_STOPPED, true, Ordering::SeqCst);

    while AtomicBool::load(&BLOCK_FOR_GC, Ordering::SeqCst) {
        count = cvar.wait(count).unwrap();
    }
}
