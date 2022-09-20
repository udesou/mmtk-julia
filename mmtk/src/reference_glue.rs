use mmtk::vm::ReferenceGlue;
use mmtk::util::{ObjectReference, Address};
use mmtk::util::opaque_pointer::*;
use mmtk::vm::Finalizable;
use mmtk::scheduler::ProcessEdgesWork;
use crate::api::mmtk_pin_object;
use crate::julia_types::*;
use crate::JuliaVM;

extern "C" {
  static jl_nothing: *mut mmtk_jl_value_t;
}

#[derive(Copy, Clone, Eq, Hash, PartialOrd, PartialEq, Debug)]
pub struct JuliaFinalizableObject (pub ObjectReference, pub Address, pub bool);

impl Finalizable for JuliaFinalizableObject {
  #[inline(always)]
  fn get_reference(&self) -> ObjectReference {
    self.0
  }
  #[inline(always)]
  fn set_reference(&mut self, object: ObjectReference) {
    self.0 = object;
  }
  fn keep_alive<E: ProcessEdgesWork>(&mut self, trace: &mut E) {
    self.set_reference(trace.trace_object(self.get_reference()));
    if crate::scanning::object_is_managed_by_mmtk(self.1.as_usize()) {
      mmtk_pin_object(unsafe {self.1.to_object_reference()});
      trace.trace_object(unsafe {self.1.to_object_reference()});
    }
  }
}

pub struct VMReferenceGlue {}

impl ReferenceGlue<JuliaVM> for VMReferenceGlue {
    type FinalizableType = JuliaFinalizableObject;
    fn set_referent(reference: ObjectReference, referent: ObjectReference) {
      unsafe {
        let mut reff = reference.to_address().to_mut_ptr::<mmtk_jl_weakref_t>();
        let referent_raw = referent.to_address().to_mut_ptr::<mmtk_jl_value_t>();
        (*reff).value = referent_raw;
      }
    }

    fn clear_referent(new_reference: ObjectReference) {
      Self::set_referent(new_reference, unsafe {
        Address::from_mut_ptr(jl_nothing).to_object_reference()
      });
    }

    fn get_referent(object: ObjectReference) -> ObjectReference {
      let referent;
      unsafe {
        let reff = object.to_address().to_mut_ptr::<mmtk_jl_weakref_t>();
        referent = Address::from_mut_ptr((*reff).value).to_object_reference();
      }
      referent
    }

    fn enqueue_references(_references: &[ObjectReference], _tls: VMWorkerThread) {
    }
}