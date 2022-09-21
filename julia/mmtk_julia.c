#include "mmtk_julia.h"
#include "mmtk.h"
#include <stdbool.h>
#include <stddef.h>
#include "gc.h"

extern int64_t perm_scanned_bytes;
extern void run_finalizer(jl_task_t *ct, jl_value_t *o, jl_value_t *ff);
extern JL_DLLIMPORT int jl_n_threads;
extern void *sysimg_base;
extern void *sysimg_end;
extern JL_DLLEXPORT void *jl_get_ptls_states(void);
extern jl_ptls_t get_next_mutator_tls();
extern jl_value_t *cmpswap_names JL_GLOBALLY_ROOTED;
extern jl_array_t *jl_global_roots_table JL_GLOBALLY_ROOTED;
extern jl_typename_t *jl_array_typename JL_GLOBALLY_ROOTED;
extern void jl_gc_premark(jl_ptls_t ptls2);
extern uint64_t finalizer_rngState[4];

// from julia.h

extern JL_DLLIMPORT jl_datatype_t *jl_typeofbottom_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_datatype_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_uniontype_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_unionall_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_tvar_type JL_GLOBALLY_ROOTED;

extern JL_DLLIMPORT jl_datatype_t *jl_any_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_unionall_t *jl_type_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_typename_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_typename_t *jl_type_typename JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_symbol_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_ssavalue_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_abstractslot_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_slotnumber_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_typedslot_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_argument_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_const_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_partial_struct_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_partial_opaque_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_interconditional_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_method_match_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_simplevector_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_typename_t *jl_tuple_typename JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_typename_t *jl_vecelement_typename JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_anytuple_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_emptytuple_type JL_GLOBALLY_ROOTED;
#define jl_tuple_type jl_anytuple_type
extern JL_DLLIMPORT jl_unionall_t *jl_anytuple_type_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_vararg_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_function_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_builtin_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_unionall_t *jl_opaque_closure_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_typename_t *jl_opaque_closure_typename JL_GLOBALLY_ROOTED;

extern JL_DLLIMPORT jl_value_t *jl_bottom_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_method_instance_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_code_instance_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_code_info_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_method_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_module_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_unionall_t *jl_abstractarray_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_unionall_t *jl_densearray_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_unionall_t *jl_array_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_typename_t *jl_array_typename JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_weakref_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_abstractstring_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_string_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_errorexception_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_argumenterror_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_loaderror_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_initerror_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_typeerror_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_methoderror_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_undefvarerror_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_atomicerror_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_lineinfonode_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_value_t *jl_stackovf_exception JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_value_t *jl_memory_exception JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_value_t *jl_readonlymemory_exception JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_value_t *jl_diverror_exception JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_value_t *jl_undefref_exception JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_value_t *jl_interrupt_exception JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_boundserror_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_value_t *jl_an_empty_vec_any JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_value_t *jl_an_empty_string JL_GLOBALLY_ROOTED;

extern JL_DLLIMPORT jl_datatype_t *jl_bool_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_char_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_int8_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_uint8_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_int16_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_uint16_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_int32_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_uint32_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_int64_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_uint64_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_float16_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_float32_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_float64_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_floatingpoint_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_number_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_void_type JL_GLOBALLY_ROOTED;  // deprecated
extern JL_DLLIMPORT jl_datatype_t *jl_nothing_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_signed_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_voidpointer_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_uint8pointer_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_unionall_t *jl_pointer_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_unionall_t *jl_llvmpointer_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_unionall_t *jl_ref_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_typename_t *jl_pointer_typename JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_typename_t *jl_llvmpointer_typename JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_typename_t *jl_namedtuple_typename JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_unionall_t *jl_namedtuple_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_task_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_value_t *jl_pair_type JL_GLOBALLY_ROOTED;

extern JL_DLLIMPORT jl_value_t *jl_array_uint8_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_value_t *jl_array_any_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_value_t *jl_array_symbol_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_value_t *jl_array_int32_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_value_t *jl_array_uint64_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_expr_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_globalref_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_linenumbernode_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_gotonode_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_gotoifnot_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_returnnode_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_phinode_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_pinode_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_phicnode_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_upsilonnode_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_quotenode_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_newvarnode_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_intrinsic_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_methtable_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_typemap_level_type JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_datatype_t *jl_typemap_entry_type JL_GLOBALLY_ROOTED;

extern JL_DLLIMPORT jl_svec_t *jl_emptysvec JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_value_t *jl_emptytuple JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_value_t *jl_true JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_value_t *jl_false JL_GLOBALLY_ROOTED;
extern JL_DLLIMPORT jl_value_t *jl_nothing JL_GLOBALLY_ROOTED;

JL_DLLEXPORT void import_global_roots() {
    adding_mmtk_object_as_root(jl_typeofbottom_type);
    adding_mmtk_object_as_root(jl_datatype_type);
    adding_mmtk_object_as_root(jl_uniontype_type);
    adding_mmtk_object_as_root(jl_unionall_type);
    adding_mmtk_object_as_root(jl_tvar_type);
    adding_mmtk_object_as_root(jl_any_type);
    adding_mmtk_object_as_root(jl_type_type);
    adding_mmtk_object_as_root(jl_typename_type);
    adding_mmtk_object_as_root(jl_type_typename);
    adding_mmtk_object_as_root(jl_symbol_type);
    adding_mmtk_object_as_root(jl_ssavalue_type);
    adding_mmtk_object_as_root(jl_abstractslot_type);
    adding_mmtk_object_as_root(jl_slotnumber_type);
    adding_mmtk_object_as_root(jl_typedslot_type);
    adding_mmtk_object_as_root(jl_argument_type);
    adding_mmtk_object_as_root(jl_const_type);
    adding_mmtk_object_as_root(jl_partial_struct_type);
    adding_mmtk_object_as_root(jl_partial_opaque_type);
    adding_mmtk_object_as_root(jl_interconditional_type);
    adding_mmtk_object_as_root(jl_method_match_type);
    adding_mmtk_object_as_root(jl_simplevector_type);
    adding_mmtk_object_as_root(jl_tuple_typename);
    adding_mmtk_object_as_root(jl_vecelement_typename);
    adding_mmtk_object_as_root(jl_anytuple_type);
    adding_mmtk_object_as_root(jl_emptytuple_type);
    adding_mmtk_object_as_root(jl_anytuple_type_type);
    adding_mmtk_object_as_root(jl_vararg_type);
    adding_mmtk_object_as_root(jl_function_type);
    adding_mmtk_object_as_root(jl_builtin_type);
    adding_mmtk_object_as_root(jl_opaque_closure_type);
    adding_mmtk_object_as_root(jl_opaque_closure_typename);
    adding_mmtk_object_as_root(jl_bottom_type);
    adding_mmtk_object_as_root(jl_method_instance_type);
    adding_mmtk_object_as_root(jl_code_instance_type);
    adding_mmtk_object_as_root(jl_code_info_type);
    adding_mmtk_object_as_root(jl_method_type);
    adding_mmtk_object_as_root(jl_module_type);
    adding_mmtk_object_as_root(jl_abstractarray_type);
    adding_mmtk_object_as_root(jl_densearray_type);
    adding_mmtk_object_as_root(jl_array_type);
    adding_mmtk_object_as_root(jl_array_typename);
    adding_mmtk_object_as_root(jl_weakref_type);
    adding_mmtk_object_as_root(jl_abstractstring_type);
    adding_mmtk_object_as_root(jl_string_type);
    adding_mmtk_object_as_root(jl_errorexception_type);
    adding_mmtk_object_as_root(jl_argumenterror_type);
    adding_mmtk_object_as_root(jl_loaderror_type);
    adding_mmtk_object_as_root(jl_initerror_type);
    adding_mmtk_object_as_root(jl_typeerror_type);
    adding_mmtk_object_as_root(jl_methoderror_type);
    adding_mmtk_object_as_root(jl_undefvarerror_type);
    adding_mmtk_object_as_root(jl_atomicerror_type);
    adding_mmtk_object_as_root(jl_lineinfonode_type);
    adding_mmtk_object_as_root(jl_stackovf_exception);
    adding_mmtk_object_as_root(jl_memory_exception);
    adding_mmtk_object_as_root(jl_readonlymemory_exception);
    adding_mmtk_object_as_root(jl_diverror_exception);
    adding_mmtk_object_as_root(jl_undefref_exception);
    adding_mmtk_object_as_root(jl_interrupt_exception);
    adding_mmtk_object_as_root(jl_boundserror_type);
    adding_mmtk_object_as_root(jl_an_empty_vec_any);
    adding_mmtk_object_as_root(jl_an_empty_string);
    adding_mmtk_object_as_root(jl_bool_type);
    adding_mmtk_object_as_root(jl_char_type);
    adding_mmtk_object_as_root(jl_int8_type);
    adding_mmtk_object_as_root(jl_uint8_type);
    adding_mmtk_object_as_root(jl_int16_type);
    adding_mmtk_object_as_root(jl_uint16_type);
    adding_mmtk_object_as_root(jl_int32_type);
    adding_mmtk_object_as_root(jl_uint32_type);
    adding_mmtk_object_as_root(jl_int64_type);
    adding_mmtk_object_as_root(jl_uint64_type);
    adding_mmtk_object_as_root(jl_float16_type);
    adding_mmtk_object_as_root(jl_float32_type);
    adding_mmtk_object_as_root(jl_float64_type);
    adding_mmtk_object_as_root(jl_floatingpoint_type);
    adding_mmtk_object_as_root(jl_number_type);
    adding_mmtk_object_as_root(jl_void_type);
    adding_mmtk_object_as_root(jl_nothing_type);
    adding_mmtk_object_as_root(jl_signed_type);
    adding_mmtk_object_as_root(jl_voidpointer_type);
    adding_mmtk_object_as_root(jl_uint8pointer_type);
    adding_mmtk_object_as_root(jl_pointer_type);
    adding_mmtk_object_as_root(jl_llvmpointer_type);
    adding_mmtk_object_as_root(jl_ref_type);
    adding_mmtk_object_as_root(jl_pointer_typename);
    adding_mmtk_object_as_root(jl_llvmpointer_typename);
    adding_mmtk_object_as_root(jl_namedtuple_typename);
    adding_mmtk_object_as_root(jl_namedtuple_type);
    adding_mmtk_object_as_root(jl_task_type);
    adding_mmtk_object_as_root(jl_pair_type);
    adding_mmtk_object_as_root(jl_array_uint8_type);
    adding_mmtk_object_as_root(jl_array_any_type);
    adding_mmtk_object_as_root(jl_array_symbol_type);
    adding_mmtk_object_as_root(jl_array_int32_type);
    adding_mmtk_object_as_root(jl_array_uint64_type);
    adding_mmtk_object_as_root(jl_expr_type);
    adding_mmtk_object_as_root(jl_globalref_type);
    adding_mmtk_object_as_root(jl_linenumbernode_type);
    adding_mmtk_object_as_root(jl_gotonode_type);
    adding_mmtk_object_as_root(jl_gotoifnot_type);
    adding_mmtk_object_as_root(jl_returnnode_type);
    adding_mmtk_object_as_root(jl_phinode_type);
    adding_mmtk_object_as_root(jl_pinode_type);
    adding_mmtk_object_as_root(jl_phicnode_type);
    adding_mmtk_object_as_root(jl_upsilonnode_type);
    adding_mmtk_object_as_root(jl_quotenode_type);
    adding_mmtk_object_as_root(jl_newvarnode_type);
    adding_mmtk_object_as_root(jl_intrinsic_type);
    adding_mmtk_object_as_root(jl_methtable_type);
    adding_mmtk_object_as_root(jl_typemap_level_type);
    adding_mmtk_object_as_root(jl_typemap_entry_type);
    adding_mmtk_object_as_root(jl_emptysvec);
    adding_mmtk_object_as_root(jl_emptytuple);
    adding_mmtk_object_as_root(jl_true);
    adding_mmtk_object_as_root(jl_false);
    adding_mmtk_object_as_root(jl_nothing);
    adding_mmtk_object_as_root(jl_main_module);
    adding_mmtk_object_as_root(jl_core_module);
    adding_mmtk_object_as_root(jl_base_module);
    adding_mmtk_object_as_root(jl_top_module);
    adding_mmtk_object_as_root(jl_all_tls_states);
    adding_mmtk_object_as_root(cmpswap_names);
    adding_mmtk_object_as_root(jl_type_type_mt);
    adding_mmtk_object_as_root(jl_nonfunction_mt);
    adding_mmtk_object_as_root(_jl_debug_method_invalidation);
    adding_mmtk_object_as_root(jl_all_methods);
    adding_mmtk_object_as_root(jl_module_init_order);
    adding_mmtk_object_as_root(jl_precompile_toplevel_module);
    adding_mmtk_object_as_root(jl_global_roots_table);
}

void adding_mmtk_object_as_root(void* obj) {
    if (object_is_managed_by_mmtk(obj)) {
        add_object_to_mmtk_roots(obj);
    }
}


JL_DLLEXPORT void (jl_mmtk_harness_begin)(void)
{
    jl_ptls_t ptls = jl_get_ptls_states();
    harness_begin(ptls);
}

JL_DLLEXPORT void (jl_mmtk_harness_end)(void)
{
    harness_end();
}

JL_DLLEXPORT jl_value_t *jl_mmtk_gc_alloc_default_llvm(int pool_offset, int osize)
{
    // safepoint
    if (__unlikely(jl_atomic_load(&jl_gc_running))) {
        jl_ptls_t ptls = jl_get_ptls_states();
        int8_t old_state = ptls->gc_state;
        jl_atomic_store_release(&ptls->gc_state, JL_GC_STATE_WAITING);
        jl_safepoint_wait_gc();
        jl_atomic_store_release(&ptls->gc_state, old_state);
    }

    jl_value_t *v;
    jl_ptls_t ptls = jl_get_ptls_states();

    ptls->mmtk_mutator_ptr->allocators.immix[0].cursor = ptls->cursor;

    // v needs to be 16 byte aligned, therefore v_tagged needs to be offset accordingly to consider the size of header
    jl_taggedvalue_t *v_tagged =
        alloc(ptls->mmtk_mutator_ptr, osize, 16, 8, 0);

    ptls->cursor = ptls->mmtk_mutator_ptr->allocators.immix[0].cursor;
    ptls->limit = ptls->mmtk_mutator_ptr->allocators.immix[0].limit;

    v = jl_valueof(v_tagged);

    post_alloc(ptls->mmtk_mutator_ptr, v, osize, 0);
    ptls->gc_num.allocd += osize;
    ptls->gc_num.poolalloc++;

    return v;
}

STATIC_INLINE void* alloc_default_object(jl_ptls_t ptls, size_t size, int offset) {
    int64_t delta = (-offset -(int64_t)(ptls->cursor)) & 15; // aligned to 16
    uint64_t aligned_addr = ptls->cursor + delta;

    if(__unlikely(aligned_addr+size > ptls->limit)) {
        jl_ptls_t ptls2 = jl_get_ptls_states();
        ptls2->mmtk_mutator_ptr->allocators.immix[0].cursor = ptls2->cursor;
        void* res = alloc(ptls2->mmtk_mutator_ptr, size, 16, offset, 0);
        ptls2->cursor = ptls2->mmtk_mutator_ptr->allocators.immix[0].cursor;
        ptls2->limit = ptls2->mmtk_mutator_ptr->allocators.immix[0].limit;
        return res;
    } else {
        ptls->cursor = aligned_addr+size;
        return aligned_addr;
    }
}

JL_DLLEXPORT jl_value_t *jl_mmtk_gc_alloc_default(jl_ptls_t ptls, int pool_offset,
                                                    int osize, void *ty)
{
    // safepoint
    if (__unlikely(jl_atomic_load(&jl_gc_running))) {
        jl_ptls_t ptls = jl_get_ptls_states();
        int8_t old_state = ptls->gc_state;
        jl_atomic_store_release(&ptls->gc_state, JL_GC_STATE_WAITING);
        jl_safepoint_wait_gc();
        jl_atomic_store_release(&ptls->gc_state, old_state);
    }

    jl_value_t *v;
    if (ty != jl_buff_tag) {
        // v needs to be 16 byte aligned, therefore v_tagged needs to be offset accordingly to consider the size of header
        jl_taggedvalue_t *v_tagged = alloc_default_object(ptls, osize, sizeof(jl_taggedvalue_t));
        v = jl_valueof(v_tagged);
        post_alloc(ptls->mmtk_mutator_ptr, v, osize, 0);
    } else {
        // allocating an extra word to store the size of buffer objects
        jl_taggedvalue_t *v_tagged = alloc_default_object(ptls, osize + sizeof(jl_taggedvalue_t), 0);
        jl_value_t* v_tagged_aligned = ((jl_value_t*)((char*)(v_tagged) + sizeof(jl_taggedvalue_t)));
        v = jl_valueof(v_tagged_aligned);
        store_obj_size_c(v, osize + sizeof(jl_taggedvalue_t));
        post_alloc(ptls->mmtk_mutator_ptr, v, osize + sizeof(jl_taggedvalue_t), 0);
    }
    
    ptls->gc_num.allocd += osize;
    ptls->gc_num.poolalloc++;

    return v;
}

JL_DLLEXPORT jl_value_t *jl_mmtk_gc_alloc_big(jl_ptls_t ptls, size_t sz)
{
    // safepoint
    if (__unlikely(jl_atomic_load(&jl_gc_running))) {
        jl_ptls_t ptls = jl_get_ptls_states();
        int8_t old_state = ptls->gc_state;
        jl_atomic_store_release(&ptls->gc_state, JL_GC_STATE_WAITING);
        jl_safepoint_wait_gc();
        jl_atomic_store_release(&ptls->gc_state, old_state);
    }

    size_t offs = offsetof(bigval_t, header);
    assert(sz >= sizeof(jl_taggedvalue_t) && "sz must include tag");
    static_assert(offsetof(bigval_t, header) >= sizeof(void*), "Empty bigval header?");
    static_assert(sizeof(bigval_t) % JL_HEAP_ALIGNMENT == 0, "");
    size_t allocsz = LLT_ALIGN(sz + offs, JL_CACHE_BYTE_ALIGNMENT);
    if (allocsz < sz) { // overflow in adding offs, size was "negative"
        assert(0 && "Error when allocating big object");
        jl_throw(jl_memory_exception);
    }

    bigval_t *v = (bigval_t*)alloc_large(ptls->mmtk_mutator_ptr, allocsz, JL_CACHE_BYTE_ALIGNMENT, 0, 2);

    if (v == NULL) {
        assert(0 && "Allocation failed");
        jl_throw(jl_memory_exception);
    }
    v->sz = allocsz;

    ptls->gc_num.allocd += allocsz;
    ptls->gc_num.bigalloc++;

    jl_value_t *result = jl_valueof(&v->header);
    post_alloc(ptls->mmtk_mutator_ptr, result, allocsz, 2);

    return result;
}

static void mmtk_sweep_malloced_arrays(void) JL_NOTSAFEPOINT
{
    gc_time_mallocd_array_start();
    reset_count_tls();
    jl_ptls_t ptls2 = (jl_ptls_t) get_next_mutator_tls();
    while(ptls2 != NULL) {
        mallocarray_t *ma = ptls2->heap.mallocarrays;
        mallocarray_t **pma = &ptls2->heap.mallocarrays;
        while (ma != NULL) {
            mallocarray_t *nxt = ma->next;
            if (!object_is_managed_by_mmtk(ma->a)) {
                pma = &ma->next;
                ma = nxt;
                continue;
            }
            if (is_live_object(ma->a)) {
                pma = &ma->next;
            }
            else {
                *pma = nxt;
                assert(ma->a->flags.how == 2);
                jl_gc_free_array(ma->a);
                ma->next = ptls2->heap.mafreelist;
                ptls2->heap.mafreelist = ma;
            }
            ma = nxt;
        }
        ptls2 = get_next_mutator_tls();
    }
    gc_time_mallocd_array_end();
}

extern void mark_metadata_scanned(jl_value_t* obj);
extern int check_metadata_scanned(jl_value_t* obj);

int object_has_been_scanned(jl_value_t* obj) 
{
    jl_taggedvalue_t *o = jl_astaggedvalue(obj);
    uintptr_t tag = (jl_value_t*)jl_typeof(obj);
    jl_datatype_t *vt = (jl_datatype_t*)tag;

    if (vt == jl_symbol_type) {
        return 1;
    };

    if (sysimg_base == NULL) {
        return 0;
    }

    if ((void*)obj < sysimg_base || (void*)obj >= sysimg_end) {
        return 0;
    }

    return check_metadata_scanned(obj);
}

void mark_object_as_scanned(jl_value_t* obj) {
    if (sysimg_base == NULL) {
        return 0;
    }

    if ((void*)obj < sysimg_base || (void*)obj >= sysimg_end) {
        return 0;
    }

    mark_metadata_scanned(obj);
}

int mmtk_wait_in_a_safepoint() {
    jl_ptls_t ptls = jl_get_ptls_states();
    int8_t old_state = ptls->gc_state;
    jl_atomic_store(&ptls->gc_state, JL_GC_STATE_WAITING);

    return old_state;
}

void mmtk_exit_from_safepoint(int old_state) {
    jl_ptls_t ptls = jl_get_ptls_states();
    jl_gc_state_set(ptls, old_state, JL_GC_STATE_WAITING);
}

// all threads pass here and if there is another thread doing GC,
// it will block until GC is done
// that thread simply exits from block_for_gc without executing finalizers
// when executing finalizers do not let another thread do GC (set a variable such that while that variable is true, no GC can be done)
int set_gc_initial_state(jl_ptls_t ptls) 
{
    int8_t old_state = ptls->gc_state;
    jl_atomic_store(&ptls->gc_state, JL_GC_STATE_WAITING);
    if (!jl_safepoint_start_gc()) {
        jl_atomic_store(&ptls->gc_state, old_state);
        return -1;
    }
    return old_state;
}

void set_gc_final_state(int old_state) 
{
    jl_ptls_t ptls = jl_get_ptls_states();

    jl_safepoint_end_gc();
    jl_gc_state_set(ptls, old_state, JL_GC_STATE_WAITING);
}

int set_gc_old_state(int old_state) 
{
    jl_ptls_t ptls = jl_get_ptls_states();
    jl_atomic_store_release(&ptls->gc_state, old_state);
}

void wait_for_the_world(void) 
{
    if (jl_n_threads > 1)
        jl_wake_libuv();
    for (int i = 0; i < jl_n_threads; i++) {
        jl_ptls_t ptls2 = jl_all_tls_states[i];
        // This acquire load pairs with the release stores
        // in the signal handler of safepoint so we are sure that
        // all the stores on those threads are visible.
        // We're currently also using atomic store release in mutator threads
        // (in jl_gc_state_set), but we may want to use signals to flush the
        // memory operations on those threads lazily instead.
        while (!jl_atomic_load_relaxed(&ptls2->gc_state) || !jl_atomic_load_acquire(&ptls2->gc_state))
            jl_cpu_pause(); // yield?
    }
}

size_t get_lo_size(bigval_t obj) 
{
    return obj.sz;
}

void set_jl_last_err(int e) 
{
    jl_ptls_t ptls = jl_get_ptls_states();
    errno = e;
}

int get_jl_last_err() 
{
    for (int t_i = 0; t_i < jl_n_threads; t_i++) {
        jl_ptls_t ptls = jl_all_tls_states[t_i];
        ptls->cursor = 0;
        ptls->limit = 0;
    }
    return errno;
}

void* get_obj_start_ref(jl_value_t* obj) 
{
    uintptr_t tag = (jl_value_t*)jl_typeof(obj);
    jl_datatype_t *vt = (jl_datatype_t*)tag;
    void* obj_start_ref; 

    if (vt == jl_buff_tag) {
        obj_start_ref = (void*)((size_t)obj - 2*sizeof(jl_taggedvalue_t));
    } else {
        obj_start_ref = (void*)((size_t)obj - sizeof(jl_taggedvalue_t));
    }

    return obj_start_ref;
}

size_t get_so_size(jl_value_t* obj) 
{
    uintptr_t tag = (jl_value_t*)jl_typeof(obj);
    jl_datatype_t *vt = (jl_datatype_t*)tag;

    if (vt == jl_buff_tag) {
        return get_obj_size(obj);
    } else if (vt->name == jl_array_typename) {
        jl_array_t* a = (jl_array_t*) obj;
        if (a->flags.how == 0) {
            int ndimwords = jl_array_ndimwords(jl_array_ndims(a));
            int tsz = sizeof(jl_array_t) + ndimwords*sizeof(size_t);
            if (object_is_managed_by_mmtk(a->data)) {
                size_t pre_data_bytes = ((size_t)a->data - a->offset*a->elsize) - (size_t)a;
                if (pre_data_bytes > 0 && pre_data_bytes <= ARRAY_INLINE_NBYTES) {
                    tsz = ((size_t)a->data - a->offset*a->elsize) - (size_t)a;
                    tsz += jl_array_nbytes(a);
                }
            }
            int pool_id = jl_gc_szclass(tsz + sizeof(jl_taggedvalue_t));
            int osize = jl_gc_sizeclasses[pool_id];
            return osize;
        } else if (a->flags.how == 1) {
            int ndimwords = jl_array_ndimwords(jl_array_ndims(a));
            int tsz = sizeof(jl_array_t) + ndimwords*sizeof(size_t);
            int pool_id = jl_gc_szclass(tsz + sizeof(jl_taggedvalue_t));
            int osize = jl_gc_sizeclasses[pool_id];

            return osize;
        } else if (a->flags.how == 2) {
            int ndimwords = jl_array_ndimwords(jl_array_ndims(a));
            int tsz = sizeof(jl_array_t) + ndimwords*sizeof(size_t);
            int pool_id = jl_gc_szclass(tsz + sizeof(jl_taggedvalue_t));
            int osize = jl_gc_sizeclasses[pool_id];

            return osize;
        } else if (a->flags.how == 3) {
            int ndimwords = jl_array_ndimwords(jl_array_ndims(a));
            int tsz = sizeof(jl_array_t) + ndimwords * sizeof(size_t) + sizeof(void*);
            int pool_id = jl_gc_szclass(tsz + sizeof(jl_taggedvalue_t));
            int osize = jl_gc_sizeclasses[pool_id];
            return osize;
        }
    } else if (vt == jl_simplevector_type) {
        size_t l = jl_svec_len(obj);
        int pool_id = jl_gc_szclass(l * sizeof(void*) + sizeof(jl_svec_t) + sizeof(jl_taggedvalue_t));
        int osize = jl_gc_sizeclasses[pool_id];
        return osize;
    } else if (vt == jl_module_type) {
        size_t dtsz = sizeof(jl_module_t);
        int pool_id = jl_gc_szclass(dtsz + sizeof(jl_taggedvalue_t));
        int osize = jl_gc_sizeclasses[pool_id];
        return osize;
    } else if (vt == jl_task_type) {
        size_t dtsz = sizeof(jl_task_t);
        int pool_id = jl_gc_szclass(dtsz + sizeof(jl_taggedvalue_t));
        int osize = jl_gc_sizeclasses[pool_id];
        return osize;
    } else if (vt == jl_string_type) {
        size_t dtsz = jl_string_len(obj) + sizeof(size_t) + 1;
        int pool_id = jl_gc_szclass_align8(dtsz + sizeof(jl_taggedvalue_t));
        int osize = jl_gc_sizeclasses[pool_id];
        return osize;
    } if (vt == jl_method_type) {
        size_t dtsz = sizeof(jl_method_t);
        int pool_id = jl_gc_szclass(dtsz + sizeof(jl_taggedvalue_t));
        int osize = jl_gc_sizeclasses[pool_id];
        return osize;
    } else  {
        size_t dtsz = jl_datatype_size(vt);
        int pool_id = jl_gc_szclass(dtsz + sizeof(jl_taggedvalue_t));
        int osize = jl_gc_sizeclasses[pool_id];
        return osize;
    }
}

static void run_finalizer_function(jl_value_t *o, jl_value_t *ff, bool is_ptr)
{
    jl_ptls_t ptls = jl_current_task->ptls;
    if (is_ptr) {
        run_finalizer(jl_current_task, (void*)(((uintptr_t)o) | 1), ff);
    } else {
        run_finalizer(jl_current_task, o, ff);
    }
}

static inline void mmtk_jl_run_finalizers_in_list(bool at_exit) {
    jl_task_t *ct = jl_current_task;
    uint8_t sticky = ct->sticky;
    mmtk_run_finalizers(at_exit);
    ct->sticky = sticky;
}

void mmtk_jl_run_finalizers(jl_ptls_t ptls) {
    // Only disable finalizers on current thread
    // Doing this on all threads is racy (it's impossible to check
    // or wait for finalizers on other threads without dead lock).
    if (!ptls->finalizers_inhibited && ptls->locks.len == 0) {
        jl_task_t *ct = jl_current_task;
        int8_t was_in_finalizer = ptls->in_finalizer;
        ptls->in_finalizer = 1;
        uint64_t save_rngState[4];
        memcpy(&save_rngState[0], &ct->rngState[0], sizeof(save_rngState));
        jl_rng_split(ct->rngState, finalizer_rngState);
        mmtk_jl_run_finalizers_in_list(false);
        memcpy(&ct->rngState[0], &save_rngState[0], sizeof(save_rngState));
        ptls->in_finalizer = was_in_finalizer;
    } else {
        jl_atomic_store_relaxed(&jl_gc_have_pending_finalizers, 1);
    }
}

void mmtk_jl_gc_run_all_finalizers() {
    mmtk_jl_run_finalizers_in_list(true);
}

// add the initial root set to mmtk roots
static void queue_roots(jl_gc_mark_cache_t *gc_cache, jl_gc_mark_sp_t *sp)
{
    // modules
    add_object_to_mmtk_roots(jl_main_module);

    import_global_roots();

    // invisible builtin values
    if (jl_an_empty_vec_any != NULL)
        add_object_to_mmtk_roots(jl_an_empty_vec_any);
    if (jl_module_init_order != NULL)
        add_object_to_mmtk_roots(jl_module_init_order);
    for (size_t i = 0; i < jl_current_modules.size; i += 2) {
        if (jl_current_modules.table[i + 1] != HT_NOTFOUND) {
            add_object_to_mmtk_roots(jl_current_modules.table[i]);
        }
    }
    add_object_to_mmtk_roots(jl_anytuple_type_type);
    for (size_t i = 0; i < N_CALL_CACHE; i++) {
         jl_typemap_entry_t *v = jl_atomic_load_relaxed(&call_cache[i]);
         if (v != NULL)
             add_object_to_mmtk_roots(v);
    }
    if (jl_all_methods != NULL)
        add_object_to_mmtk_roots(jl_all_methods);

    if (_jl_debug_method_invalidation != NULL)
        add_object_to_mmtk_roots(_jl_debug_method_invalidation);

    // constants
    add_object_to_mmtk_roots(jl_emptytuple_type);
    if (cmpswap_names != NULL)
        add_object_to_mmtk_roots(cmpswap_names);
    add_object_to_mmtk_roots(jl_global_roots_table);

}

static void jl_gc_queue_bt_buf_mmtk(jl_gc_mark_cache_t *gc_cache, jl_gc_mark_sp_t *sp, jl_ptls_t ptls2)
{
    jl_bt_element_t *bt_data = ptls2->bt_data;
    jl_value_t* bt_entry_value;
    size_t bt_size = ptls2->bt_size;
    for (size_t i = 0; i < bt_size; i += jl_bt_entry_size(bt_data + i)) {
        jl_bt_element_t *bt_entry = bt_data + i;
        if (jl_bt_is_native(bt_entry))
            continue;
        size_t njlvals = jl_bt_num_jlvals(bt_entry);
        for (size_t j = 0; j < njlvals; j++) {
            bt_entry_value = jl_bt_entry_jlvalue(bt_entry, j);
            add_object_to_mmtk_roots(bt_entry_value);
        }
    }
}

static void jl_gc_queue_thread_local_mmtk(jl_gc_mark_cache_t *gc_cache, jl_gc_mark_sp_t *sp, jl_ptls_t ptls2)
{
    mallocarray_t *ma = ptls2->heap.mallocarrays;
    mallocarray_t **pma = &ptls2->heap.mallocarrays;
    while (ma != NULL) {
        mallocarray_t *nxt = ma->next;
        if (!object_is_managed_by_mmtk(ma->a)) {
            pma = &ma->next;
            ma = nxt;
            continue;
        } else { 
            *pma = nxt;
            mmtk_pin_object(ma->a);
        }
        ma = nxt;
    }

    add_object_to_mmtk_task_roots(ptls2->current_task);
    add_object_to_mmtk_roots(ptls2->current_task);
    add_object_to_mmtk_task_roots(ptls2->root_task);
    add_object_to_mmtk_roots(ptls2->current_task);
    if (ptls2->next_task) {
        add_object_to_mmtk_task_roots(ptls2->next_task);
        add_object_to_mmtk_roots(ptls2->next_task);
    }
    if (ptls2->previous_task) {
        add_object_to_mmtk_task_roots(ptls2->previous_task);
        add_object_to_mmtk_roots(ptls2->previous_task);
    }
    if (ptls2->previous_exception) {
        add_object_to_mmtk_task_roots(ptls2->previous_exception);
        add_object_to_mmtk_roots(ptls2->previous_exception);
    }
}

static void jl_gc_queue_remset_mmtk(jl_gc_mark_cache_t *gc_cache, jl_gc_mark_sp_t *sp, jl_ptls_t ptls2)
{
    size_t len = ptls2->heap.last_remset->len;
    void **items = ptls2->heap.last_remset->items;
    for (size_t i = 0; i < len; i++) {
        add_object_to_mmtk_roots(items[i]);
    }
    int n_bnd_refyoung = 0;
    len = ptls2->heap.rem_bindings.len;
    items = ptls2->heap.rem_bindings.items;

    for (size_t i = 0; i < len; i++) {
        jl_binding_t *ptr = (jl_binding_t*)items[i];
        // A null pointer can happen here when the binding is cleaned up
        // as an exception is thrown after it was already queued (#10221)
        if (!ptr->value) continue;
        add_object_to_mmtk_roots(ptr->value);
    }
    ptls2->heap.rem_bindings.len = 0;
}

static int calculate_roots(jl_ptls_t ptls)
{
    jl_gc_mark_cache_t *gc_cache = &ptls->gc_cache;
    jl_gc_mark_sp_t sp;
    gc_mark_sp_init(gc_cache, &sp);

    uint64_t t0 = jl_hrtime();
    int64_t last_perm_scanned_bytes = perm_scanned_bytes;

    for (int t_i = 0; t_i < jl_n_threads; t_i++)
        jl_gc_premark(jl_all_tls_states[t_i]);

    for (int t_i = 0; t_i < jl_n_threads; t_i++) {
        jl_ptls_t ptls2 = jl_all_tls_states[t_i];
        // 2.1. add every object in the `last_remsets` and `rem_binding` to mmtk roots
        jl_gc_queue_remset_mmtk(gc_cache, &sp, ptls2);
        // 2.2. add every thread local root to mmtk roots
        jl_gc_queue_thread_local_mmtk(gc_cache, &sp, ptls2);
        // 2.3. add any managed objects in the backtrace buffer to mmtk roots
        jl_gc_queue_bt_buf_mmtk(gc_cache, &sp, ptls2);
    }

    queue_roots(gc_cache, &sp);

    return 1;
}

// Handle the case where the stack is only partially copied.
static inline uintptr_t mmtk_gc_get_stack_addr(void *_addr, uintptr_t offset,
                                          uintptr_t lb, uintptr_t ub)
{
    uintptr_t addr = (uintptr_t)_addr;
    if (addr >= lb && addr < ub)
        return addr + offset;
    return addr;
}

static inline uintptr_t mmtk_gc_read_stack(void *_addr, uintptr_t offset,
                                      uintptr_t lb, uintptr_t ub)
{
    uintptr_t real_addr = mmtk_gc_get_stack_addr(_addr, offset, lb, ub);
    return *(uintptr_t*)real_addr;
}

JL_DLLEXPORT void scan_julia_exc_obj(jl_value_t* obj, closure_pointer closure, ProcessEdgeFn process_edge) {
    uintptr_t tag = (jl_value_t*)jl_typeof(obj);
    jl_datatype_t *vt = (jl_datatype_t*)tag; // type of obj
    int len = 0;

    assert(vt == jl_task_type);
    jl_task_t *ta = (jl_task_t*)obj;

    const char *type_name = jl_typeof_str(obj);

    if (ta->excstack) { // inlining label `excstack` from mark_loop
        // if it is not managed by MMTk, nothing needs to be done because the object does not need to be scanned
        if (object_is_managed_by_mmtk(ta->excstack)) {
            process_edge(closure, &ta->excstack, obj, type_name, vt);
        }
        jl_excstack_t *excstack = ta->excstack;
        size_t itr = ta->excstack->top;
        size_t bt_index = 0;
        size_t jlval_index = 0;
        while (itr > 0) {
            size_t bt_size = jl_excstack_bt_size(excstack, itr);
            jl_bt_element_t *bt_data = jl_excstack_bt_data(excstack, itr);
            for (; bt_index < bt_size; bt_index += jl_bt_entry_size(bt_data + bt_index)) {
                jl_bt_element_t *bt_entry = bt_data + bt_index;
                if (jl_bt_is_native(bt_entry))
                    continue;
                // Found an extended backtrace entry: iterate over any
                // GC-managed values inside.
                size_t njlvals = jl_bt_num_jlvals(bt_entry);
                while (jlval_index < njlvals) {
                    uintptr_t nptr = 0;
                    jl_value_t** new_obj_edge = &bt_entry[2 + jlval_index].jlvalue;
                    jlval_index += 1;
                    process_edge(closure, new_obj_edge, obj, type_name, vt);
                }
                jlval_index = 0;
            }

            jl_bt_element_t *stack_raw = (jl_bt_element_t *)(excstack+1);
            jl_value_t** stack_obj_edge = &stack_raw[itr-1].jlvalue;

            jl_value_t* new_obj = jl_excstack_exception(excstack, itr);
            itr = jl_excstack_next(excstack, itr);
            bt_index = 0;
            jlval_index = 0;
            process_edge(closure, stack_obj_edge, obj, type_name, vt);
        }
    }
}

#define jl_array_data_owner_addr(a) (((jl_value_t**)((char*)a + jl_array_data_owner_offset(jl_array_ndims(a)))))

JL_DLLEXPORT void* get_stackbase(int16_t tid) {
    assert(tid >= 0);
    jl_ptls_t ptls2 = jl_all_tls_states[tid];
    return ptls2->stackbase;
}

/** 
 * Corresponds to the function mark_loop in the original Julia GC. It
 * dispatches MMTk work for scanning internal pointers for the object obj.
 * This function follows the flow defined in the `mark` goto label in mark_loop.
 * based on the type of obj it computes the internal pointers which are passed back to mmtk in 2 different ways:
 * (1) By adding an edge to vec_internals in order to create work packets. Note that this is a Rust vector limited 
 * by a `capacity`, once the vector is full, the work is dispatched through the function dispatch_work.
 * (2) By creating the work directly through the functions `trace_slot_with_offset`, `trace_obj` and `scan_obj`. The functions 
 * respectively: trace a buffer that contains an offset which needs to be added to the object after it is loaded; trace an object 
 * directly (not an edge), specifying whether to scan the object or not; and only scan the object 
 * (necessary for boot image / non-MMTk objects)
**/
JL_DLLEXPORT void scan_julia_obj(jl_value_t* obj, closure_pointer closure, ProcessEdgeFn process_edge, ProcessOffsetEdgeFn process_offset_edge, ProcessMaskedEdgeFn process_masked_edge) 
{
    uintptr_t tag = (jl_value_t*)jl_typeof(obj);
    jl_datatype_t *vt = (jl_datatype_t*)tag; // type of obj

    // if it is a symbol type it does not contain internal pointers 
    // but we need to dispatch the work to appropriately drop the rust vector
    if (vt == jl_symbol_type || vt == jl_buff_tag) {
        return;
    };

    int foreign_alloc = 0;
    int meta_updated = 0;
    int update_meta = __likely(!meta_updated && !gc_verifying);

    const char *type_name = jl_typeof_str(obj);

    if (vt == jl_simplevector_type) { // scanning a jl_simplevector_type object (inlining label `objarray_loaded` from mark_loop)
        size_t l = jl_svec_len(obj);
        jl_value_t **data = jl_svec_data(obj);
        size_t dtsz = l * sizeof(void*) + sizeof(jl_svec_t);
        jl_value_t **objary_begin = data;
        jl_value_t **objary_end = data + l;
        for (; objary_begin < objary_end; objary_begin += 1) {
            process_edge(closure, objary_begin, obj, type_name, vt);
        }
    } else if (vt->name == jl_array_typename) { // scanning a jl_array_typename object
        jl_array_t *a = (jl_array_t*)obj;
        jl_array_flags_t flags = a->flags;

        if (flags.how == 1) { // julia-allocated buffer that needs to be marked
            long offset = a->offset * a->elsize;
            process_offset_edge(closure, &a->data, offset);
        }
        if (flags.how == 2) { // malloc-allocated pointer this array object manages
            // should be processed below if it contains pointers
        } else if (flags.how == 3) { // has a pointer to the object that owns the data
            jl_value_t **owner_addr = jl_array_data_owner_addr(a);
            process_edge(closure, owner_addr, obj, type_name, vt);
            return;
        }
        if (a->data == NULL || jl_array_len(a) == 0) {
            return;
        }
        if (flags.ptrarray) { // inlining label `objarray_loaded` from mark_loop
            size_t l = jl_array_len(a);

            jl_value_t** objary_begin = (jl_value_t**)a->data;
            jl_value_t** objary_end = objary_begin + l;
            jl_value_t** pnew_obj;

            for (; objary_begin < objary_end; objary_begin++) {
                process_edge(closure, objary_begin, obj, type_name, vt);
            }
        } else if (flags.hasptr) { // inlining label `objarray_loaded` from mark_loop
            jl_datatype_t *et = (jl_datatype_t*)jl_tparam0(vt);
            const jl_datatype_layout_t *layout = et->layout;
            unsigned npointers = layout->npointers;
            unsigned elsize = a->elsize / sizeof(jl_value_t*);
            size_t l = jl_array_len(a);
            uintptr_t nptr = ((l * npointers) << 2);
            jl_value_t** objary_begin = (jl_value_t**)a->data;
            jl_value_t** objary_end = objary_begin + l * elsize;
            uint8_t *obj8_begin;
            uint8_t *obj8_end;

            if (npointers == 1) { // inlining label `objarray_loaded` from mark_loop
                objary_begin += layout->first_ptr;
                for (; objary_begin < objary_end; objary_begin+=elsize) {
                    process_edge(closure, objary_begin, obj, type_name, vt);
                }
            } else if (layout->fielddesc_type == 0) { // inlining label `array8_loaded` from mark_loop
                obj8_begin = (uint8_t*)jl_dt_layout_ptrs(layout);
                obj8_end = obj8_begin + npointers;
                size_t elsize = ((jl_array_t*)obj)->elsize / sizeof(jl_value_t*);
                jl_value_t **begin = objary_begin;
                jl_value_t **end = objary_end;
                uint8_t *elem_begin = obj8_begin;
                uint8_t *elem_end = obj8_end;

                for (; begin < end; begin += elsize) {
                    for (; elem_begin < elem_end; elem_begin++) {
                        jl_value_t **slot = &begin[*elem_begin];
                        process_edge(closure, slot, obj, type_name, vt);
                    }
                    elem_begin = obj8_begin;
                }
            } else {
                assert(0 && "unimplemented");
            }
        } else { 
            return;
        }
    } else if (vt == jl_module_type) { // inlining label `module_binding` from mark_loop
        jl_module_t *m = (jl_module_t*)obj;
        jl_binding_t **table = (jl_binding_t**)m->bindings.table;
        size_t bsize = m->bindings.size;
        uintptr_t nptr = ((bsize + m->usings.len + 1) << 2);
        gc_mark_binding_t binding = {m, table + 1, table + bsize, nptr, 0};
        jl_binding_t **begin = (jl_binding_t**)m->bindings.table + 1;
        jl_binding_t **end = (jl_binding_t**)m->bindings.table + bsize;
        for (; begin < end; begin += 2) {
            jl_binding_t *b = *begin;
            if (b == (jl_binding_t*)HT_NOTFOUND)
                continue;
            process_edge(closure, begin, obj, type_name, vt);
            
            void *vb = jl_astaggedvalue(b);
            verify_parent1("module", binding->parent, &vb, "binding_buff");
            (void)vb;
            jl_value_t *value = b->value;
            jl_value_t *globalref = b->globalref;

            process_edge(closure, &b->value, obj, type_name, vt);
            process_edge(closure, &b->globalref, obj, type_name, vt);
            process_edge(closure, &b->owner, obj, type_name, vt);
            process_edge(closure, &b->ty, obj, type_name, vt);
        }
        jl_module_t *parent = binding.parent;
        process_edge(closure, &parent->parent, obj, type_name, vt);

        size_t nusings = m->usings.len;
        if (nusings) {
            size_t i;
            for (i = 0; i < m->usings.len; i++) {
                process_edge(closure, &m->usings.items[i], obj, type_name, vt);
            }
        }
    } else if (vt == jl_task_type) { // scanning a jl_task_type object
        jl_task_t *ta = (jl_task_t*)obj;
        void *stkbuf = ta->stkbuf;
        int16_t tid = ta->tid;
#ifdef COPY_STACKS
        if (stkbuf && ta->copy_stack && object_is_managed_by_mmtk(ta->stkbuf))
            process_edge(closure, &ta->stkbuf, obj, type_name, vt);
#endif
        jl_gcframe_t *s = ta->gcstack;
        size_t nroots;
        uintptr_t offset = 0;
        uintptr_t lb = 0;
        uintptr_t ub = (uintptr_t)-1;
#ifdef COPY_STACKS
        if (stkbuf && ta->copy_stack && ta->ptls == NULL) {
            assert(ta->tid >= 0);
            jl_ptls_t ptls2 = jl_all_tls_states[ta->tid];
            ub = (uintptr_t)ptls2->stackbase;
            lb = ub - ta->copy_stack;
            offset = (uintptr_t)stkbuf - lb;
        }
#endif
        if (s) { // inlining label `stack` from mark_loop
            nroots = mmtk_gc_read_stack(&s->nroots, offset, lb, ub);
            assert(nroots <= UINT32_MAX);
            gc_mark_stackframe_t stack = {s, 0, (uint32_t)nroots, offset, lb, ub};
            jl_gcframe_t *s = stack.s;
            uint32_t i = stack.i;
            uint32_t nroots = stack.nroots;
            uintptr_t offset = stack.offset;
            uintptr_t lb = stack.lb;
            uintptr_t ub = stack.ub;
            uint32_t nr = nroots >> 2;
            uintptr_t nptr = 0;
            jl_value_t *new_obj = NULL;
            while (1) {
                jl_value_t ***rts = (jl_value_t***)(((void**)s) + 2);
                for (; i < nr; i++) {
                    if (nroots & 1) {
                        void **slot = (void**)mmtk_gc_read_stack(&rts[i], offset, lb, ub);
                        uintptr_t real_addr = mmtk_gc_get_stack_addr(slot, offset, lb, ub);
                        new_obj = *(uintptr_t*)real_addr;
                        process_edge(closure, real_addr, obj, type_name, vt);
                    }
                    else {
                        uintptr_t real_addr = mmtk_gc_get_stack_addr(&rts[i], offset, lb, ub);
                        new_obj = *(uintptr_t*)real_addr;
                        process_edge(closure, real_addr, obj, type_name, vt);
                    }
                }

                s = (jl_gcframe_t*)mmtk_gc_read_stack(&s->prev, offset, lb, ub);
                if (s != 0) {
                    stack.s = s;
                    i = 0;
                    uintptr_t new_nroots = mmtk_gc_read_stack(&s->nroots, offset, lb, ub);
                    assert(new_nroots <= UINT32_MAX);
                    nroots = stack.nroots = (uint32_t)new_nroots;
                    nr = nroots >> 2;
                    continue;
                }
                break;
            }
        }
        if (ta->excstack) { // inlining label `excstack` from mark_loop
            // if it is not managed by MMTk, nothing needs to be done because the object does not need to be scanned
            if (object_is_managed_by_mmtk(ta->excstack)) {
                process_edge(closure, &ta->excstack, obj, type_name, vt);
            }
            jl_excstack_t *excstack = ta->excstack;
            size_t itr = ta->excstack->top;
            size_t bt_index = 0;
            size_t jlval_index = 0;
            while (itr > 0) {
                size_t bt_size = jl_excstack_bt_size(excstack, itr);
                jl_bt_element_t *bt_data = jl_excstack_bt_data(excstack, itr);
                for (; bt_index < bt_size; bt_index += jl_bt_entry_size(bt_data + bt_index)) {
                    jl_bt_element_t *bt_entry = bt_data + bt_index;
                    if (jl_bt_is_native(bt_entry))
                        continue;
                    // Found an extended backtrace entry: iterate over any
                    // GC-managed values inside.
                    size_t njlvals = jl_bt_num_jlvals(bt_entry);
                    while (jlval_index < njlvals) {
                        jl_value_t* new_obj = jl_bt_entry_jlvalue(bt_entry, jlval_index);
                        uintptr_t nptr = 0;
                        jl_value_t** new_obj_edge = &bt_entry[2 + jlval_index].jlvalue;
                        jlval_index += 1;
                        process_edge(closure, new_obj_edge, obj, type_name, vt);
                    }
                    jlval_index = 0;
                }

                jl_bt_element_t *stack_raw = (jl_bt_element_t *)(excstack+1);
                jl_value_t** stack_obj_edge = &stack_raw[itr-1].jlvalue;

                jl_value_t* new_obj = jl_excstack_exception(excstack, itr);
                itr = jl_excstack_next(excstack, itr);
                bt_index = 0;
                jlval_index = 0;
                process_edge(closure, stack_obj_edge, obj, type_name, vt);
            }
        }
        const jl_datatype_layout_t *layout = jl_task_type->layout; // inlining label `obj8_loaded` from mark_loop 
        assert(layout->fielddesc_type == 0);
        assert(layout->nfields > 0);
        uint32_t npointers = layout->npointers;
        uint8_t *obj8_begin = (uint8_t*)jl_dt_layout_ptrs(layout);
        uint8_t *obj8_end = obj8_begin + npointers;
        (void)jl_assume(obj8_begin < obj8_end);
        for (; obj8_begin < obj8_end; obj8_begin++) {
            jl_value_t **slot = &((jl_value_t**)obj)[*obj8_begin];
            process_edge(closure, slot, obj, type_name, vt);
        }
    } else if (vt == jl_string_type) { // scanning a jl_string_type object
        return;
    } else {  // scanning a jl_datatype object
        size_t dtsz = jl_datatype_size(vt);
        if (vt == jl_weakref_type) {
            return;
        }
        const jl_datatype_layout_t *layout = vt->layout;
        uint32_t npointers = layout->npointers;
        if (npointers == 0) {
            return;
        } else {
            assert(layout->nfields > 0 && layout->fielddesc_type != 3 && "opaque types should have been handled specially");
            if (layout->fielddesc_type == 0) { // inlining label `obj8_loaded` from mark_loop 
                uint8_t *obj8_begin;
                uint8_t *obj8_end;
                
                obj8_begin = (uint8_t*)jl_dt_layout_ptrs(layout);
                obj8_end = obj8_begin + npointers;

                (void)jl_assume(obj8_begin < obj8_end);
                for (; obj8_begin < obj8_end; obj8_begin++) {
                    jl_value_t **slot = &((jl_value_t**)obj)[*obj8_begin];
                    process_edge(closure, slot, obj, type_name, vt);
                }
            }
            else if(layout->fielddesc_type == 1) { // inlining label `obj16_loaded` from mark_loop 
                // scan obj16
                char *obj16_parent;
                uint16_t *obj16_begin;
                uint16_t *obj16_end;
                obj16_begin = (uint16_t*)jl_dt_layout_ptrs(layout);
                obj16_end = obj16_begin + npointers;
                for (; obj16_begin < obj16_end; obj16_begin++) {
                    jl_value_t **slot = &((jl_value_t**)obj)[*obj16_begin];
                    process_edge(closure, slot, obj, type_name, vt);
                }
            }
            else if (layout->fielddesc_type == 2) {
                // FIXME: scan obj32
                assert(0 && "unimplemented for obj32");
            }
            else {
                // simply dispatch the work at the end of the function
                assert(layout->fielddesc_type == 3);
            }
        }
    }

    return;
}


void introspect_objects_after_copying(void* from, void* to) {
    jl_value_t* jl_from = (jl_value_t*) from;
    jl_value_t* jl_to = (jl_value_t*) to;

    uintptr_t tag_to = (jl_value_t*)jl_typeof(jl_to);
    uintptr_t tag_from = (jl_value_t*)jl_typeof(jl_from);

    if(tag_to != tag_from) {
        printf("TAGS ARE DIFFERENT?\n");
        fflush(stdout);
    }

    const char *type_name = jl_typeof_str(from);
    FILE *fp;
    fp = fopen("/home/eduardo/mmtk-julia/copied_objs.log", "a");
    fprintf(fp, "\ttype = %s\n", type_name);
    fflush(fp);
    fclose(fp);

    jl_datatype_t *vt = (jl_datatype_t*)tag_to;
    if(vt->name == jl_array_typename) {
        jl_array_t *a = (jl_array_t*)jl_from;
        jl_array_flags_t flags = a->flags;
        jl_array_t *b = (jl_array_t*)jl_to;
        jl_array_flags_t bflags = b->flags;
        int data_is_inlined = 0;
        if (a->flags.how == 0) {
            if (object_is_managed_by_mmtk(a->data)) {
                size_t pre_data_bytes = ((size_t)a->data - a->offset*a->elsize) - (size_t)a;
                if (pre_data_bytes > 0 && pre_data_bytes <= ARRAY_INLINE_NBYTES) {
                    // data is inlined and pointer in copied array should be updated
                    // printf("a->data = %p, b->data = %p, a = %p, b = %p\n", a->data, b->data, a, b);
                    // fflush(stdout);
                    b->data = (size_t) b + pre_data_bytes;
                    data_is_inlined = 1;
                    // printf("a->data = %p, b->data = %p, a = %p, b = %p\n", a->data, b->data, a, b);
                    // fflush(stdout);
                }
            }
        }
        jl_value_t* data = (jl_value_t*)((size_t)a->data - a->offset*a->elsize);
        uintptr_t tag_data = (jl_value_t*)jl_typeof(data);
        // if(tag_data != jl_buff_tag && data_is_inlined != 1 && object_is_managed_by_mmtk(data)) {
        //     printf("Buffer isn't a buffer? a = %p, a->data = %p, a->how = %d, a->offset = %d\n", a, a->data, a->flags.how, a->offset);
        //     fflush(stdout);
        // }


    }
}

Julia_Upcalls mmtk_upcalls = { scan_julia_obj, scan_julia_exc_obj, get_stackbase, calculate_roots, run_finalizer_function, get_jl_last_err, set_jl_last_err, get_lo_size,
                               get_so_size, get_obj_start_ref, wait_for_the_world, set_gc_initial_state, set_gc_final_state, set_gc_old_state, mmtk_jl_run_finalizers,
                               jl_throw_out_of_memory_error, mark_object_as_scanned, object_has_been_scanned, mmtk_sweep_malloced_arrays,
                               mmtk_wait_in_a_safepoint, mmtk_exit_from_safepoint, introspect_objects_after_copying
                             };

