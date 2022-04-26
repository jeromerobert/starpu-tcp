
extern crate bindgen;

use std::env;
use std::path::PathBuf;
use std::path::MAIN_SEPARATOR;

fn main() {
    let starpu_src = env::var("STARPU_SRC_DIR").unwrap();
    let starpu_build = env::var("STARPU_BUILD_DIR").unwrap();
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    let starpu_i_inc = format!("-I{}{}include", starpu_src, MAIN_SEPARATOR);
    let starpu_i_src = format!("-I{}{}src", starpu_src, MAIN_SEPARATOR);
    let starpu_i_binc = format!("-I{}{}include", starpu_build, MAIN_SEPARATOR);
    let starpu_i_bsrc = format!("-I{}{}src", starpu_build, MAIN_SEPARATOR);
    println!("cargo:rustc-link-search={}{1}src{1}.libs", starpu_build, MAIN_SEPARATOR);
    println!("cargo:rustc-link-lib=starpu-1.3");
    let bindings = bindgen::Builder::default()
        .clang_arg(&starpu_i_inc)
        .clang_arg(&starpu_i_src)
        .clang_arg(&starpu_i_binc)
        .clang_arg(&starpu_i_bsrc)
        .header(format!("{}{1}src{1}datawizard{1}coherency.h", starpu_src, MAIN_SEPARATOR))
        .allowlist_function("starpu_task_submit")
        .allowlist_function("starpu_codelet_pack_arg_init")
        .allowlist_function("starpu_codelet_pack_arg_fini")
        .allowlist_function("starpu_data_pack")
        .allowlist_function("starpu_codelet_pack_arg")
        .allowlist_function("starpu_task_create")
        .allowlist_function("starpu_task_destroy")
        .allowlist_function("starpu_data_acquire_cb")
        .allowlist_function("starpu_task_wait_for_all")
        .allowlist_function("starpu_task_insert_data_process_array_arg")
        .allowlist_function("starpu_task_insert_data_process_arg")
        .allowlist_function("starpu_data_unpack")
        .allowlist_function("starpu_data_release")
        .allowlist_function("starpu_free")
        .allowlist_function("starpu_malloc")
        .allowlist_function("starpu_task_insert")
        .allowlist_function("_starpu_data_set_unregister_hook")
        .allowlist_function("starpu_task_declare_deps_array")
        .allowlist_function("starpu_data_register_same")
        .allowlist_function("starpu_data_unregister_submit")
        .opaque_type("_starpu_data_request")
        .opaque_type("_starpu_jobid_list")
        .opaque_type("starpu_data_interface_ops")
        .opaque_type("_starpu_data_replicate")
        .opaque_type("starpu_profiling_task_info")
        .opaque_type("_starpu_task_wrapper_.*")
        .opaque_type("_starpu_data_requester_.*")
        .opaque_type("starpu_perfmodel")
        .allowlist_var("STARPU_.*")
        .generate()
        .expect("Unable to generate bindings");
    bindings
        .write_to_file(out_path.join("starpu_coherency.rs"))
        .expect("Couldn't write bindings!");

    let bindings = bindgen::Builder::default()
        .clang_arg(&starpu_i_inc)
        .clang_arg(&starpu_i_binc)
        .header(format!("{}{1}mpi{1}include{1}starpu_mpi.h", starpu_src, MAIN_SEPARATOR))
        .allowlist_type("starpu_mpi_tag_t")
        .allowlist_type("MPI_Comm")
        .generate()
        .expect("Unable to generate bindings");
    bindings
        .write_to_file(out_path.join("starpu_mpi.rs"))
        .expect("Couldn't write bindings!");
}
