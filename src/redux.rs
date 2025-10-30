use crate::{
    c_uint, c_void, data_from_handle, recv_with_callback, send_with_callback, singleton,
    starpu_codelet, starpu_data_access_mode, starpu_data_access_mode_STARPU_R,
    starpu_data_access_mode_STARPU_RW, starpu_data_access_mode_STARPU_W, starpu_data_handle_t,
    starpu_data_register_same, starpu_data_unregister_submit, starpu_mpi_tag_t, starpu_task,
    starpu_task_create, starpu_task_declare_deps_array, starpu_task_insert, starpu_task_submit,
    starpu_task_wait_for_all, MaybeUninit, Rank, STARPU_CALLBACK_WITH_ARG,
};

// Translated to rust from mpi/src/starpu_mpi_task_insert.c

const extern "C" fn dummy_func(_: *mut *mut c_void, _: *mut c_void) {}

unsafe impl Send for starpu_codelet {}
unsafe impl Sync for starpu_codelet {}

unsafe extern "C" fn detached_callback(arg: *mut c_void) {
    let bargs = arg.cast::<Arguments>();
    let args = &*bargs;
    args.task_b.as_mut().unwrap().get_handles_mut()[1] = args.new_handle;
    assert_eq!(starpu_task_submit(args.task_b), 0);
    starpu_data_unregister_submit(args.new_handle);
}

impl starpu_codelet {
    const fn with_zero() -> Self {
        unsafe { MaybeUninit::zeroed().assume_init() }
    }
    const fn with_modes(mode: starpu_data_access_mode) -> Self {
        let mut r: Self = Self::with_zero();
        r.cpu_funcs[0] = Some(dummy_func);
        r.cuda_funcs[0] = Some(dummy_func);
        r.opencl_funcs[0] = Some(dummy_func);
        r.nbuffers = 1;
        r.modes[0] = mode;
        r
    }
}

struct Arguments {
    data_handle: starpu_data_handle_t,
    new_handle: starpu_data_handle_t,
    data_tag: starpu_mpi_tag_t,
    node: Rank,
    task_b: *mut starpu_task,
}

unsafe extern "C" fn recv_callback(varg: *mut c_void) {
    let arg = &mut *varg.cast::<Arguments>();
    starpu_data_register_same(&raw mut arg.new_handle, arg.data_handle);
    recv_with_callback(
        arg.new_handle,
        arg.node,
        arg.data_tag,
        Some(detached_callback),
        varg,
        true,
    );
}

/// Translated to rust from `starpu_mpi_redux_data_prio`
pub unsafe fn perform(data_handle: starpu_data_handle_t) {
    static mut READ_CALLBACK: starpu_codelet =
        starpu_codelet::with_modes(starpu_data_access_mode_STARPU_R);
    static mut READWRITE_CALLBACK: starpu_codelet =
        starpu_codelet::with_modes(starpu_data_access_mode_STARPU_RW);
    let s = singleton();
    let me = s.rank();
    let dh = data_from_handle(data_handle);
    let (tag, rank) = (dh.tag(), dh.rank());
    let nb_nodes = s.world_size();
    if me == rank {
        let mut task_b_s = Vec::with_capacity(nb_nodes as usize);
        for i in 0..nb_nodes {
            if i == rank {
                continue;
            }
            let args = Box::new(Arguments {
                new_handle: std::ptr::null_mut(),
                data_handle,
                data_tag: tag,
                node: i,
                task_b: starpu_task_create(),
            });
            (*args.task_b).cl = (*data_handle).redux_cl;
            (*args.task_b).set_sequential_consistency(0);
            (*args.task_b).get_handles_mut()[0] = data_handle;
            task_b_s.push(args.task_b);
            starpu_task_insert(
                (&raw const READ_CALLBACK).cast_mut(),
                starpu_data_access_mode_STARPU_R,
                data_handle,
                STARPU_CALLBACK_WITH_ARG,
                recv_callback as unsafe extern "C" fn(*mut std::ffi::c_void),
                Box::into_raw(args),
                0,
            );
        }
        let task_c = starpu_task_create();
        let pc: *const starpu_codelet = &raw const READWRITE_CALLBACK;
        (*task_c).cl = pc.cast_mut();
        (*task_c).get_handles_mut()[0] = data_handle;
        starpu_task_declare_deps_array(task_c, task_b_s.len() as c_uint, task_b_s.as_mut_ptr());
        assert_eq!(starpu_task_submit(task_c), 0);
    } else {
        send_with_callback(data_handle, rank, tag, 0, None, std::ptr::null_mut(), true);
        starpu_task_insert(
            (*data_handle).init_cl,
            starpu_data_access_mode_STARPU_W,
            data_handle,
            0,
        );
    }
    starpu_task_wait_for_all();
}
