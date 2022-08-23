// This files contains the public API of starpu-tcp that is the one which mimic starpu-mpi
// With redux.rs they are the 2 ones which keep all unsafe function and everything specific to starpu
// Everything not specific to starpu should go to core and be safe.

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(deref_nullptr)]
#![allow(improper_ctypes)]
#![allow(clippy::approx_constant)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::useless_transmute)]
#![feature(c_variadic)] // https://github.com/rust-lang/rust/issues/44930
#![feature(const_maybe_uninit_zeroed)] // https://github.com/rust-lang/rust/issues/91850
include!(concat!(env!("OUT_DIR"), "/starpu_coherency.rs"));
include!(concat!(env!("OUT_DIR"), "/starpu_mpi.rs"));
mod core;
mod redux;
use crate::core::{HandleData, HandleManager, Rank, RecvReq, SendReq, Tag};
use std::os::raw::{c_char, c_int, c_uint, c_ulong, c_void};
use std::{ffi::VaList, mem::MaybeUninit, sync::Once};
extern crate log;
use log::*;

#[derive(Debug)]
struct StarPUBuffer {
    size: usize,
    raw: *mut c_void,
}
unsafe impl Send for StarPUBuffer {}
unsafe impl Sync for StarPUBuffer {}

#[derive(Debug, Copy, Clone)]
struct Req {
    /// can be 0 in case of barrier
    handle: starpu_data_handle_t,
    rank: Rank,
    tag: starpu_mpi_tag_t,
    prio: isize,
    callback: Option<unsafe extern "C" fn(arg: *mut c_void)>,
    cb_args: *mut c_void,
    early: bool,
}

impl Req {
    fn without_handle(rank: Rank) -> Self {
        Req {
            handle: std::ptr::null_mut(),
            rank,
            tag: -1,
            prio: 0,
            callback: None,
            cb_args: std::ptr::null_mut(),
            early: false,
        }
    }
}
unsafe impl Send for Req {}
unsafe impl Sync for Req {}

fn data_from_handle<'a>(handle: starpu_data_handle_t) -> &'a mut HandleData {
    unsafe { Box::leak(Box::from_raw((*handle).mpi_data as *mut HandleData)) }
}

fn size_from_handle(handle: starpu_data_handle_t) -> starpu_ssize_t {
    let mut size: starpu_ssize_t = 0;
    if !handle.is_null() {
        unsafe {
            starpu_data_pack(handle, std::ptr::null_mut(), &mut size);
        }
    }
    size
}

impl StarPUBuffer {
    unsafe fn from_handle(handle: starpu_data_handle_t) -> Self {
        if handle.is_null() {
            return StarPUBuffer {
                size: 0,
                raw: std::ptr::null_mut(),
            };
        }
        let mut size = size_from_handle(handle);
        let mut raw: *mut c_void = std::ptr::null_mut();
        let r = starpu_data_pack(handle, &mut raw, &mut size);
        assert_eq!(r, 0);
        StarPUBuffer {
            size: size as usize,
            raw,
        }
    }
    unsafe fn with_size(size: usize) -> Self {
        let mut r = StarPUBuffer {
            size: size as usize,
            raw: std::ptr::null_mut(),
        };
        if size > 0 {
            starpu_malloc(&mut r.raw, size as size_t);
        }
        r
    }
}

impl AsRef<[u8]> for StarPUBuffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.raw as *mut u8, self.size) }
    }
}

impl Drop for StarPUBuffer {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe {
                starpu_free(self.raw);
            }
        }
    }
}

impl SendReq for Req {
    type Buf = StarPUBuffer;
    fn tag(&self) -> Tag {
        self.tag as Tag
    }

    fn clear(&self) {
        if !self.handle.is_null() {
            unsafe {
                starpu_data_release(self.handle);
            }
        }
        if self.callback.is_some() {
            let f = self.callback.unwrap();
            unsafe {
                f(self.cb_args);
            }
        }
    }

    fn data(&self) -> StarPUBuffer {
        unsafe { StarPUBuffer::from_handle(self.handle) }
    }

    fn size(&self) -> usize {
        size_from_handle(self.handle) as usize
    }

    fn priority(&self) -> isize {
        self.prio
    }

    fn early(&self) -> bool {
        self.early
    }
}

impl AsMut<[u8]> for StarPUBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.raw as *mut u8, self.size) }
    }
}

impl RecvReq for Req {
    type Buf = StarPUBuffer;

    fn tag(&self) -> Tag {
        self.tag
    }

    fn create_buf(size: usize) -> Self::Buf {
        unsafe { StarPUBuffer::with_size(size) }
    }

    fn finish(&self, mut buf: Self::Buf) {
        if !self.handle.is_null() {
            let b = buf.as_mut();
            unsafe {
                let r = starpu_data_unpack(
                    self.handle,
                    b.as_mut_ptr() as *mut c_void,
                    b.len() as size_t,
                );
                assert_eq!(r, 0);
                // Because starpu_data_unpack has freed the memory
                buf.raw = std::ptr::null_mut();
                starpu_data_release(self.handle);
            }
        }
        if self.callback.is_some() {
            let f = self.callback.unwrap();
            unsafe {
                f(self.cb_args);
            }
        }
    }

    fn early(&self) -> bool {
        self.early
    }
}

struct StarPUManager(HandleManager<Req, Req>);

// https://stackoverflow.com/questions/27791532/how-do-i-create-a-global-mutable-singleton
fn singleton() -> &'static mut HandleManager<Req, Req> {
    static mut SINGLETON: MaybeUninit<StarPUManager> = MaybeUninit::uninit();
    static ONCE: Once = Once::new();
    unsafe {
        ONCE.call_once(|| {
            SINGLETON.write(StarPUManager(HandleManager::new()));
        });
        &mut SINGLETON.assume_init_mut().0
    }
}

fn find_executee_node(task: &starpu_task, xrank: c_int) -> c_int {
    let modes = task.get_modes();
    let handles = task.get_handles();
    if xrank >= 0 {
        // rank already set
        return xrank;
    }
    for i in 0..modes.len() {
        if modes[i] & starpu_data_access_mode_STARPU_W != 0
            || modes[i] & starpu_data_access_mode_STARPU_REDUX != 0
        {
            return data_from_handle(handles[i]).rank().into();
        }
    }
    // No WRITE handle found
    xrank
}

unsafe extern "C" fn recv_cb(arg: *mut c_void) {
    let req = Box::from_raw(arg as *mut Req);
    singleton().recv(req.rank as Rank, *req);
}

unsafe extern "C" fn send_cb(arg: *mut c_void) {
    let req = Box::from_raw(arg as *mut Req);
    singleton().send(req.rank as Rank, *req);
}

fn send(handle: starpu_data_handle_t, rank: Rank, tag: starpu_mpi_tag_t, prio: isize) {
    send_with_callback(handle, rank, tag, prio, None, std::ptr::null_mut(), false);
}

fn send_with_callback(
    handle: starpu_data_handle_t,
    rank: Rank,
    tag: starpu_mpi_tag_t,
    prio: isize,
    callback: Option<unsafe extern "C" fn(a: *mut c_void)>,
    cb_args: *mut c_void,
    early: bool,
) {
    let req = Box::into_raw(Box::new(Req {
        handle,
        rank,
        tag,
        prio,
        callback,
        cb_args,
        early,
    }));
    unsafe {
        starpu_data_acquire_cb(
            handle,
            starpu_data_access_mode_STARPU_R,
            Some(send_cb),
            req as *mut c_void,
        );
    }
}

fn recv(handle: starpu_data_handle_t, rank: Rank, tag: starpu_mpi_tag_t) {
    recv_with_callback(handle, rank, tag, None, std::ptr::null_mut(), false);
}

fn recv_with_callback(
    handle: starpu_data_handle_t,
    rank: Rank,
    tag: starpu_mpi_tag_t,
    callback: Option<unsafe extern "C" fn(a: *mut c_void)>,
    cb_args: *mut c_void,
    early: bool,
) {
    let req = Box::into_raw(Box::new(Req {
        handle,
        rank,
        tag,
        prio: 0,
        callback,
        cb_args,
        early,
    }));
    unsafe {
        starpu_data_acquire_cb(
            handle,
            starpu_data_access_mode_STARPU_W,
            Some(recv_cb),
            req as *mut c_void,
        );
    }
}

fn exchange_data_before_execution(
    modes: &[starpu_data_access_mode],
    handles: &[starpu_data_handle_t],
    xrank: Rank,
    me: Rank,
    prio: isize,
) {
    for i in 0..modes.len() {
        if modes[i] & starpu_data_access_mode_STARPU_R == 0 {
            continue;
        }
        let hd = data_from_handle(handles[i]);
        let cur_rank = hd.cur_rank();
        if me == xrank && !hd.is_on(me) {
            trace!("{} insert receive {} from {}", me, hd.tag(), cur_rank);
            // I execute the task but I do not have the handle. So I received it.
            recv(handles[i], cur_rank, hd.tag());
        }
        if me == cur_rank && me != xrank && !hd.is_on(xrank) {
            trace!("{} insert send {} to {}", me, hd.tag(), xrank);
            send(handles[i], xrank, hd.tag(), prio);
        }
        hd.set_on(xrank);
    }
}

fn task_insert_v(codelet: *mut starpu_codelet, args: VaList) -> c_int {
    let me = singleton().rank();
    let ptask = unsafe { starpu_task_create() };
    let task = unsafe { &mut *ptask };
    assert_eq!(task.magic, 42);
    let xrank = unsafe { task_insert_create(&mut *codelet, task, args) };
    let prio = task.priority;
    // clone handles and modes because starpu_task_submit may delete the task
    let handles: Vec<starpu_data_handle_t> = task.get_handles().to_vec();
    let modes: Vec<starpu_data_access_mode> = task.get_modes().to_vec();
    assert_eq!(modes.len(), handles.len());
    exchange_data_before_execution(&modes, &handles, xrank, me, prio as isize);
    if xrank == me {
        let ret = unsafe { starpu_task_submit(ptask) };
        assert_eq!(ret, 0);
    }
    for i in 0..modes.len() {
        // We deliberately choose not to systematically return the handle to its original node.
        // If someone wants that, he has to call starpu_mpi_get_data_on_node. This is
        // different from starpu-mpi do.
        // When a handle has been written on a node, invalidate data on the other nodes.
        if modes[i] & starpu_data_access_mode_STARPU_W != 0 {
            let dh = data_from_handle(handles[i]);
            if me != xrank && dh.is_on(me) {
                unsafe {
                    starpu_data_invalidate_submit(handles[i]);
                }
            }
            dh.set_on_only(xrank);
        }
    }
    if xrank != me {
        task.set_destroy(0);
        unsafe {
            starpu_task_destroy(ptask);
        }
    }
    0
}

unsafe extern "C" fn unregister_hook(handle: starpu_data_handle_t) {
    // free the HandleData struct
    Box::from_raw((*handle).mpi_data);
}

impl starpu_task {
    fn get_nbuffers(self) -> usize {
        let cl = unsafe { &*self.cl };
        if cl.nbuffers == STARPU_VARIABLE_NBUFFERS {
            self.nbuffers as usize
        } else {
            cl.nbuffers as usize
        }
    }

    fn get_handles(&self) -> &[starpu_data_handle_t] {
        let n = self.get_nbuffers();
        if self.dyn_handles.is_null() {
            &self.handles[0..n]
        } else {
            unsafe { std::slice::from_raw_parts(self.dyn_handles, n) }
        }
    }

    fn get_handles_mut(&mut self) -> &mut [starpu_data_handle_t] {
        let n = self.get_nbuffers();
        if self.dyn_handles.is_null() {
            &mut self.handles[0..n]
        } else {
            assert!(!self.dyn_modes.is_null());
            unsafe { std::slice::from_raw_parts_mut(self.dyn_handles, n) }
        }
    }

    fn get_modes(&self) -> &[starpu_data_access_mode] {
        let n = self.get_nbuffers();
        unsafe {
            if !self.dyn_modes.is_null() {
                assert!(!self.dyn_handles.is_null());
                return std::slice::from_raw_parts(self.dyn_modes, n);
            }
            let cl = &*self.cl;
            if cl.nbuffers == STARPU_VARIABLE_NBUFFERS {
                &self.modes[0..n]
            } else if !cl.dyn_modes.is_null() {
                std::slice::from_raw_parts(cl.dyn_modes, n)
            } else {
                &cl.modes[0..n]
            }
        }
    }
}

/// Return true if val is a valid StarPU mode (ex: STARPU_RW)
fn is_mode(val: u32) -> bool {
    (val & starpu_data_access_mode_STARPU_W != 0)
        || (val & starpu_data_access_mode_STARPU_R != 0)
        || (val & starpu_data_access_mode_STARPU_REDUX != 0)
}

// Adapted from _starpu_task_insert_create in starpu/src/util/starpu_task_insert_utils.c
// also return the execution node
unsafe fn task_insert_create(
    cl: &mut starpu_codelet,
    task: &mut starpu_task,
    mut varg_list: std::ffi::VaList,
) -> Rank {
    let mut current_buffer: c_int = 0;
    let mut allocated_buffers: c_int = 0;
    let mut state = std::mem::MaybeUninit::uninit();
    starpu_codelet_pack_arg_init(state.as_mut_ptr());
    let mut state = state.assume_init();
    let mut xrank = -1;
    task.cl = cl;
    task.set_cl_arg_free(1);
    loop {
        let arg_type: u32 = varg_list.arg();
        match arg_type {
            0 => break,
            mode if is_mode(mode) => {
                let handle: starpu_data_handle_t = varg_list.arg();
                starpu_task_insert_data_process_arg(
                    cl,
                    task,
                    &mut allocated_buffers,
                    &mut current_buffer,
                    mode as c_int,
                    handle,
                );
            }
            STARPU_DATA_ARRAY => {
                // Expect to find a array of handles and its size
                let handles: *mut starpu_data_handle_t = varg_list.arg();
                let nb_handles: c_int = varg_list.arg();
                starpu_task_insert_data_process_array_arg(
                    cl,
                    task,
                    &mut allocated_buffers,
                    &mut current_buffer,
                    nb_handles,
                    handles,
                );
            }
            STARPU_VALUE => {
                let ptr: *mut c_void = varg_list.arg();
                let ptr_size: c_ulong = varg_list.arg();
                starpu_codelet_pack_arg(&mut state, ptr, ptr_size)
            }
            STARPU_PRIORITY => task.priority = varg_list.arg(),
            STARPU_EXECUTE_ON_DATA => {
                let h = varg_list.arg::<starpu_data_handle_t>();
                xrank = data_from_handle(h).rank().into()
            }
            STARPU_NAME => task.name = varg_list.arg(),
            STARPU_EXECUTE_ON_NODE => xrank = varg_list.arg::<c_int>(),
            _ => panic!(
                "Unrecognized argument {}, did you perhaps forget to end arguments with 0?",
                arg_type
            ),
        }
    }
    if state.nargs > 0 {
        starpu_codelet_pack_arg_fini(&mut state, &mut task.cl_arg, &mut task.cl_arg_size);
    }
    xrank = find_executee_node(task, xrank);
    assert!(xrank != -1);
    xrank as Rank
}

fn wait_for_comms(time_resol_ms: u64) {
    loop {
        if singleton().is_done() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(time_resol_ms));
    }
}

fn wait_for_all() {
    // TODO: add a timer to detect deadlock
    loop {
        // Tasks may post comms so we must wait for both alternatively
        unsafe {
            let r = starpu_task_wait_for_all();
            assert_eq!(r, 0);
        }
        if singleton().is_done() {
            break;
        }
        wait_for_comms(5);
    }
}

fn barrier() {
    let s = singleton();
    let rank = s.rank();
    let ws = s.world_size();
    debug!("[{}] Starting barrier", rank);
    if rank == 0 {
        for i in 1..ws {
            let r = Req::without_handle(i);
            s.recv(i, r);
            s.send(i, r);
        }
    } else {
        let r = Req::without_handle(0);
        s.send(0, r);
        s.recv(0, r);
    }
    // there are no starpu task there so we just wait for comms
    wait_for_comms(1);
    debug!("[{}] Barrier done", rank);
}

/// # Safety
/// Unsafe because var args
#[no_mangle]
pub unsafe extern "C" fn starpu_mpi_insert_task(
    _comm: MPI_Comm,
    codelet: *mut starpu_codelet,
    mut args: ...
) -> c_int {
    task_insert_v(codelet, args.as_va_list())
}

/// # Safety
/// Unsafe because var args
#[no_mangle]
pub unsafe extern "C" fn starpu_mpi_task_insert(
    _comm: MPI_Comm,
    codelet: *mut starpu_codelet,
    mut args: ...
) -> c_int {
    task_insert_v(codelet, args.as_va_list())
}

#[doc = "Call starpu_mpi_init_comm() with the MPI communicator \\c MPI_COMM_WORLD."]
#[no_mangle]
pub unsafe extern "C" fn starpu_mpi_init(
    _argc: *mut c_int,
    _argv: *mut *mut *mut c_char,
    _initialize_mpi: c_int,
) -> c_int {
    singleton().start();
    0
}

#[doc = "Register to MPI a StarPU data handle with the given tag, rank and MPI"]
#[doc = "communicator. It also automatically clears the MPI communication cache"]
#[doc = "when unregistering the data."]
#[no_mangle]
pub unsafe extern "C" fn starpu_mpi_data_register_comm(
    data_handle: starpu_data_handle_t,
    data_tag: starpu_mpi_tag_t,
    rank: c_int,
    _comm: MPI_Comm,
) {
    let ws = singleton().world_size();
    assert!(
        (rank as Rank) < ws,
        "starpu_mpi_data_register_comm: invalid rank"
    );
    let b = Box::new(HandleData::new(data_tag as Tag, rank as Rank, ws.into()));
    (*data_handle).mpi_data = Box::into_raw(b) as *mut c_void;
    _starpu_data_set_unregister_hook(data_handle, Some(unregister_hook));
}

#[doc = "Return the rank of the given data."]
#[no_mangle]
pub unsafe extern "C" fn starpu_mpi_data_get_rank(handle: starpu_data_handle_t) -> c_int {
    data_from_handle(handle).rank().into()
}

#[doc = "Wait until all StarPU tasks and communications for the given"]
#[doc = "communicator are completed."]
#[no_mangle]
pub unsafe extern "C" fn starpu_mpi_wait_for_all(_comm: MPI_Comm) -> c_int {
    wait_for_all();
    barrier();
    0
}

#[doc = "Perform a reduction on the given data \\p handle. All nodes send the"]
#[doc = "data to its owner node which will perform a reduction."]
#[no_mangle]
pub unsafe extern "C" fn starpu_mpi_redux_data(_comm: MPI_Comm, data_handle: starpu_data_handle_t) {
    redux::perform(data_handle);
}

#[doc = "Clear the send and receive communication cache for all data and"]
#[doc = "invalidate their values. The function has to be called at the same"]
#[doc = "point of task graph submission by all the MPI nodes. The function"]
#[doc = "does nothing if the cache mechanism is disabled (see \\ref"]
#[doc = "STARPU_MPI_CACHE)."]
#[no_mangle]
pub unsafe extern "C" fn starpu_mpi_cache_flush_all_data(_comm: MPI_Comm) {
    // Nothing to do here
}

#[doc = "Transfer data \\p data_handle to MPI node \\p node, sending it from"]
#[doc = "its owner if needed. At least the target node and the owner have to"]
#[doc = "call the function."]
#[no_mangle]
pub unsafe extern "C" fn starpu_mpi_get_data_on_node(
    _comm: MPI_Comm,
    handle: starpu_data_handle_t,
    node: c_int,
) {
    let node = node as Rank;
    let s = singleton();
    let d = data_from_handle(handle);
    let (tag, rank) = (d.tag(), d.cur_rank());
    if node == rank {
        return;
    }
    let me = s.rank();
    if me == node {
        recv(handle, rank, tag);
    } else if me == rank {
        send(handle, node, tag, 0);
    }
}

#[doc = "Clean the starpumpi library. This must be called after calling any"]
#[doc = "\\c starpu_mpi functions and before the call to starpu_shutdown(),"]
#[doc = "if any. \\c MPI_Finalize() will be called if StarPU-MPI has been"]
#[doc = "initialized by starpu_mpi_init()."]
#[no_mangle]
pub unsafe extern "C" fn starpu_mpi_shutdown() -> c_int {
    singleton().shutdown();
    0
}

#[doc = "Post a standard-mode, non blocking send of \\p data_handle to the"]
#[doc = "node \\p dest using the message tag \\p data_tag within the"]
#[doc = "communicator \\p comm. On completion, the \\p callback function is"]
#[doc = "called with the argument \\p arg."]
#[doc = "Similarly to the pthread detached functionality, when a detached"]
#[doc = "communication completes, its resources are automatically released"]
#[doc = "back to the system, there is no need to test or to wait for the"]
#[doc = "completion of the request."]
#[no_mangle]
pub unsafe extern "C" fn starpu_mpi_isend_detached(
    data_handle: starpu_data_handle_t,
    dest: c_int,
    data_tag: starpu_mpi_tag_t,
    _comm: MPI_Comm,
    callback: Option<unsafe extern "C" fn(arg1: *mut c_void)>,
    arg: *mut c_void,
) -> c_int {
    send_with_callback(data_handle, dest as Rank, data_tag, 0, callback, arg, true);
    0
}

#[doc = "Post a nonblocking receive in \\p data_handle from the node \\p"]
#[doc = "source using the message tag \\p data_tag within the communicator \\p"]
#[doc = "comm. On completion, the \\p callback function is called with the"]
#[doc = "argument \\p arg."]
#[doc = "Similarly to the pthread detached functionality, when a detached"]
#[doc = "communication completes, its resources are automatically released"]
#[doc = "back to the system, there is no need to test or to wait for the"]
#[doc = "completion of the request."]
#[no_mangle]
pub unsafe extern "C" fn starpu_mpi_irecv_detached(
    data_handle: starpu_data_handle_t,
    source: c_int,
    data_tag: starpu_mpi_tag_t,
    _comm: MPI_Comm,
    callback: Option<unsafe extern "C" fn(arg1: *mut c_void)>,
    arg: *mut c_void,
) -> c_int {
    recv_with_callback(data_handle, source as Rank, data_tag, callback, arg, true);
    0
}

#[doc = "Block the caller until all group members of the communicator \\p"]
#[doc = "comm have called it."]
#[no_mangle]
pub unsafe extern "C" fn starpu_mpi_barrier(_comm: MPI_Comm) -> c_int {
    wait_for_all();
    barrier();
    0
}
