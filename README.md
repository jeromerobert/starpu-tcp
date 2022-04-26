# About

*starpu-tcp* aims at being a binary compatible alternative to [*starpu-mpi*](https://github.com/starpu-runtime/starpu),
no relying on any MPI implementation. Instead it just use TCP sockets.

*starpu-tcp* is written in Rust.

# Build

To get the binary compatibility *starpu-tcp* needs a build folder of
*StarPU* with starpu-mpi enabled:

```
mkdir /path/to/build-starpu && cd /path/to/build-starpu
/path/to/starpu-xxx/configure
make
```

Set the following environment variables:

```
C_INCLUDE_PATH=/path/to/openmpi/include
STARPU_SRC_DIR=/path/to/starpu-xxx
STARPU_BUILD_DIR=/path/to/build-starpu
```

Note that environment variables can be set [in the cargo configuration](https://doc.rust-lang.org/cargo/reference/config.html).

Now to build *starpu-tcp*:

- Run `cargo build --release`.
- Copy or link `target/release/libstarpumpi.so` to `libstarpumpi-1.3.so.0` (because of <https://github.com/rust-lang/cargo/issues/1970>)

# Using

You just need to ensure that starpu will load *starpu-tcp* instead of *starpu-mpi*, for example:

- Set `LD_LIBRARY_PATH` (probably prefered)
- Replace the `libstarpumpi-1.3.so.0` in the StarPU installation

*starpu-tcp* does not deal with the program bootstrap. For this it can rely on the OpenMPI
`mpirun` and read the following variables:

- `OMPI_COMM_WORLD_RANK`
- `OMPI_COMM_WORLD_SIZE`
- `OMPI_MCA_btl_tcp_if_include`

`OMPI_MCA_btl_tcp_if_include` must be set (ex:`--mca btl_tcp_if_include ib0`)
to force the usage of the fast (ex: IPoIB) network on your cluster.

*starpu-tcp* also read the first host name of the LSF `LSB_MCPU_HOSTS` variable
so all process know how to contact the 0 process.

(Support for mpich, slurm, ... can easily be added in `src/topology.rs`.)

# Status

This is proof of concept and/or alpha software.

- Only a few functions are implemented (actually only those that I need)
- *starpu-tcp* is faster than *starpu-mpi* on my use cases
- The documentation is reduced to this README.

# Comparison with starpu-nmad

I guess that both starpu-tcp and starpu-nmad try to solve the same issue which
is removing the `MPI_Test` polling and have a really asynchronous implementation.
As I write this starpu-nmad still rely a bit on Mad MPI (see `git grep
nm_mpi_nmad`). This prevent mixing plain MPI solvers and MPI implementation
and starpu-nmad in the same executable.
