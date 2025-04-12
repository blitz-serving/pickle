# Pickle

A simple and easy-to-use GPU-Direct-RDMA P2P communication library.

## Quick start

Build and run examples, tests and benchmarks as following:

```bash
git submodule update --init --recursive
cmake -Bbuild
cmake --build build/
```

Select the proper gid_index that should be used:

- In my testbed (RoCE), gid_index 3 is available.
- You may set it 0 on some IB machine.

Create rdma context on a specific RNIC

- See examples for more details.