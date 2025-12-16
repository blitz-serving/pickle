#include <cuda_runtime.h>
#include <mpi.h>
#include <nccl.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "cuda_util.h"

#ifndef USE_NCCL
int main() {
    std::fprintf(stderr, "NCCL support is disabled. Reconfigure with -DUSE_NCCL=ON.\n");
    return 1;
}
#else

#define NCCL_CHECK(cmd)                                                                                 \
    do {                                                                                                \
        ncclResult_t r = cmd;                                                                           \
        if (r != ncclSuccess) {                                                                         \
            std::fprintf(stderr, "NCCL error %s:%d '%s'\n", __FILE__, __LINE__, ncclGetErrorString(r)); \
            std::abort();                                                                               \
        }                                                                                               \
    } while (0)

/*

Run rdma test:
```
NCCL_DEBUG_SUBSYS=INIT,NET \
NCCL_DEBUG=INFO \
NCCL_SHM_DISABLE=1 \
NCCL_P2P_DISABLE=1 \
NCCL_MIN_NCHANNELS=16 \
NCCL_P2P_NET_CHUNKSIZE=262144 \
NCCL_IB_SPLIT_DATA_ON_QPS=1 \
NCCL_IB_QPS_PER_CONNECTION=4 \
mpirun --allow-run-as-root -n 8 \
./build/bench_nccl_p2p \
--size 68719476736 \
--warmup 1 \
--iters 10
```

*/
void print_usage(const char* prog) {
    std::fprintf(
        stderr,
        "Usage: mpirun --allow-run-as-root -n <world_size> %s [--size BYTES] [--warmup N] [--iters N]\n",
        prog
    );
}

int main(int argc, char** argv) {
    size_t bytes = 64 * 1024 * 1024;
    int warmup = 10;
    int iters = 100;

    auto need_value = [&](int idx) -> const char* {
        if (idx + 1 >= argc) {
            std::fprintf(stderr, "Missing value for %s\n", argv[idx]);
            print_usage(argv[0]);
            std::exit(EXIT_FAILURE);
        }
        return argv[idx + 1];
    };

    for (int i = 1; i < argc; ++i) {
        const char* arg = argv[i];
        if (std::strcmp(arg, "--size") == 0) {
            bytes = std::stoull(need_value(i));
            ++i;
        } else if (std::strcmp(arg, "--warmup") == 0) {
            warmup = std::stoi(need_value(i));
            ++i;
        } else if (std::strcmp(arg, "--iters") == 0) {
            iters = std::stoi(need_value(i));
            ++i;
        } else if (std::strcmp(arg, "--help") == 0 || std::strcmp(arg, "-h") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            std::fprintf(stderr, "Unknown argument: %s\n", arg);
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
    }

    MPI_Init(&argc, &argv);

    int world_size = 0;
    int world_rank = 0;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    if (world_size < 2) {
        if (world_rank == 0) {
            std::fprintf(stderr, "This benchmark requires at least 2 MPI processes.\n");
        }
        MPI_Finalize();
        return EXIT_FAILURE;
    }

    if (bytes == 0 || iters <= 0 || warmup < 0) {
        if (world_rank == 0) {
            std::fprintf(stderr, "Invalid arguments. Ensure size>0, iters>0, warmup>=0.\n");
        }
        MPI_Finalize();
        return EXIT_FAILURE;
    }

    int device_count = 0;
    CUDA_CHECK(cudaGetDeviceCount(&device_count));
    if (world_size > device_count) {
        if (world_rank == 0) {
            std::fprintf(stderr, "Requested %d GPUs (world_size), but only %d present.\n", world_size, device_count);
        }
        MPI_Finalize();
        return EXIT_FAILURE;
    }

    const int local_device = world_rank;
    const int send_peer = (world_rank + 1) % world_size;
    const int recv_peer = (world_rank - 1 + world_size) % world_size;
    const bool is_sender = world_rank % 2 == 0;
    CUDA_CHECK(cudaSetDevice(local_device));

    cudaStream_t stream;
    void* buffer = nullptr;

    CUDA_CHECK(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking));
    CUDA_CHECK(cudaMalloc(&buffer, bytes));

    ncclUniqueId id;
    if (world_rank == 0) {
        NCCL_CHECK(ncclGetUniqueId(&id));
    }
    MPI_Bcast(&id, sizeof(id), MPI_BYTE, 0, MPI_COMM_WORLD);

    ncclComm_t comm;
    NCCL_CHECK(ncclCommInitRank(&comm, world_size, id, world_rank));

    auto run_once = [&](bool measure) -> double {
        MPI_Barrier(MPI_COMM_WORLD);
        auto start = std::chrono::high_resolution_clock::now();

        NCCL_CHECK(ncclGroupStart());
        if (is_sender) {
            NCCL_CHECK(ncclSend(buffer, bytes, ncclInt8, /*peer=*/send_peer, comm, stream));
        } else {
            NCCL_CHECK(ncclRecv(buffer, bytes, ncclInt8, /*peer=*/recv_peer, comm, stream));
        }
        NCCL_CHECK(ncclGroupEnd());

        CUDA_CHECK(cudaStreamSynchronize(stream));
        MPI_Barrier(MPI_COMM_WORLD);

        auto end = std::chrono::high_resolution_clock::now();
        if (!measure) {
            return 0.0;
        }
        std::chrono::duration<double> elapsed = end - start;
        return elapsed.count();
    };

    for (int i = 0; i < warmup; ++i) {
        run_once(false);
    }

    double total_sec = 0.0;
    for (int i = 0; i < iters; ++i) {
        double sec = run_once(true);
        total_sec += sec;
        double gbps = (bytes * 8.0) / (sec * 1e9);
        std::printf("Rank %d -> %d Iter %d: %.2f Gbps (%.3f ms)\n", world_rank, send_peer, i, gbps, sec * 1e3);
    }

    double avg_gbps = (bytes * 8.0 * iters) / (total_sec * 1e9);
    std::printf(
        "Rank %d -> %d average bandwidth: %.2f Gbps over %d iterations, payload %zu bytes\n",
        world_rank,
        send_peer,
        avg_gbps,
        iters,
        bytes
    );

    CUDA_CHECK(cudaStreamDestroy(stream));
    CUDA_CHECK(cudaFree(buffer));
    NCCL_CHECK(ncclCommDestroy(comm));

    MPI_Finalize();
    return 0;
}

#endif
