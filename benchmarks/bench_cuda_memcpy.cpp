#include <cuda_runtime.h>

#include <cstdio>
#include <cstdlib>

#define CHECK(x)                                                                         \
    do {                                                                                 \
        cudaError_t e = (x);                                                             \
        if (e != cudaSuccess) {                                                          \
            printf("CUDA error %s:%d: %s\n", __FILE__, __LINE__, cudaGetErrorString(e)); \
            std::exit(1);                                                                \
        }                                                                                \
    } while (0)

int main(int argc, char** argv) {
    int src_dev = 0;
    int dst_dev = 1;

    if (argc >= 3) {
        src_dev = std::atoi(argv[1]);
        dst_dev = std::atoi(argv[2]);
    } else {
        std::fprintf(stderr, "Usage: %s <src_dev> <dst_dev> (default 0 1)\n", argv[0]);
        return EXIT_FAILURE;
    }

    const size_t bytes = 64ull * 1024 * 1024;  // 64 MiB
    const int iters = 200;

    // P2P check + enable
    int can01 = 0, can10 = 0;
    CHECK(cudaDeviceCanAccessPeer(&can01, src_dev, dst_dev));
    CHECK(cudaDeviceCanAccessPeer(&can10, dst_dev, src_dev));

    if (!can01 || !can10) {
        printf("P2P not supported between %d and %d\n", src_dev, dst_dev);
        return 0;
    }

    CHECK(cudaSetDevice(src_dev));
    CHECK(cudaDeviceEnablePeerAccess(dst_dev, 0));

    CHECK(cudaSetDevice(dst_dev));
    CHECK(cudaDeviceEnablePeerAccess(src_dev, 0));

    // alloc
    void* d_src = nullptr;
    void* d_dst = nullptr;

    CHECK(cudaSetDevice(src_dev));
    CHECK(cudaMalloc(&d_src, bytes));

    CHECK(cudaSetDevice(dst_dev));
    CHECK(cudaMalloc(&d_dst, bytes));

    // stream + events (measure on dst side)
    cudaStream_t stream;
    cudaEvent_t start, stop;

    CHECK(cudaSetDevice(dst_dev));
    CHECK(cudaStreamCreate(&stream));
    CHECK(cudaEventCreate(&start));
    CHECK(cudaEventCreate(&stop));

    // warmup
    for (int i = 0; i < 10; ++i) {
        CHECK(cudaMemcpyPeerAsync(d_dst, dst_dev, d_src, src_dev, bytes, stream));
    }
    CHECK(cudaStreamSynchronize(stream));

    // timed
    CHECK(cudaEventRecord(start, stream));
    for (int i = 0; i < iters; ++i) {
        CHECK(cudaMemcpyAsync(d_dst, d_src, bytes, cudaMemcpyDeviceToDevice, stream));
        // CHECK(cudaMemcpyPeerAsync(d_dst, dst_dev, d_src, src_dev, bytes, stream));
    }
    CHECK(cudaEventRecord(stop, stream));
    CHECK(cudaEventSynchronize(stop));

    float ms = 0.f;
    CHECK(cudaEventElapsedTime(&ms, start, stop));

    double gb = (double)bytes * iters / 1e9;
    printf("BW: %.2f GB/s\n", gb / (ms / 1e3));

    // cleanup
    CHECK(cudaFree(d_dst));
    CHECK(cudaSetDevice(src_dev));
    CHECK(cudaFree(d_src));

    return 0;
}
