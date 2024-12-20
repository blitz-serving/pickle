#include <cuda_runtime.h>

#include <cstdint>
#include <cstdio>

#include "rdma_util.h"

int main() {
    cudaSetDevice(7);
    int device;
    cudaGetDevice(&device);
    printf("Device: %d\n", device);

    uint32_t flag;
    cudaGetDeviceFlags(&flag);
    printf("Device flags: %u\n", flag);

    void* d_ptr;
    cudaMalloc(&d_ptr, 1024);

    auto pd = rdma_util::ProtectionDomain::create(rdma_util::Context::create("mlx5_1"));
    auto mr = rdma_util::MemoryRegion::create(std::move(pd), d_ptr, 1024);

    return 0;
}
