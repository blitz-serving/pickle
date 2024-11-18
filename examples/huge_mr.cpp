
#include <cstdio>

#ifdef USE_CUDA

#include <cstdint>
#include <utility>

#include "gpu_mem_util.h"
#include "rdma_util.h"

constexpr uint32_t kGPU = 7;
constexpr const char* kRNIC = "mlx5_5";

// 75GB
constexpr uint64_t kSize = 75ull * 1024 * 1024 * 1024;

int main() {
    void* d_ptr = gpu_mem_util::malloc_gpu_buffer(kSize, kGPU);

    {
        auto pd = rdma_util::ProtectionDomain::create(rdma_util::Context::create(kRNIC));
        auto mr = rdma_util::MemoryRegion::create(std::move(pd), d_ptr, kSize);
    }

    gpu_mem_util::free_gpu_buffer(d_ptr, kGPU);

    return 0;
}

#else

int main() {
    printf("CUDA is disabled\n");
    return 0;
}

#endif