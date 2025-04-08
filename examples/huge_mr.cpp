#include <cstdint>
#include <cstdio>
#include <utility>

#include "cuda_util.h"
#include "rdma_util.h"

constexpr uint32_t kGPU = 7;
constexpr const char* kRNIC = "mlx5_5";

// 75GB
constexpr uint64_t kSize = 75ull * 1024 * 1024 * 1024;

int main() {
    void* d_ptr = cuda_util::malloc_gpu_buffer(kSize, kGPU);

    {
        auto pd = rdma_util::ProtectionDomain::create(rdma_util::Context::create(kRNIC));
        auto mr = rdma_util::MemoryRegion::create(std::move(pd), d_ptr, kSize);
    }

    cuda_util::free_gpu_buffer(d_ptr);
    return 0;
}
