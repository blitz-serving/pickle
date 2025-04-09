#include <cstdint>
#include <cstdio>
#include <utility>

#include "cuda_util.h"
#include "rdma_util.h"

const uint32_t kGPU = 7;
const char* kRNIC = "mlx5_5";

// 75GB
constexpr uint64_t kSize = 75ull * 1024 * 1024 * 1024;

int main() {
    auto buffer = std::shared_ptr<void>(cuda_util::malloc_gpu_buffer(kSize, kGPU), cuda_util::free_gpu_buffer);
    auto pd = rdma_util::ProtectionDomain::create(rdma_util::Context::create(kRNIC));
    auto mr = rdma_util::MemoryRegion::create(std::move(pd), buffer, kSize);
    return 0;
}
