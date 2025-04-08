#include <memory>

#include "cuda_util.h"
#include "rdma_util.h"

constexpr const char* kRNIC = "mlx5_1";
constexpr const uint32_t kGPU = 1;

int main() {
    auto pd = rdma_util::ProtectionDomain::create(rdma_util::Context::create(kRNIC));
    auto buffer_with_deleter =
        std::shared_ptr<void>(cuda_util::malloc_gpu_buffer(1024, kGPU), cuda_util::free_gpu_buffer);
    auto mr = rdma_util::MemoryRegion::create(std::move(pd), buffer_with_deleter, 1024);
    return 0;
}