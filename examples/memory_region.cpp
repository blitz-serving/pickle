#include <cstdio>
#include <cstdlib>
#include <memory>

#include "gpu_mem_util.h"
#include "pickle_logger.h"
#include "rdma_util.h"

int main() {
    {
        std::shared_ptr<rdma_util::MemoryRegion> mr = nullptr;

        {
            auto context = rdma_util::Context::create("mlx5_0");
            auto pd = rdma_util::ProtectionDomain::create(std::move(context));

            auto length = 1024;
            std::shared_ptr<void> buffer_with_deleter(gpu_mem_util::malloc_gpu_buffer(1024, 7), [](void* p) {
                INFO("buffer_with_deleter deallocating GPU memory");
                gpu_mem_util::free_gpu_buffer(p, 7);
            });

            mr = rdma_util::MemoryRegion::create(std::move(pd), buffer_with_deleter, length);
            INFO("mr created");
        }
        INFO("mr out of scope");
    }
    INFO("returning");

    return 0;
}