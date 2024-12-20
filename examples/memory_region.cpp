#include <cstdio>
#include <cstdlib>
#include <memory>

#include "gpu_mem_util.h"
#include "rdma_util.h"

int main() {
    {
        rdma_util::Arc<rdma_util::MemoryRegion> mr = nullptr;

        {
            auto context = rdma_util::Context::create("mlx5_0");
            auto pd = rdma_util::ProtectionDomain::create(std::move(context));

            auto length = 1024;
            rdma_util::Arc<void> buffer_with_deleter(gpu_mem_util::malloc_gpu_buffer(1024, 7), [](void* p) {
                printf("buffer_with_deleter deallocating GPU memory\n");
                gpu_mem_util::free_gpu_buffer(p, 7);
            });

            mr = rdma_util::MemoryRegion::create(std::move(pd), buffer_with_deleter, length);
            printf("mr created\n");
        }
        printf("mr out of scope\n");
    }
    printf("returning\n");

    return 0;
}