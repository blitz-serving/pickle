#include <cstdio>
#include <cstdlib>
#include <memory>

#include "rdma_util.h"

#if USE_CUDA
#include "gpu_mem_util.h"
#endif

int main() {
    {
        rdma_util::Arc<rdma_util::MemoryRegion> mr = nullptr;

        {
            auto context = rdma_util::Context::create("mlx5_0");
            auto pd = rdma_util::ProtectionDomain::create(std::move(context));
#if USE_CUDA
            auto length = 1024;
            rdma_util::Arc<void> buffer_with_deleter(gpu_mem_util::malloc_gpu_buffer(1024, 7), [](void* p) {
                printf("buffer_with_deleter deallocating GPU memory\n");
                gpu_mem_util::free_gpu_buffer(p, 7);
            });
#else
            auto length = sizeof(rdma_util::Ticket) * 128;
            rdma_util::Arc<void> buffer_with_deleter(new rdma_util::Ticket[128], [](rdma_util::Ticket* p) {
                printf("buffer_with_deleter deallocating host memory\n");
                delete[] p;
            });
#endif
            mr = rdma_util::MemoryRegion::create(std::move(pd), buffer_with_deleter, length);
            printf("mr created\n");
        }
        printf("mr out of scope\n");
    }
    printf("returning\n");

    return 0;
}