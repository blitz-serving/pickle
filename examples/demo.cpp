#include <cstdint>
#include <cstdlib>

#include "rdma_util.h"

int main() {
    const uint64_t size = 1024 * 1024 * 1024;
    auto buffer = new uint8_t[size];

    {
        auto qp1 = rdma_util::RcQueuePair::create("mlx5_0");
        auto qp2 = rdma_util::RcQueuePair::create("mlx5_0");

        auto mr1 = rdma_util::MemoryRegion::create(qp1->get_pd(), buffer, size);
        auto mr2 = rdma_util::MemoryRegion::create(qp1->get_pd(), buffer, size);

        qp1->bring_up(qp2->get_handshake_data());
        qp1->query_qp_state();
        qp2->bring_up(qp1->get_handshake_data());
        qp2->query_qp_state();
    }

    free(buffer);
    return 0;
}