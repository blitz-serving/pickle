#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include "rdma_util.h"

int main() {
    const uint64_t size = 1024;
    auto buffer1 = new uint8_t[size];
    auto buffer2 = new uint8_t[size];

    {
        std::shared_ptr<rdma_util::ProtectionDomain> pd1 =
            rdma_util::ProtectionDomain::create(rdma_util::Context::create("mlx5_1"));
        auto mr1 = rdma_util::MemoryRegion::create(pd1, buffer1, size);
        std::shared_ptr<rdma_util::ProtectionDomain> pd2 =
            rdma_util::ProtectionDomain::create(rdma_util::Context::create("mlx5_2"));
        auto mr2 = rdma_util::MemoryRegion::create(pd2, buffer2, size);

        auto qp1 = rdma_util::RcQueuePair::create(pd1);
        auto qp2 = rdma_util::RcQueuePair::create(pd2);

        // auto qp1 = rdma_util::RcQueuePair::create("mlx5_1");
        // auto qp2 = rdma_util::RcQueuePair::create("mlx5_2");
        // auto mr1 = rdma_util::MemoryRegion::create(qp1->get_pd(), buffer1, size);
        // auto mr2 = rdma_util::MemoryRegion::create(qp2->get_pd(), buffer2, size);

        printf("mr1: %p\n", mr1->get_addr());
        printf("mr2: %p\n", mr2->get_addr());
        qp1->bring_up(qp2->get_handshake_data(3), 3);
        qp2->bring_up(qp1->get_handshake_data(3), 3);

        if (qp1->query_qp_state() != ibv_qp_state::IBV_QPS_RTS) {
            printf("qp1 is not in RTS state\n");
            return 1;
        }
        if (qp2->query_qp_state() != ibv_qp_state::IBV_QPS_RTS) {
            printf("qp2 is not in RTS state\n");
            return 1;
        }

        if (qp1->post_send_read(
                1,
                reinterpret_cast<uint64_t>(buffer1),
                reinterpret_cast<uint64_t>(buffer2),
                size,
                mr1->get_lkey(),
                mr2->get_rkey(),
                true
            )) {
            printf("post_send_read failed\n");
            return 1;
        } else {
            std::vector<rdma_util::WorkCompletion> polled_recv_wcs, polled_send_wcs;
            qp1->wait_until_send_completion(1, polled_recv_wcs);
            for (const auto& wc : polled_recv_wcs) {
                printf("polled %s\n", wc.to_string().c_str());
            }
        }

        if (qp2->post_send_write(
                1,
                reinterpret_cast<uint64_t>(buffer2),
                reinterpret_cast<uint64_t>(buffer1),
                size,
                mr2->get_lkey(),
                mr1->get_rkey(),
                true
            )) {
            printf("post_send_write failed\n");
            return 1;
        } else {
            std::vector<rdma_util::WorkCompletion> polled_recv_wcs, polled_send_wcs;
            qp2->wait_until_send_completion(1, polled_send_wcs);
            for (const auto& wc : polled_send_wcs) {
                printf("polled %s\n", wc.to_string().c_str());
            }
        }
    }

    delete[] buffer1;
    return 0;
}