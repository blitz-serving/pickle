#include <cstdint>
#include <cstdlib>
#include <vector>

#include "rdma_util.h"

int main() {
    const uint64_t size = 1024;
    auto buffer = new uint8_t[size];

    {
        auto qp1 = rdma_util::RcQueuePair::create("mlx5_1");
        auto qp2 = rdma_util::RcQueuePair::create("mlx5_2");

        auto mr1 = rdma_util::MemoryRegion::create(qp1->get_pd(), buffer, size);
        auto mr2 = rdma_util::MemoryRegion::create(qp2->get_pd(), buffer, size);

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

        if (qp1->post_recv(1, reinterpret_cast<uint64_t>(buffer), size, mr1->get_lkey())) {
            printf("post_recv failed\n");
            return 1;
        }
        if (qp2->post_send_send(1, reinterpret_cast<uint64_t>(buffer), size, mr2->get_lkey(), true)) {
            printf("post_send failed\n");
            return 1;
        }

        std::vector<ibv_wc> polled_recv_wcs, polled_send_wcs;
        qp2->wait_until_send_completion(1, polled_send_wcs);
        for (const auto& wc : polled_send_wcs) {
            printf("polled %lu %d\n", wc.wr_id, wc.status);
        }

        qp1->wait_until_recv_completion(1, polled_recv_wcs);
        for (const auto& wc : polled_recv_wcs) {
            printf("polled %lu %d\n", wc.wr_id, wc.status);
        }
    }

    delete[] buffer;
    return 0;
}