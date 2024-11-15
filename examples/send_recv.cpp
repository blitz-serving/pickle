#include <cstdint>
#include <cstdlib>
#include <vector>

#include "rdma_util.h"

int main() {
    const uint64_t size = 1024 * 1024 * 1024;
    auto buffer = new uint8_t[size];

    {
        auto qp1 = rdma_util::RcQueuePair::create("mlx5_0");
        auto qp2 = rdma_util::RcQueuePair::create("mlx5_5");

        auto mr1 = rdma_util::MemoryRegion::create(qp1->get_pd(), buffer, size);
        auto mr2 = rdma_util::MemoryRegion::create(qp2->get_pd(), buffer, size);

        qp1->bring_up(qp2->get_handshake_data());
        qp2->bring_up(qp1->get_handshake_data());

        if (qp1->query_qp_state() != rdma_util::QueuePairState::RTS) {
            printf("qp1 is not in RTS state\n");
            return 1;
        }
        if (qp2->query_qp_state() != rdma_util::QueuePairState::RTS) {
            printf("qp2 is not in RTS state\n");
            return 1;
        }

        if (qp1->post_recv(1, reinterpret_cast<uint64_t>(buffer), 1024, mr1->get_lkey())) {
            printf("post_recv failed\n");
            return 1;
        }
        if (qp2->post_send_send(1, reinterpret_cast<uint64_t>(buffer), 512, mr2->get_lkey(), true)) {
            printf("post_send failed\n");
            return 1;
        }

        std::vector<rdma_util::WorkCompletion> polled_recv_wcs, polled_send_wcs;
        qp2->wait_until_send_completion(1, polled_send_wcs);
        for (const auto& wc : polled_send_wcs) {
            printf("success %s\n", wc.to_string().c_str());
        }

        qp1->wait_until_recv_completion(1, polled_recv_wcs);
        for (const auto& wc : polled_recv_wcs) {
            printf("success %s\n", wc.to_string().c_str());
        }
    }

    delete[] buffer;
    return 0;
}