#include <cstdint>
#include <cstdlib>
#include <vector>

#include "rdma_util.h"

int main() {
    const uint64_t size = 1024 * 1024 * 1024;
    auto buffer = new uint8_t[size];

    {
        auto qp = rdma_util::RcQueuePair::create("mlx5_0");
        auto mr = rdma_util::MemoryRegion::create(qp->get_pd(), buffer, size);
        qp->bring_up(qp->get_handshake_data());

        if (qp->query_qp_state() != rdma_util::QueuePairState::RTS) {
            printf("qp1 is not in RTS state\n");
            return 1;
        }

        for (int i = 0; i < 10; ++i) {
            if (qp->post_recv(i, reinterpret_cast<uint64_t>(buffer) + i * 1024, 1024, mr->get_lkey())) {
                printf("post_recv failed\n");
                return 1;
            }
        }

        for (int i = 10; i < 20; ++i) {
            if (qp->post_send(i, reinterpret_cast<uint64_t>(buffer) + i * 1024, 1024, mr->get_lkey(), true)) {
                printf("post_send failed\n");
                return 1;
            }
        }

        std::vector<rdma_util::WorkCompletion> polled_recv_wcs, polled_send_wcs;

        qp->wait_until_send_completion(10, polled_send_wcs);
        for (const auto& wc : polled_send_wcs) {
            printf("success %s\n", wc.to_string().c_str());
        }

        qp->wait_until_recv_completion(10, polled_recv_wcs);
        for (const auto& wc : polled_recv_wcs) {
            printf("success %s\n", wc.to_string().c_str());
        }
    }

    delete[] buffer;
    return 0;
}