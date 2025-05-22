#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include "rdma_util.h"

#define CHECK(expr)                                                                 \
    do {                                                                            \
        if (!(expr)) {                                                              \
            fprintf(stderr, "%s:%d Check failed: %s\n", __FILE__, __LINE__, #expr); \
            exit(EXIT_FAILURE);                                                     \
        }                                                                           \
    } while (0)

int main() {
    uint64_t buffer_size = 1024 * 1024 * 1024;
    void* buffer = malloc(buffer_size);
    int gid_index = 0;
    const char* dev_name = "mlx5_1";

    {
        auto qp = rdma_util::RcQueuePair::create(dev_name);
        auto mr = rdma_util::MemoryRegion::create(qp->get_pd(), buffer, buffer_size);
        qp->bring_up(qp->get_handshake_data(gid_index), gid_index);

        if (qp->query_qp_state() != ibv_qp_state::IBV_QPS_RTS) {
            printf("qp1 is not in RTS state\n");
            return 1;
        }

        for (int i = 0; i < 10; ++i) {
            CHECK(qp->post_recv(i, reinterpret_cast<uint64_t>(buffer) + i * 1024, 1024, mr->get_lkey()));
        }

        for (int i = 10; i < 20; ++i) {
            CHECK(qp->post_send_send(i, reinterpret_cast<uint64_t>(buffer) + i * 1024, 1024, mr->get_lkey(), true));
        }

        std::vector<rdma_util::WorkCompletion> polled_recv_wcs, polled_send_wcs;

        CHECK(qp->wait_until_send_completion(10, polled_send_wcs));
        for (const auto& wc : polled_send_wcs) {
            printf("success %s\n", wc.to_string().c_str());
        }

        CHECK(qp->wait_until_recv_completion(10, polled_recv_wcs));
        for (const auto& wc : polled_recv_wcs) {
            printf("success %s\n", wc.to_string().c_str());
        }

        CHECK(qp->post_send_read(
            20,
            reinterpret_cast<uint64_t>(buffer) + 20 * 1024,
            reinterpret_cast<uint64_t>(buffer) + 0 * 1024,
            1024,
            mr->get_lkey(),
            mr->get_rkey(),
            true
        ));
        CHECK(qp->wait_until_send_completion(1, polled_send_wcs));
        for (const auto& wc : polled_send_wcs) {
            printf("success %s\n", wc.to_string().c_str());
        }

        CHECK(qp->post_send_write(
            21,
            reinterpret_cast<uint64_t>(buffer) + 21 * 1024,
            reinterpret_cast<uint64_t>(buffer) + 0 * 1024,
            1024,
            mr->get_lkey(),
            mr->get_rkey(),
            true
        ));
        CHECK(qp->wait_until_send_completion(1, polled_send_wcs));
        for (const auto& wc : polled_send_wcs) {
            printf("success %s\n", wc.to_string().c_str());
        }

        CHECK(qp->post_recv(22, reinterpret_cast<uint64_t>(buffer), 1024, mr->get_lkey()));
        CHECK(qp->post_send_write_with_imm(
            22,
            reinterpret_cast<uint64_t>(buffer),
            reinterpret_cast<uint64_t>(buffer),
            1024,
            1234,
            mr->get_lkey(),
            mr->get_rkey(),
            true
        ));

        CHECK(qp->wait_until_send_completion(1, polled_send_wcs));
        for (const auto& wc : polled_send_wcs) {
            printf("%s\n", wc.to_string().c_str());
        }

        CHECK(qp->wait_until_recv_completion(1, polled_recv_wcs));
        for (const auto& wc : polled_recv_wcs) {
            printf("%s\n", wc.to_string().c_str());
        }
    }

    free(buffer);
    return 0;
}