#include <cuda_runtime.h>
#include <infiniband/verbs.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include "cuda_util.h"
#include "rdma_util.h"

#define RDMA_CHECK(expr)                                                                 \
    do {                                                                                 \
        int ret = expr;                                                                  \
        if (ret != 0) {                                                                  \
            fprintf(stderr, "%s:%d RDMA_CHECK failed: %s\n", __FILE__, __LINE__, #expr); \
            std::abort();                                                                \
        }                                                                                \
    } while (0)

int main(int argc, char* argv[]) {
    if (argc != 4) {
        fprintf(
            stderr,
            "Usage: %s <rnic_name> <gid_index> <gpu_index>\nUse host memory when <gpu_index>==-1\n",
            argv[0]
        );
        std::abort();
    }

    uint64_t buffer_size = 8ull * 1024 * 1024 * 1024;

    const char* dev_name = argv[1];
    int gid_index = atoi(argv[2]);
    const int device = atoi(argv[3]);

    printf("dev_name=%s gid_index=%d device=%d\n", dev_name, gid_index, device);

    void* buffer = nullptr;
    if (device == -1) {
        buffer = std::malloc(buffer_size);
        PICKLE_ASSERT(buffer != nullptr);
    } else {
        buffer = cuda_util::try_malloc(buffer_size, device).unwrap();
    }

    {
        auto qp = rdma_util::RcQueuePair::create(dev_name);
        auto mr = rdma_util::MemoryRegion::create(qp->get_pd(), buffer, buffer_size);
        qp->bring_up(qp->get_handshake_data(gid_index), gid_index);
        PICKLE_ASSERT(qp->query_qp_state() == ibv_qp_state::IBV_QPS_RTS);
        std::vector<ibv_wc> polled_recv_wcs, polled_send_wcs;

        // Loopback send/recv
        for (int i = 0; i < 10; ++i) {
            RDMA_CHECK(qp->post_recv(i, reinterpret_cast<uint64_t>(buffer) + i * 1024, 1024, mr->get_lkey()));
        }
        for (int i = 10; i < 20; ++i) {
            RDMA_CHECK(
                qp->post_send_send(i, reinterpret_cast<uint64_t>(buffer) + i * 1024, 1024, mr->get_lkey(), true)
            );
        }

        RDMA_CHECK(qp->wait_until_send_completion(10, polled_send_wcs));
        RDMA_CHECK(qp->wait_until_recv_completion(10, polled_recv_wcs));

        // Loopback read
        RDMA_CHECK(qp->post_send_read(
            20,
            reinterpret_cast<uint64_t>(buffer) + 20 * 1024,
            reinterpret_cast<uint64_t>(buffer) + 0 * 1024,
            1024,
            mr->get_lkey(),
            mr->get_rkey(),
            true
        ));
        RDMA_CHECK(qp->wait_until_send_completion(1, polled_send_wcs));

        // Loopback write
        RDMA_CHECK(qp->post_send_write(
            21,
            reinterpret_cast<uint64_t>(buffer) + 21 * 1024,
            reinterpret_cast<uint64_t>(buffer) + 0 * 1024,
            1024,
            mr->get_lkey(),
            mr->get_rkey(),
            true
        ));
        RDMA_CHECK(qp->wait_until_send_completion(1, polled_send_wcs));

        // Loopback write with immediate data
        RDMA_CHECK(qp->post_recv(22, reinterpret_cast<uint64_t>(buffer), 1024, mr->get_lkey()));
        RDMA_CHECK(qp->post_send_write_with_imm(
            22,
            reinterpret_cast<uint64_t>(buffer),
            reinterpret_cast<uint64_t>(buffer),
            1024,
            1234,
            mr->get_lkey(),
            mr->get_rkey(),
            true
        ));
        RDMA_CHECK(qp->wait_until_send_completion(1, polled_send_wcs));
        RDMA_CHECK(qp->wait_until_recv_completion(1, polled_recv_wcs));
    }

    if (device == -1) {
        std::free(buffer);
    } else {
        cuda_util::try_free(buffer).unwrap();
    }

    printf("Success\n");

    return 0;
}
