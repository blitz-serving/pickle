#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>

#include "rdma_util.h"

static uint8_t buffer[1024];

TEST(OpenDevice, CreateQP) {
    const char* dev_name = "mlx5_0";
    auto context = rdma_util::Context::create(dev_name);
    auto qp = rdma_util::RcQueuePair::create(context);
    qp->bring_up(qp->get_handshake_data());
    ASSERT_EQ(qp->query_qp_state(), rdma_util::QueuePairState::RTS);
}

TEST(OpenDevice, SendRecv) {
    const char* dev_name = "mlx5_0";
    auto context = rdma_util::Context::create(dev_name);
    auto qp = rdma_util::RcQueuePair::create(context);
    qp->bring_up(qp->get_handshake_data());
    ASSERT_EQ(qp->query_qp_state(), rdma_util::QueuePairState::RTS);

    auto mr = rdma_util::MemoryRegion::create(qp->get_pd(), buffer, 1024);
    std::vector<rdma_util::WorkCompletion> polled_recv_wcs, polled_send_wcs;
    ASSERT_EQ(0, qp->post_recv(1, reinterpret_cast<uint64_t>(buffer), 1024, mr->get_lkey()));
    ASSERT_EQ(0, qp->post_send_send(1, reinterpret_cast<uint64_t>(buffer), 1024, mr->get_lkey(), true));
    ASSERT_EQ(0, qp->wait_until_send_completion(1, polled_send_wcs));
    ASSERT_EQ(0, qp->wait_until_recv_completion(1, polled_recv_wcs));

    for (const auto& wc : polled_send_wcs) {
        ASSERT_EQ(wc.status, IBV_WC_SUCCESS);
    }

    for (const auto& wc : polled_recv_wcs) {
        ASSERT_EQ(wc.status, IBV_WC_SUCCESS);
    }
}

TEST(OpenDevice, SendRecvError) {
    const char* dev_name = "mlx5_0";
    auto context = rdma_util::Context::create(dev_name);
    auto qp = rdma_util::RcQueuePair::create(context);
    qp->bring_up(qp->get_handshake_data());
    ASSERT_EQ(qp->query_qp_state(), rdma_util::QueuePairState::RTS);

    auto mr = rdma_util::MemoryRegion::create(qp->get_pd(), buffer, 1024);
    std::vector<rdma_util::WorkCompletion> polled_recv_wcs, polled_send_wcs;

    qp->post_recv(1, reinterpret_cast<uint64_t>(buffer), 512, mr->get_lkey());
    qp->post_recv(2, reinterpret_cast<uint64_t>(buffer), 1024, mr->get_lkey());

    qp->post_send_send(3, reinterpret_cast<uint64_t>(buffer), 1024, mr->get_lkey(), true);
    qp->post_send_send(4, reinterpret_cast<uint64_t>(buffer), 512, mr->get_lkey(), true);

    std::this_thread::sleep_for(std::chrono::seconds(1));

    ASSERT_NE(0, qp->poll_send_cq_once(2, polled_send_wcs));
    ASSERT_NE(0, qp->poll_recv_cq_once(2, polled_recv_wcs));

    for (const auto& wc : polled_send_wcs) {
        ASSERT_NE(IBV_WC_SUCCESS, wc.status);
    }
    qp->poll_recv_cq_once(2, polled_recv_wcs);
    for (const auto& wc : polled_recv_wcs) {
        ASSERT_NE(IBV_WC_SUCCESS, wc.status);
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}