#include <rdma_util.h>

#include <cstdint>
#include <cstdio>
#include <utility>

uint8_t data_buffer1[1024];
uint8_t data_buffer2[1024];

int main() {
    auto qp1 = rdma_util::RcQueuePair::create("mlx5_0");
    auto qp2 = rdma_util::RcQueuePair::create("mlx5_5");

    qp1->bring_up(qp2->get_handshake_data());
    qp2->bring_up(qp1->get_handshake_data());

    auto data_mr_1 = rdma_util::MemoryRegion::create(qp1->get_pd(), data_buffer1, 1024);
    auto data_mr_2 = rdma_util::MemoryRegion::create(qp2->get_pd(), data_buffer2, 1024);

    printf("created data mr\n");

    auto context1 = rdma_util::TcclContext::create(std::move(qp1));
    auto context2 = rdma_util::TcclContext::create(std::move(qp2));

    printf("created tccl context\n");

    context1->send(0, uint64_t(data_mr_1->get_addr()), data_mr_1->get_length(), data_mr_1->get_lkey());
    context1->send(1, uint64_t(data_mr_1->get_addr()), data_mr_1->get_length(), data_mr_1->get_lkey());
    context1->send(3, uint64_t(data_mr_1->get_addr()), data_mr_1->get_length(), data_mr_1->get_lkey());
    context1->send(2, uint64_t(data_mr_1->get_addr()), data_mr_1->get_length(), data_mr_1->get_lkey());

    context2->recv(1, uint64_t(data_mr_2->get_addr()), data_mr_2->get_length(), data_mr_2->get_rkey());
    context2->recv(2, uint64_t(data_mr_2->get_addr()), data_mr_2->get_length(), data_mr_2->get_rkey());
    context2->recv(3, uint64_t(data_mr_2->get_addr()), data_mr_2->get_length(), data_mr_2->get_rkey());
    context2->recv(0, uint64_t(data_mr_2->get_addr()), data_mr_2->get_length(), data_mr_2->get_rkey());

    printf("received\n");

    return 0;
}